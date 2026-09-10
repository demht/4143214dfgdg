from contextlib import closing
import asyncio
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import tempfile
import unittest

from aiohttp.test_utils import TestClient, TestServer
from license_service import LicenseService, parse_duration
from license_api import create_app

DEVICE = hashlib.sha256(b"test-device-a").hexdigest()
OTHER = hashlib.sha256(b"test-device-b").hexdigest()


class ServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "bot.db"
        self.now = [1900000000]
        self.service = LicenseService(self.path, clock=lambda: self.now[0])
        self.service.migrate()
        self.row = self.service.create_license(123, 120)
        self.id = self.row["debris_id"]

    def tearDown(self):
        self.tmp.cleanup()

    def activate(self):
        result = self.service.authorize("activate", self.id, DEVICE)
        self.assertEqual(result["status"], "ACTIVE")
        return result["binding_token"]

    def check(self, token, device=DEVICE):
        return self.service.authorize("check", self.id, device, token)["status"]

    def test_two_minutes_expiry_and_extension_same_id(self):
        token = self.activate()
        self.now[0] += 119
        self.assertEqual(self.check(token), "ACTIVE")
        self.now[0] += 1
        self.assertEqual(self.check(token), "EXPIRED")
        extended = self.service.extend_license(123, 60)
        self.assertEqual(extended["debris_id"], self.id)
        self.assertEqual(self.check(token), "ACTIVE")

    def test_reset_during_game_and_reactivation(self):
        token = self.activate()
        self.service.reset_device(123, actor_id=999)
        self.assertEqual(self.check(token), "DEVICE_RESET")
        self.assertEqual(self.check(token), "DEVICE_RESET")
        self.assertFalse(self.service.get_license(123)["device_bound"])
        fresh = self.activate()
        self.assertNotEqual(fresh, token)
        self.assertEqual(self.check(fresh), "ACTIVE")
        self.assertEqual(self.check(token), "DEVICE_RESET")

    def test_other_device_refused_even_with_token(self):
        token = self.activate()
        self.assertEqual(self.check(token, OTHER), "DEVICE_MISMATCH")
        self.assertEqual(self.service.authorize("activate", self.id, OTHER)["status"], "DEVICE_MISMATCH")
        self.assertEqual(self.check(token), "ACTIVE")

    def test_block_unblock_and_reset_while_expired(self):
        token = self.activate()
        self.service.set_blocked(123, True)
        self.assertEqual(self.check(token), "BLOCKED")
        self.service.extend_license(123, 30)
        self.assertEqual(self.check(token), "BLOCKED")
        self.service.set_blocked(123, False)
        self.assertEqual(self.check(token), "ACTIVE")
        self.now[0] += 1000
        self.service.reset_device(123)
        self.assertEqual(self.check(token), "DEVICE_RESET")

    def test_restart_persists_credentials_and_entitlements(self):
        token = self.activate()
        restarted = LicenseService(self.path, clock=lambda: self.now[0])
        restarted.migrate()
        self.assertEqual(restarted.get_license(123)["debris_id"], self.id)
        self.assertEqual(restarted.authorize("check", self.id, DEVICE, token)["status"], "ACTIVE")
        self.assertEqual(len(list(self.path.parent.glob("*.bak"))), 0)

    def test_check_never_binds_or_accepts_client_expiry(self):
        self.assertEqual(self.service.authorize("check", self.id, DEVICE)["status"], "REAUTH_REQUIRED")
        self.assertFalse(self.service.get_license(123)["device_bound"])
        self.assertEqual(self.check("A" * 43), "DEVICE_RESET")
        self.assertFalse(self.service.get_license(123)["device_bound"])

    def test_hashes_only_and_no_user_data_in_api(self):
        token = self.activate()
        with closing(sqlite3.connect(self.path)) as conn:
            contents = "\n".join(conn.iterdump())
        self.assertNotIn(token, contents)
        self.assertNotIn(DEVICE, contents)
        result = self.service.authorize("check", self.id, DEVICE, token)
        self.assertFalse({"user_id", "telegram_id", "username", "device_hash", "binding_token"} & result.keys())

    def test_arbitrary_duration_and_normalization(self):
        for text, value in [("30s", 30), ("2m", 120), ("3h", 10800), ("7d", 604800)]:
            self.assertEqual(parse_duration(text), value)
        for value in ["0s", "-1d", "1.5h", "forever", "9999999999d", "２m"]:
            with self.assertRaises(ValueError): parse_duration(value)
        self.assertEqual(self.service.authorize("activate", " " + self.id.lower() + " ", DEVICE)["status"], "ACTIVE")

    def test_concurrent_extension_and_payment_retry(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(lambda _: self.service.extend_license(123, 1), range(24)))
        self.assertEqual(self.service.get_license(123)["remaining_seconds"], 144)
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda _: self.service.extend_license(123, 60, event_key="test-event"), range(4)))
        self.assertEqual(self.service.get_license(123)["remaining_seconds"], 204)
        with self.assertRaises(ValueError): self.service.extend_license(123, 2, event_key="test-event")

    def test_concurrent_device_activation_has_one_winner(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda device: self.service.authorize("activate", self.id, device)["status"], [DEVICE, OTHER]))
        self.assertCountEqual(results, ["ACTIVE", "DEVICE_MISMATCH"])

    def test_reset_check_race_never_rebinds(self):
        token = self.activate()
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(self.check, token) for _ in range(10)]
            futures.append(pool.submit(self.service.reset_device, 123))
            for future in futures: future.result()
        self.assertEqual(self.check(token), "DEVICE_RESET")
        self.assertFalse(self.service.get_license(123)["device_bound"])

    def test_set_term_replaces_and_create_is_idempotent(self):
        same = self.service.create_license(123, 1000)
        self.assertEqual(same["remaining_seconds"], 120)
        self.assertEqual(same["debris_id"], self.id)
        self.assertEqual(self.service.set_duration(123, 2)["remaining_seconds"], 2)


class MigrationTests(unittest.TestCase):
    def test_existing_schema_preserved_and_backed_up(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "bot.db"
            conn = sqlite3.connect(path)
            conn.executescript("""
                CREATE TABLE licenses(user_id INTEGER PRIMARY KEY,active INTEGER,purchased_at TEXT,username TEXT);
                INSERT INTO licenses VALUES(100,1,'2026-01-02 03:04:05','old'),(200,0,'2026-01-02','blocked');
                CREATE TABLE orders(id INTEGER PRIMARY KEY,user_id INTEGER,status TEXT);
                INSERT INTO orders VALUES(1,100,'DELIVERED'),(2,300,'DELIVERED'),(3,400,'WAITING');
                CREATE TABLE releases(id INTEGER PRIMARY KEY,notes TEXT);
                INSERT INTO releases VALUES(1,'preserve');
            """)
            before = {table: conn.execute(f"SELECT * FROM {table}").fetchall() for table in ["licenses", "orders", "releases"]}
            conn.close()
            service = LicenseService(path)
            service.migrate()
            with closing(sqlite3.connect(path)) as conn:
                for table, rows in before.items():
                    self.assertEqual(conn.execute(f"SELECT * FROM {table}").fetchall(), rows)
                self.assertEqual(conn.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            self.assertTrue(service.get_license(100)["legacy_lifetime"])
            self.assertEqual(service.get_license(200)["status"], "BLOCKED")
            self.assertIsNotNone(service.get_license(300))
            self.assertIsNone(service.get_license(400))
            initial = service.list_licenses()
            service.migrate()
            self.assertEqual(service.list_licenses(), initial)
            backups = list(Path(tmp).glob("*.bak"))
            self.assertEqual(len(backups), 1)
            with closing(sqlite3.connect(backups[0])) as conn:
                self.assertEqual(conn.execute("SELECT * FROM releases").fetchall(), before["releases"])
            service.set_duration(100, 120)
            self.assertFalse(service.get_license(100)["legacy_lifetime"])

    def test_real_database_copy(self):
        source = os.getenv("TEST_LEGACY_DB")
        if not source: self.skipTest("Set TEST_LEGACY_DB to validate a copy of an existing bot.db")
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "bot.db"
            shutil.copy2(source, target)
            def original_tables(path):
                with closing(sqlite3.connect(path)) as conn:
                    return {name: conn.execute(f'SELECT * FROM "{name}"').fetchall()
                            for (name,) in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'debris_%'").fetchall()}
            before = original_tables(target)
            service = LicenseService(target)
            service.migrate()
            service.migrate()
            self.assertEqual(original_tables(target), before)
            self.assertGreaterEqual(len(service.list_licenses(limit=100000)), len(before.get("licenses", [])))


class ApiTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = LicenseService(Path(self.tmp.name) / "bot.db")
        self.service.migrate()
        self.id = self.service.create_license(10, 120)["debris_id"]
        self.client = TestClient(TestServer(create_app(self.service)))
        await self.client.start_server()

    async def asyncTearDown(self):
        await self.client.close()
        self.tmp.cleanup()

    async def test_public_contract(self):
        r = await self.client.get("/api/v1/health")
        self.assertEqual((await r.json())["status"], "OK")
        data = {"debris_id": self.id, "device_id": DEVICE}
        r = await self.client.post("/api/v1/license/activate", json=data)
        active = await r.json()
        self.assertEqual(active["status"], "ACTIVE")
        self.assertEqual(r.headers["Cache-Control"], "no-store")
        data["binding_token"] = active["binding_token"]
        self.service.reset_device(10)
        r = await self.client.post("/api/v1/license/check", json=data)
        self.assertEqual((await r.json())["status"], "DEVICE_RESET")
        data["expires_at"] = 9999999999
        r = await self.client.post("/api/v1/license/check", json=data)
        self.assertEqual(r.status, 400)

    async def test_malformed_oversized_and_rate_limited(self):
        for data in [[], {"debris_id": self.id}, {"debris_id": self.id, "device_id": 7}]:
            r = await self.client.post("/api/v1/license/check", json=data)
            self.assertEqual(r.status, 400)
        r = await self.client.post("/api/v1/license/check", data="x" * 5000, headers={"Content-Type": "application/json"})
        self.assertEqual(r.status, 413)
        for _ in range(11):
            r = await self.client.post("/api/v1/license/activate", json={"debris_id": self.id, "device_id": OTHER})
        self.assertEqual(r.status, 429)


if __name__ == "__main__": unittest.main()
