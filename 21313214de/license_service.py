"""Transactional license service. No Telegram or payment dependencies.

UTC epoch seconds belong to the server. BEGIN IMMEDIATE serializes binding,
reset and extension operations across the bot and API processes.
"""
from __future__ import annotations

import hashlib
import hmac
import re
import secrets
import sqlite3
import time
from contextlib import contextmanager, closing
from datetime import datetime, timezone
from pathlib import Path

ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
ID_PATTERN = re.compile(r"DBR-[A-Z2-9]{4}-[A-Z2-9]{4}-[A-Z2-9]{4}\Z", re.ASCII)
DEVICE_PATTERN = re.compile(r"[a-f0-9]{64}\Z", re.ASCII)
TOKEN_PATTERN = re.compile(r"[A-Za-z0-9_-]{43}\Z", re.ASCII)
MAX_DURATION = 10 * 366 * 86400
SCHEMA_VERSION = 1


def parse_duration(value: str) -> int:
    match = re.fullmatch(r"([1-9][0-9]{0,9})([smhd])", value.strip().lower(), re.ASCII)
    if not match:
        raise ValueError("Введите срок: 30s, 2m, 3h или 7d.")
    return duration_seconds(int(match[1]) * {"s": 1, "m": 60, "h": 3600, "d": 86400}[match[2]])


def duration_seconds(value: int) -> int:
    if type(value) is not int or not 1 <= value <= MAX_DURATION:
        raise ValueError("Срок должен быть от 1 секунды до 10 лет.")
    return value


def normalize_id(value: str) -> str:
    if not isinstance(value, str) or len(value) > 32 or not value.isascii():
        raise ValueError("Некорректный Debris ID.")
    value = value.strip().upper()
    if not ID_PATTERN.fullmatch(value):
        raise ValueError("Формат: DBR-XXXX-XXXX-XXXX.")
    return value


def new_id() -> str:
    return "DBR-" + "-".join("".join(secrets.choice(ALPHABET) for _ in range(4)) for _ in range(3))


def digest(value: str) -> str:
    return hashlib.sha256(value.encode("ascii")).hexdigest()


def status_of(row, now: int) -> str:
    if row["blocked"]:
        return "BLOCKED"
    if not row["legacy_lifetime"] and row["expires_at"] <= now:
        return "EXPIRED"
    return "ACTIVE"


class LicenseService:
    def __init__(self, db_path: str | Path, *, clock=time.time, check_interval=5, lease_seconds=15):
        self.path = Path(db_path).resolve()
        self.clock = clock
        self.check_interval = int(check_interval)
        self.lease_seconds = int(lease_seconds)
        if not 1 <= self.check_interval <= 300:
            raise ValueError("LICENSE_CHECK_INTERVAL_SECONDS must be 1..300")
        if not self.check_interval + 2 <= self.lease_seconds <= 600:
            raise ValueError("LICENSE_LEASE_SECONDS must be interval+2..600")

    def connect(self):
        conn = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=10000")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    @contextmanager
    def transaction(self):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    def migrate(self):
        """Add tables only; snapshot existing DB before first migration.

        Backup runs through a second read connection while the reserved write
        lock prevents another migrator or writer changing the source.
        """
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.transaction() as conn:
            names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            version = 0
            if "debris_schema" in names:
                version = conn.execute("SELECT COALESCE(MAX(version),0) FROM debris_schema").fetchone()[0]
            if version > SCHEMA_VERSION:
                raise RuntimeError("Database schema is newer than this application; refusing downgrade")
            if version == SCHEMA_VERSION:
                return
            if names:
                backup = self.path.with_name(self.path.name + ".pre-license-v1." + secrets.token_hex(6) + ".bak")
                source = self.connect()
                target = sqlite3.connect(backup)
                try:
                    source.backup(target)
                    if target.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                        raise RuntimeError("Migration backup integrity check failed")
                finally:
                    source.close()
                    target.close()
                backup.chmod(0o600)
            conn.execute("""CREATE TABLE debris_licenses (
                user_id INTEGER PRIMARY KEY CHECK(user_id > 0),
                debris_id TEXT NOT NULL UNIQUE,
                expires_at INTEGER NOT NULL,
                blocked INTEGER NOT NULL DEFAULT 0 CHECK(blocked IN (0,1)),
                legacy_lifetime INTEGER NOT NULL DEFAULT 0 CHECK(legacy_lifetime IN (0,1)),
                device_hash TEXT, token_hash TEXT,
                binding_version INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL,
                activated_at INTEGER, last_check_at INTEGER, reset_at INTEGER,
                CHECK ((device_hash IS NULL) = (token_hash IS NULL))
            )""")
            conn.execute("""CREATE TABLE debris_license_audit (
                id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL,
                action TEXT NOT NULL, actor_id INTEGER,
                created_at INTEGER NOT NULL, details TEXT NOT NULL DEFAULT ''
            )""")
            conn.execute("""CREATE TABLE debris_license_events (
                event_key TEXT PRIMARY KEY, user_id INTEGER NOT NULL,
                duration INTEGER NOT NULL, created_at INTEGER NOT NULL
            )""")
            conn.execute("CREATE TABLE debris_schema (version INTEGER PRIMARY KEY, applied_at INTEGER NOT NULL)")
            now = int(self.clock())
            if "licenses" in names:
                for old in conn.execute("SELECT user_id, active, purchased_at FROM licenses").fetchall():
                    created = now
                    try:
                        dt = datetime.fromisoformat(old["purchased_at"])
                        created = int(dt.replace(tzinfo=dt.tzinfo or timezone.utc).timestamp())
                    except (ValueError, TypeError):
                        pass
                    self._insert(conn, old["user_id"], 0, legacy=True,
                                 blocked=not bool(old["active"]), created=created)
            # Recover the same delivered-order users as the old bot's backfill.
            if "orders" in names:
                for old in conn.execute("SELECT DISTINCT user_id FROM orders WHERE status='DELIVERED'").fetchall():
                    if not self._by_user(conn, old[0]):
                        self._insert(conn, old[0], 0, legacy=True)
            conn.execute("INSERT INTO debris_schema VALUES (?,?)", (SCHEMA_VERSION, now))
        with closing(self.connect()) as conn:
            conn.execute("PRAGMA journal_mode=WAL")

    def _by_user(self, conn, user_id):
        return conn.execute("SELECT * FROM debris_licenses WHERE user_id=?", (user_id,)).fetchone()

    def _insert(self, conn, user_id, duration, *, legacy=False, blocked=False, created=None):
        if type(user_id) is not int or not 0 < user_id < 2**63:
            raise ValueError("Нужен положительный Telegram ID.")
        now = int(self.clock())
        for _ in range(10):
            try:
                conn.execute("""INSERT INTO debris_licenses
                    (user_id,debris_id,expires_at,blocked,legacy_lifetime,created_at)
                    VALUES (?,?,?,?,?,?)""", (user_id, new_id(), now + duration,
                    int(blocked), int(legacy), now if created is None else created))
                break
            except sqlite3.IntegrityError:
                if self._by_user(conn, user_id):
                    raise ValueError("У пользователя уже есть ID.") from None
        else:
            raise RuntimeError("Could not allocate unique ID")
        self._audit(conn, user_id, "MIGRATE" if legacy else "CREATE")
        return self._by_user(conn, user_id)

    def _audit(self, conn, user_id, action, actor_id=None, details=""):
        conn.execute("INSERT INTO debris_license_audit(user_id,action,actor_id,created_at,details) VALUES (?,?,?,?,?)",
                     (user_id, action, actor_id, int(self.clock()), details))

    def create_license(self, user_id: int, duration: int = 0, *, actor_id=None):
        if duration != 0:
            duration_seconds(duration)
        with self.transaction() as conn:
            row = self._by_user(conn, user_id)
            if not row:
                row = self._insert(conn, user_id, duration)
                self._audit(conn, user_id, "ADMIN_CREATE", actor_id)
            return self._info(row)

    def extend_license(self, user_id: int, duration: int, *, actor_id=None, event_key: str | None = None):
        """Future payment adapter calls this AFTER confirmed payment.

        Supply a unique provider event key to make retries idempotent.
        Existing lifetime entitlements remain lifetime when merely extended.
        """
        duration_seconds(duration)
        if event_key is not None and (not isinstance(event_key, str) or not 1 <= len(event_key) <= 200):
            raise ValueError("Invalid event key")
        with self.transaction() as conn:
            if event_key:
                previous = conn.execute("SELECT * FROM debris_license_events WHERE event_key=?", (event_key,)).fetchone()
                if previous:
                    if previous["user_id"] != user_id or previous["duration"] != duration:
                        raise ValueError("Event key reused with different arguments")
                    return self._info(self._by_user(conn, user_id))
            row = self._by_user(conn, user_id) or self._insert(conn, user_id, 0)
            expires = max(int(self.clock()), row["expires_at"]) + duration
            conn.execute("UPDATE debris_licenses SET expires_at=? WHERE user_id=?", (expires, user_id))
            if event_key:
                conn.execute("INSERT INTO debris_license_events VALUES (?,?,?,?)", (event_key, user_id, duration, int(self.clock())))
            self._audit(conn, user_id, "EXTEND", actor_id, str(duration))
            return self._info(self._by_user(conn, user_id))

    def set_duration(self, user_id: int, duration: int, *, actor_id=None):
        duration_seconds(duration)
        return self.set_expiry(user_id, int(self.clock()) + duration, actor_id=actor_id)

    def set_expiry(self, user_id: int, expires_at: int, *, actor_id=None):
        if type(expires_at) is not int or not 0 <= expires_at <= int(self.clock()) + MAX_DURATION:
            raise ValueError("Недопустимая дата окончания.")
        with self.transaction() as conn:
            if not self._by_user(conn, user_id):
                raise ValueError("Сначала создайте ID.")
            conn.execute("UPDATE debris_licenses SET expires_at=?,legacy_lifetime=0 WHERE user_id=?", (expires_at, user_id))
            self._audit(conn, user_id, "SET_EXPIRY", actor_id, str(expires_at))
            return self._info(self._by_user(conn, user_id))

    def set_blocked(self, user_id: int, blocked: bool, *, actor_id=None):
        with self.transaction() as conn:
            if not self._by_user(conn, user_id):
                raise ValueError("Лицензия не найдена.")
            conn.execute("UPDATE debris_licenses SET blocked=? WHERE user_id=?", (int(blocked), user_id))
            self._audit(conn, user_id, "BLOCK" if blocked else "UNBLOCK", actor_id)
            return self._info(self._by_user(conn, user_id))

    def reset_device(self, user_id: int, *, actor_id=None):
        with self.transaction() as conn:
            if not self._by_user(conn, user_id):
                raise ValueError("Лицензия не найдена.")
            conn.execute("""UPDATE debris_licenses SET device_hash=NULL,token_hash=NULL,
                         binding_version=binding_version+1,reset_at=? WHERE user_id=?""", (int(self.clock()), user_id))
            self._audit(conn, user_id, "RESET_DEVICE", actor_id)
            return self._info(self._by_user(conn, user_id))

    def _info(self, row):
        if row is None:
            return None
        now = int(self.clock())
        return {"user_id": row["user_id"], "debris_id": row["debris_id"],
                "status": status_of(row, now),
                "expires_at": None if row["legacy_lifetime"] else row["expires_at"],
                "remaining_seconds": None if row["legacy_lifetime"] else max(0, row["expires_at"] - now),
                "legacy_lifetime": bool(row["legacy_lifetime"]), "device_bound": row["device_hash"] is not None,
                "binding_version": row["binding_version"], "created_at": row["created_at"],
                "activated_at": row["activated_at"], "last_check_at": row["last_check_at"], "reset_at": row["reset_at"]}

    def get_license(self, user: int | str):
        with closing(self.connect()) as conn:
            if isinstance(user, int):
                row = self._by_user(conn, user)
            else:
                row = conn.execute("SELECT * FROM debris_licenses WHERE debris_id=?", (normalize_id(user),)).fetchone()
            return self._info(row)

    def list_licenses(self, offset=0, limit=10):
        with closing(self.connect()) as conn:
            return [self._info(r) for r in conn.execute("SELECT * FROM debris_licenses ORDER BY created_at,user_id LIMIT ? OFFSET ?", (limit, offset))]

    def sync_legacy_purchases(self):
        """Preserve the old bot's lifetime delivery contract, including resumed orders.

        Never modifies or extends an existing Debris ID. Future subscription
        payments must use extend_license instead of this legacy bridge.
        """
        with self.transaction() as conn:
            if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='licenses'").fetchone():
                return
            rows = conn.execute("""SELECT user_id,active FROM licenses
                WHERE NOT EXISTS (SELECT 1 FROM debris_licenses d WHERE d.user_id=licenses.user_id)""").fetchall()
            for row in rows:
                self._insert(conn, row["user_id"], 0, legacy=True, blocked=not bool(row["active"]))

    def health(self):
        with closing(self.connect()) as conn:
            return conn.execute("SELECT MAX(version) FROM debris_schema").fetchone()[0] == SCHEMA_VERSION

    def authorize(self, action: str, debris_id: str, device_id: str, binding_token: str | None = None):
        """check NEVER binds. Only the explicit activate endpoint creates tokens."""
        debris_id = normalize_id(debris_id)
        if action not in {"activate", "check"}:
            raise ValueError("Invalid action")
        if not isinstance(device_id, str) or not DEVICE_PATTERN.fullmatch(device_id):
            raise ValueError("Invalid device_id")
        if binding_token is not None and (not isinstance(binding_token, str) or not TOKEN_PATTERN.fullmatch(binding_token)):
            raise ValueError("Invalid binding_token")
        with self.transaction() as conn:
            row = conn.execute("SELECT * FROM debris_licenses WHERE debris_id=?", (debris_id,)).fetchone()
            now = float(self.clock())
            def result(status, **extra):
                return {"status": status, "code": status, "server_time": now, **extra}
            if row is None:
                return result("INVALID_LICENSE")
            # Check token before entitlement, so reset is visible even on expired licenses.
            if action == "check":
                if not binding_token:
                    return result("REAUTH_REQUIRED")
                if row["token_hash"] is None or not hmac.compare_digest(row["token_hash"], digest(binding_token)):
                    return result("DEVICE_RESET")
            if row["device_hash"] is not None and not hmac.compare_digest(row["device_hash"], digest(device_id)):
                return result("DEVICE_MISMATCH")
            status = status_of(row, now)
            if status != "ACTIVE":
                return result(status)
            token = None
            if action == "activate":
                token = secrets.token_urlsafe(32)
                conn.execute("""UPDATE debris_licenses SET device_hash=?,token_hash=?,
                             binding_version=binding_version+1,activated_at=? WHERE user_id=?""",
                             (digest(device_id), digest(token), int(now), row["user_id"]))
                self._audit(conn, row["user_id"], "ACTIVATE")
            conn.execute("UPDATE debris_licenses SET last_check_at=? WHERE user_id=?", (int(now), row["user_id"]))
            lease = self.lease_seconds if row["legacy_lifetime"] else min(self.lease_seconds, row["expires_at"] - now)
            extra = {"expires_at": None if row["legacy_lifetime"] else row["expires_at"],
                     "lease_seconds": lease, "check_interval_seconds": self.check_interval}
            if token:
                extra["binding_token"] = token
            return result("ACTIVE", **extra)
