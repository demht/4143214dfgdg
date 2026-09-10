"""Real elapsed-time integration of Java LicenseClient + aiohttp + SQLite.

No Minecraft instance, Telegram account, external API or real customer is used.
Set TEST_JAVA_CLASSPATH to compiled ClientProbe/LicenseClient plus Gson.
"""
import asyncio
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time

from aiohttp import web
from license_service import LicenseService
from license_api import create_app


async def main():
    classpath = os.environ["TEST_JAVA_CLASSPATH"]
    processes = []
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        service = LicenseService(root / "bot.db")
        service.migrate()
        runner = None

        async def server(port=0):
            nonlocal runner
            runner = web.AppRunner(create_app(service), access_log=None)
            await runner.setup()
            site = web.TCPSite(runner, "127.0.0.1", port)
            await site.start()
            return site._server.sockets[0].getsockname()[1]

        port = await server()

        async def probe(name, machine):
            directory = root / name
            directory.mkdir(exist_ok=True)
            (directory / "debris-license-api.properties").write_text(
                f"api_url=http://127.0.0.1:{port}\nallow_local_http=true\nrequest_timeout_seconds=2\n")
            process = await asyncio.create_subprocess_exec("java", "-cp", classpath, "ClientProbe", str(directory),
                hashlib.sha256(machine.encode()).hexdigest(), stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            processes.append(process)
            return process

        async def command(process, text="STATUS"):
            process.stdin.write((text + "\n").encode())
            await process.stdin.drain()
            line = await asyncio.wait_for(process.stdout.readline(), 15)
            if not line:
                raise AssertionError("Java probe stopped: " + (await process.stderr.read()).decode(errors="replace")[:1000])
            return json.loads(line)

        async def until(process, predicate, timeout=9):
            end = time.monotonic() + timeout
            while time.monotonic() < end:
                result = await command(process)
                if predicate(result): return result
                await asyncio.sleep(0.1)
            raise AssertionError(f"Client state timeout: {result}")

        async def close(process):
            if process.returncode is None:
                process.stdin.write(b"CLOSE\n")
                await process.stdin.drain()
                await asyncio.wait_for(process.wait(), 15)

        try:
            test_seconds = int(os.getenv("LIVE_TEST_SECONDS", "120"))
            short = service.create_license(1001, test_seconds)
            first = await probe("short", "A")
            assert not (await command(first))["allowed"]
            await command(first, "ACTIVATE " + short["debris_id"])
            await until(first, lambda r: r["allowed"])
            print(f"PASS: real {test_seconds}-second license ACTIVE; Java process remains running", flush=True)

            row = service.create_license(1002, 600)
            second = await probe("long", "A")
            await command(second, "ACTIVATE " + row["debris_id"])
            await until(second, lambda r: r["allowed"])
            old_token = json.loads((root / "long/debris-license.json").read_text())["binding_token"]
            service.reset_device(1002)
            await until(second, lambda r: not r["allowed"] and r["status"] == "DEVICE_RESET")
            assert "binding_token" not in json.loads((root / "long/debris-license.json").read_text())
            await asyncio.sleep(6)
            assert not service.get_license(1002)["device_bound"]
            print("PASS: reset revokes running Java client, removes local token, no automatic binding", flush=True)
            await command(second, "ACTIVATE " + row["debris_id"])
            await until(second, lambda r: r["allowed"])
            assert service.authorize("check", row["debris_id"], hashlib.sha256(b"A").hexdigest(), old_token)["status"] == "DEVICE_RESET"
            print("PASS: explicit reactivation works; old token remains revoked", flush=True)

            other = await probe("other", "B")
            await command(other, "ACTIVATE " + row["debris_id"])
            await until(other, lambda r: r["status"] == "DEVICE_MISMATCH")
            assert not (await command(other))["allowed"]
            print("PASS: other device refused", flush=True)

            service.set_blocked(1002, True)
            await until(second, lambda r: not r["allowed"] and r["status"] == "BLOCKED")
            service.set_blocked(1002, False)
            await until(second, lambda r: r["allowed"])
            print("PASS: block/unblock changes running Java access", flush=True)

            await close(second)
            second = await probe("long", "A")
            await until(second, lambda r: r["allowed"])
            print("PASS: saved binding checks successfully after Java restart", flush=True)

            # Keep the real two-minute client running until its server expiry.
            while time.time() < short["expires_at"]:
                remaining = short["expires_at"] - time.time()
                print(f"Real expiry test: {max(0, int(remaining))} seconds remaining", flush=True)
                await asyncio.sleep(min(20, remaining))
            await until(first, lambda r: not r["allowed"], timeout=2)
            await until(first, lambda r: r["status"] == "EXPIRED", timeout=7)
            assert first.returncode is None
            print(f"PASS: real {test_seconds} seconds -> EXPIRED, same Java process, no restart", flush=True)
            renewed = service.extend_license(1001, 120)
            assert renewed["debris_id"] == short["debris_id"]
            await until(first, lambda r: r["allowed"])
            print("PASS: expired ID renewed -> running Java client ACTIVE", flush=True)

            await runner.cleanup()
            await until(second, lambda r: not r["allowed"], timeout=17)
            # A copied saved credential never authorizes offline on startup.
            await close(second)
            second = await probe("long", "A")
            await until(second, lambda r: r["status"] == "NETWORK_ERROR", timeout=15)
            assert not (await command(second))["allowed"]
            print("PASS: API outage revokes access within lease; offline restart grants no access", flush=True)
            await server(port)
            await until(second, lambda r: r["allowed"])
            assert service.get_license(1002)["debris_id"] == row["debris_id"]
            print("PASS: API restart preserves license and binding", flush=True)
            print("ALL LIVE JAVA/API SCENARIOS PASSED", flush=True)
        finally:
            for process in processes:
                if process.returncode is None:
                    try: await close(process)
                    except Exception:
                        process.kill()
                        await process.wait()
            if runner: await runner.cleanup()


if __name__ == "__main__": asyncio.run(main())
