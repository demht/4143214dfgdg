"""Public license API. Run separately with python license_api.py, or inside bot.py."""
import asyncio
import ipaddress
import json
import logging
import os
import sqlite3
import time
from collections import OrderedDict

from aiohttp import web
from dotenv import load_dotenv

from subscription_store import SubscriptionStore

log = logging.getLogger(__name__)
SERVICE = web.AppKey("license_service", SubscriptionStore)


def service_from_env():
    return SubscriptionStore(os.getenv("DB_PATH", "bot.db"),
                          check_interval=int(os.getenv("LICENSE_CHECK_INTERVAL_SECONDS", "5")),
                          lease_seconds=int(os.getenv("LICENSE_LEASE_SECONDS", "15")))


class RateLimiter:
    """Bounded per-process buckets; production proxy must also enforce a limit."""
    def __init__(self):
        self.buckets = OrderedDict()

    def allow(self, key, limit, period=60):
        now = time.monotonic()
        count, start = self.buckets.pop(key, (0, now))
        if now - start >= period:
            count, start = 0, now
        self.buckets[key] = (count + 1, start)
        while len(self.buckets) > 10000:
            self.buckets.popitem(last=False)
        return count < limit


def response(code, http_status=200, **extra):
    return web.json_response({"status": code, "code": code, **extra}, status=http_status,
                             headers={"Cache-Control": "no-store"})


def create_app(service: SubscriptionStore, *, trusted_proxies=()):
    limiter = RateLimiter()
    proxy_addresses = {str(ipaddress.ip_address(x)) for x in trusted_proxies}

    @web.middleware
    async def boundary(request, handler):
        try:
            peer = request.remote or "unknown"
            # Only an explicitly trusted local proxy may supply this single IP.
            # The proxy configuration overwrites the header; it never appends.
            if peer in proxy_addresses and "X-Real-IP" in request.headers:
                peer = str(ipaddress.ip_address(request.headers["X-Real-IP"]))
            if not limiter.allow(("global", "all"), 3000):
                return response("RATE_LIMITED", 429)
            action = "activate" if request.path.endswith("/activate") else "other"
            if not limiter.allow((action, peer), 10 if action == "activate" else 300):
                return response("RATE_LIMITED", 429)
            return await handler(request)
        except (ValueError, json.JSONDecodeError, UnicodeDecodeError):
            return response("BAD_REQUEST", 400)
        except web.HTTPException as exc:
            return response("BAD_REQUEST" if exc.status == 413 else "HTTP_ERROR", exc.status)
        except sqlite3.Error:
            log.error("License database operation failed")
            return response("SERVICE_UNAVAILABLE", 503)
        except Exception:
            # Do not include request payloads, IDs, tokens or database row values.
            log.error("Unexpected license request failure")
            return response("INTERNAL_ERROR", 500)

    app = web.Application(client_max_size=4096, middlewares=[boundary])
    app[SERVICE] = service

    async def health(request):
        if not await asyncio.to_thread(service.health):
            return response("SERVICE_UNAVAILABLE", 503)
        return response("OK")

    async def authorize(request):
        if request.content_type != "application/json":
            return response("UNSUPPORTED_MEDIA_TYPE", 415)
        data = await request.json()
        if not isinstance(data, dict) or set(data) - {"debris_id", "device_id", "binding_token"}:
            return response("BAD_REQUEST", 400)
        if not {"debris_id", "device_id"} <= data.keys():
            return response("BAD_REQUEST", 400)
        result = await asyncio.to_thread(service.authorize, request.match_info["action"],
                                        data["debris_id"], data["device_id"], data.get("binding_token"))
        return web.json_response(result, headers={"Cache-Control": "no-store"})

    app.router.add_get("/api/v1/health", health)
    app.router.add_post("/api/v1/license/{action:activate|check}", authorize)
    return app


async def start_api(service):
    proxies = [x.strip() for x in os.getenv("LICENSE_TRUSTED_PROXIES", "").split(",") if x.strip()]
    runner = web.AppRunner(create_app(service, trusted_proxies=proxies), access_log=None, shutdown_timeout=10)
    await runner.setup()
    try:
        await web.TCPSite(runner, os.getenv("LICENSE_API_HOST", "127.0.0.1"),
                          int(os.getenv("LICENSE_API_PORT", "8081"))).start()
    except BaseException:
        await runner.cleanup()
        raise
    return runner


async def main():
    load_dotenv()
    service = service_from_env()
    await asyncio.to_thread(service.migrate)
    runner = await start_api(service)
    try:
        await asyncio.Event().wait()
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
