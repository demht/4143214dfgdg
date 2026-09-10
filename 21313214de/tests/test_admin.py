import asyncio
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock

from aiogram import Bot, Dispatcher
from aiogram.client.session.base import BaseSession
from aiogram.methods import TelegramMethod
from aiogram.types import Update, User
from license_admin import create_router, license_text
from license_service import LicenseService


class FakeTelegramSession(BaseSession):
    """Records outgoing methods in memory. Never contacts Telegram."""
    def __init__(self):
        super().__init__()
        self.methods = []
    async def close(self): pass
    async def make_request(self, bot, method, timeout=None):
        self.methods.append(method)
        return True
    async def stream_content(self, *args, **kwargs):
        if False: yield b""


class AdminTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = LicenseService(Path(self.tmp.name) / "bot.db")
        self.service.migrate()
        self.telegram = FakeTelegramSession()
        self.bot = Bot("123456:FAKE_TEST_TOKEN", session=self.telegram)
        self.dp = Dispatcher()
        self.dp.include_router(create_router(self.service, {99}))
        self.number = 0

    async def asyncTearDown(self):
        await self.dp.storage.close()
        await self.bot.session.close()
        self.tmp.cleanup()

    async def callback(self, data, actor=99):
        self.number += 1
        update = Update.model_validate({"update_id": self.number, "callback_query": {
            "id": str(self.number), "from": {"id": actor, "is_bot": False, "first_name": "Test"},
            "chat_instance": "1", "data": data,
            "message": {"message_id": 1, "date": 1900000000, "chat": {"id": actor, "type": "private"}, "text": "test"}}})
        await self.dp.feed_update(self.bot, update)

    async def message(self, text, actor=99):
        self.number += 1
        update = Update.model_validate({"update_id": self.number, "message": {
            "message_id": self.number, "date": 1900000000, "chat": {"id": actor, "type": "private"},
            "from": {"id": actor, "is_bot": False, "first_name": "Test"}, "text": text}})
        await self.dp.feed_update(self.bot, update)

    async def test_admin_buttons_and_custom_fsm(self):
        await self.callback("lic:search")
        await self.message("123")
        await self.callback("lic:create:123")
        markup = next(m.reply_markup for m in reversed(self.telegram.methods) if getattr(m, "reply_markup", None))
        labels = [b.text for row in markup.inline_keyboard for b in row]
        self.assertIn("2 минуты", labels)
        self.assertIn("90 дней", labels)
        self.assertIn("Свой срок", labels)
        await self.callback("lic:term:123:create:2m")
        original = self.service.get_license(123)
        self.assertEqual(original["status"], "ACTIVE")
        await self.callback("lic:custom:123:extend")
        await self.message("30s")
        self.assertGreaterEqual(self.service.get_license(123)["remaining_seconds"], 148)
        await self.callback("lic:block:123")
        self.assertEqual(self.service.get_license(123)["status"], "BLOCKED")
        await self.callback("lic:unblock:123")
        self.assertEqual(self.service.get_license(123)["status"], "ACTIVE")
        await self.callback("lic:reset:123")
        await self.callback("lic:term:123:set:10m")
        current = self.service.get_license(123)
        self.assertEqual(current["debris_id"], original["debris_id"])
        self.assertGreaterEqual(current["remaining_seconds"], 599)
        self.assertIn("UTC", license_text(current))

    async def test_nonadmin_cannot_mutate_even_with_forged_callbacks(self):
        self.service.create_license(123, 120)
        for data in ["lic:block:123", "lic:reset:123", "lic:term:123:set:1s", "lic:empty:456", "lic:list:0"]:
            await self.callback(data, actor=55)
        self.assertEqual(self.service.get_license(123)["status"], "ACTIVE")
        self.assertIsNone(self.service.get_license(456))
        self.assertEqual(self.service.get_license(123)["binding_version"], 0)

    async def test_bad_duration_does_not_lose_state(self):
        self.service.create_license(123, 120)
        await self.callback("lic:custom:123:set")
        await self.message("invalid")
        await self.message("30s")
        self.assertLessEqual(self.service.get_license(123)["remaining_seconds"], 30)


if __name__ == "__main__": unittest.main()
