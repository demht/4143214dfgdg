"""License controls added to the existing bot, with authorization on every event."""
import asyncio
import html
import logging
from datetime import datetime, timezone

from aiogram import BaseMiddleware, F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton as Button, InlineKeyboardMarkup as Markup, Message

from license_service import parse_duration


class LicenseState(StatesGroup):
    user = State()
    duration = State()
    date = State()


def keyboard(rows):
    return Markup(inline_keyboard=[[Button(text=text, callback_data=data) for text, data in row] for row in rows])


def format_date(value):
    return "—" if value is None else datetime.fromtimestamp(value, timezone.utc).strftime("%d.%m.%Y %H:%M:%S UTC")


def license_text(row):
    remaining = row["remaining_seconds"]
    if remaining is None:
        left = "бессрочно (сохранённая старая покупка)"
    else:
        days, seconds = divmod(remaining, 86400)
        hours, seconds = divmod(seconds, 3600)
        minutes, seconds = divmod(seconds, 60)
        left = f"{days}д {hours}ч {minutes}м {seconds}с"
    return (f"<b>Лицензия Debris</b>\nID: <code>{row['debris_id']}</code>\n"
            f"Telegram: <code>{row['user_id']}</code>\nСтатус: <b>{row['status']}</b>\n"
            f"Окончание: {format_date(row['expires_at'])}\nОсталось: {left}\n"
            f"Устройство: {'привязано' if row['device_bound'] else 'не привязано'}\n"
            f"Создана: {format_date(row['created_at'])}\n"
            f"Активация: {format_date(row['activated_at'])}\nПроверка: {format_date(row['last_check_at'])}\n"
            f"Сброс: {format_date(row['reset_at'])}")


def actions(user_id, exists=True):
    rows = []
    if exists:
        rows += [[("Добавить время", f"lic:extend:{user_id}"), ("Установить срок", f"lic:set:{user_id}")],
                 [("Точная дата UTC", f"lic:date:{user_id}")],
                 [("Заблокировать", f"lic:block:{user_id}"), ("Разблокировать", f"lic:unblock:{user_id}")],
                 [("Сбросить устройство", f"lic:reset:{user_id}")],
                 [("Обновить информацию", f"lic:info:{user_id}")]]
    else:
        rows += [[("Создать ID", f"lic:create:{user_id}")]]
    rows += [[("Все лицензии", "lic:list:0"), ("Поиск / новый пользователь", "lic:search")],
             [("В админ-панель", "admin_home")]]
    return keyboard(rows)


class AdminGuard(BaseMiddleware):
    def __init__(self, admin_ids):
        self.admin_ids = admin_ids

    async def __call__(self, handler, event, data):
        chat = getattr(getattr(event, "message", event), "chat", None)
        if chat is None or chat.type != "private":
            if isinstance(event, CallbackQuery):
                await event.answer("Управление лицензиями доступно в личном чате с ботом.", show_alert=True)
            else:
                await event.answer("Управление лицензиями доступно в личном чате с ботом.")
            return
        if event.from_user.id not in self.admin_ids:
            if isinstance(event, CallbackQuery):
                await event.answer("Нет доступа.", show_alert=True)
            else:
                await event.answer("Нет доступа.")
            return
        try:
            return await handler(event, data)
        except ValueError as exc:
            text = html.escape(str(exc))
            if isinstance(event, CallbackQuery):
                await event.answer(text[:180], show_alert=True)
            else:
                await event.answer(text)
        except Exception:
            logging.error("License admin operation failed")
            if isinstance(event, CallbackQuery):
                await event.answer("Ошибка сервиса. Повторите позже.", show_alert=True)
            else:
                await event.answer("Ошибка сервиса. Повторите позже.")


def create_router(service, admin_ids):
    router = Router(name="license_admin")
    router.message.middleware(AdminGuard(admin_ids))
    router.callback_query.middleware(AdminGuard(admin_ids))

    async def show(message, user_id):
        row = await asyncio.to_thread(service.get_license, user_id)
        await message.answer(license_text(row) if row else f"У пользователя {user_id} ещё нет Debris ID.",
                             reply_markup=actions(user_id, row is not None), parse_mode="HTML")

    @router.message(Command("license"))
    async def command(message: Message, state: FSMContext):
        await state.clear()
        parts = (message.text or "").split(maxsplit=1)
        if len(parts) == 2:
            await lookup(message, parts[1])
        else:
            await state.set_state(LicenseState.user)
            await message.answer("Введите Telegram ID пользователя или Debris ID. Отмена: /cancel")

    async def lookup(message, value):
        if value.isascii() and value.isdigit() and 0 < int(value) < 2**63:
            await show(message, int(value))
        else:
            row = await asyncio.to_thread(service.get_license, value)
            if row:
                await show(message, row["user_id"])
            else:
                await message.answer("Лицензия не найдена.")

    @router.message(LicenseState.user, F.text, ~F.text.startswith("/"))
    async def user_input(message: Message, state: FSMContext):
        await lookup(message, message.text.strip())
        await state.clear()

    @router.callback_query(F.data.startswith("lic:"))
    async def control(callback: CallbackQuery, state: FSMContext):
        parts = callback.data.split(":")
        action = parts[1]
        if action == "search":
            await state.clear()
            await state.set_state(LicenseState.user)
            await callback.message.answer("Введите Telegram ID или Debris ID. Отмена: /cancel")
        elif action == "list":
            await state.clear()
            offset = max(0, min(int(parts[2]), 1000000))
            rows = await asyncio.to_thread(service.list_licenses, offset, 11)
            buttons = [[(f"{r['user_id']} · {r['status']} · {r['debris_id']}", f"lic:info:{r['user_id']}")] for r in rows[:10]]
            nav = []
            if offset:
                nav.append(("Назад", f"lic:list:{max(0, offset-10)}"))
            if len(rows) > 10:
                nav.append(("Далее", f"lic:list:{offset+10}"))
            if nav:
                buttons.append(nav)
            buttons += [[("Поиск / создать ID", "lic:search")], [("В админ-панель", "admin_home")]]
            await callback.message.answer("Лицензии Debris:", reply_markup=keyboard(buttons))
        else:
            user_id = int(parts[2])
            actor_id = callback.from_user.id
            if action in {"create", "extend", "set"}:
                rows = [[(label, f"lic:term:{user_id}:{action}:{term}")] for label, term in
                        [("2 минуты", "2m"), ("10 минут", "10m"), ("1 час", "1h"), ("1 день", "1d"),
                         ("7 дней", "7d"), ("30 дней", "30d"), ("90 дней", "90d")]]
                rows.append([("Свой срок", f"lic:custom:{user_id}:{action}")])
                if action == "create":
                    rows.append([("Только ID, без времени", f"lic:empty:{user_id}")])
                await callback.message.answer("Выберите срок. «Установить» заменяет окончание на сейчас + срок.", reply_markup=keyboard(rows))
            elif action == "custom":
                await state.set_state(LicenseState.duration)
                await state.update_data(user_id=user_id, operation=parts[3])
                await callback.message.answer("Введите 30s, 2m, 3h или 7d. Отмена: /cancel")
            elif action == "date":
                await state.set_state(LicenseState.date)
                await state.update_data(user_id=user_id)
                await callback.message.answer("Введите дату UTC: 2026-09-30 18:00:00. Отмена: /cancel")
            elif action == "term":
                await apply_term(user_id, parts[3], parse_duration(parts[4]), actor_id)
                await state.clear()
                await show(callback.message, user_id)
            else:
                if action == "empty":
                    await asyncio.to_thread(service.create_license, user_id, actor_id=actor_id)
                elif action in {"block", "unblock"}:
                    await asyncio.to_thread(service.set_blocked, user_id, action == "block", actor_id=actor_id)
                elif action == "reset":
                    await asyncio.to_thread(service.reset_device, user_id, actor_id=actor_id)
                elif action != "info":
                    raise ValueError("Неизвестная команда.")
                await state.clear()
                await show(callback.message, user_id)
        await callback.answer()

    async def apply_term(user_id, operation, duration, actor_id):
        func = {"create": service.create_license, "extend": service.extend_license, "set": service.set_duration}.get(operation)
        if func is None:
            raise ValueError("Неизвестная операция.")
        await asyncio.to_thread(func, user_id, duration, actor_id=actor_id)

    @router.message(LicenseState.duration, F.text, ~F.text.startswith("/"))
    async def duration_input(message: Message, state: FSMContext):
        data = await state.get_data()
        await apply_term(data["user_id"], data["operation"], parse_duration(message.text), message.from_user.id)
        await state.clear()
        await show(message, data["user_id"])

    @router.message(LicenseState.date, F.text, ~F.text.startswith("/"))
    async def date_input(message: Message, state: FSMContext):
        try:
            date = datetime.strptime(message.text.strip(), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        except ValueError:
            raise ValueError("Формат UTC: 2026-09-30 18:00:00") from None
        data = await state.get_data()
        await asyncio.to_thread(service.set_expiry, data["user_id"], int(date.timestamp()), actor_id=message.from_user.id)
        await state.clear()
        await show(message, data["user_id"])

    return router
