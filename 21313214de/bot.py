import asyncio
import html
import logging
import os
import shutil
import secrets
import sqlite3
from subscription_store import SubscriptionStore
from cloudflare_store import CloudflareStore
from cloudflare_license_client import CloudflareLicenseError, DEFAULT_LICENSE_URL
from license_api import start_api, service_from_env
from jar_delivery import prepare_jar, inspect_jar
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

import aiohttp
from aiohttp import web

from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LabeledPrice,
    Message,
    PreCheckoutQuery,
)
from dotenv import load_dotenv
from sqlalchemy import BigInteger, Boolean, DateTime, Integer, String, Text, func, select
from sqlalchemy.ext.asyncio import AsyncAttrs, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

APP_DIR = Path(__file__).resolve().parent
load_dotenv(APP_DIR / '.env')
os.chdir(APP_DIR)
os.environ.setdefault('LICENSE_PUBLIC_URL', DEFAULT_LICENSE_URL)
logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_IDS = {
    int(x.strip())
    for x in os.getenv("ADMIN_IDS", "").split(",")
    if x.strip().isdigit()
}

PLATEGA_BASE_URL = os.getenv("PLATEGA_BASE_URL", "https://app.platega.io").rstrip("/")
PLATEGA_MERCHANT_ID = os.getenv("PLATEGA_MERCHANT_ID", "").strip()
PLATEGA_SECRET = os.getenv("PLATEGA_SECRET", "").strip()
PLATEGA_POLL_INTERVAL = max(5, int(os.getenv("PLATEGA_POLL_INTERVAL", "10")))
PLATEGA_WEBHOOK_ENABLED = os.getenv("PLATEGA_WEBHOOK_ENABLED", "0").strip() == "1"
PLATEGA_WEBHOOK_HOST = os.getenv("PLATEGA_WEBHOOK_HOST", "0.0.0.0").strip()
PLATEGA_WEBHOOK_PORT = int(os.getenv("PLATEGA_WEBHOOK_PORT", "8080"))

MOD_FILE_PATH = os.getenv("MOD_FILE_PATH", "Debris-1.21.8-integrated.jar").strip()
DELIVERY_TEXT = os.getenv(
    "DELIVERY_TEXT",
    "✅ Debris выдан. Спасибо за покупку!",
)

PROMO_CODES = {
    x.strip().lower()
    for x in os.getenv("PROMO_CODES", "debris,bust,maksudax").split(",")
    if x.strip()
}


def _load_tariffs() -> dict[int, dict[str, int]]:
    # Format: days:rub:xtr:promo_rub:promo_xtr,days:...
    # xtr/promo_xtr may be 0 to disable Telegram Stars for a tariff.
    raw = os.getenv("TARIFFS", "").strip()
    if not raw:
        return {
            30: {"rub": 150, "xtr": 145, "promo_rub": 125, "promo_xtr": 120},
            90: {"rub": 350, "xtr": 349, "promo_rub": 300, "promo_xtr": 299},
            # No 180-day Stars price was specified, so Stars are disabled for this tariff by default.
            180: {"rub": 799, "xtr": 0, "promo_rub": 690, "promo_xtr": 0},
        }
    result: dict[int, dict[str, int]] = {}
    for item in raw.split(","):
        parts = [x.strip() for x in item.split(":")]
        if len(parts) != 5:
            raise RuntimeError(f"Bad TARIFFS item: {item!r}")
        days, rub, xtr, promo_rub, promo_xtr = map(int, parts)
        if min(days, rub, promo_rub) <= 0 or xtr < 0 or promo_xtr < 0:
            raise RuntimeError(f"Bad TARIFFS values: {item!r}")
        if (xtr == 0) != (promo_xtr == 0):
            raise RuntimeError(f"Both Stars prices must be 0 (disabled) or both positive: {item!r}")
        result[days] = {
            "rub": rub,
            "xtr": xtr,
            "promo_rub": promo_rub,
            "promo_xtr": promo_xtr,
        }
    if not result:
        raise RuntimeError("TARIFFS is empty")
    return result


TARIFFS = _load_tariffs()
DEFAULT_TARIFF_DAYS = 30 if 30 in TARIFFS else sorted(TARIFFS)[0]

DB_PATH = os.getenv("DB_PATH", "bot.db").strip()
START_IMAGE_PATH = os.getenv("START_IMAGE_PATH", "assets/debris_banner.png").strip()
RELEASES_DIR = os.getenv("RELEASES_DIR", "releases").strip()

# Debris ID licensing
LICENSE_DAYS = max(1, int(os.getenv("LICENSE_DAYS", "30")))
LICENSE_API_ENABLED = os.getenv("LICENSE_API_ENABLED", "0").strip() == "1"
LICENSE_API_HOST = os.getenv("LICENSE_API_HOST", "0.0.0.0").strip()
LICENSE_API_PORT = int(os.getenv("LICENSE_API_PORT", "8081"))

store = CloudflareStore(DB_PATH,
    check_interval=int(os.getenv('LICENSE_CHECK_INTERVAL_SECONDS', '5')),
    lease_seconds=int(os.getenv('LICENSE_LEASE_SECONDS', '15')))
PAYMENTS_ENABLED = os.getenv('PAYMENTS_ENABLED', '1').strip() == '1'


class Base(AsyncAttrs, DeclarativeBase):
    pass


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, index=True)
    username: Mapped[str | None] = mapped_column(String(64), nullable=True)

    payment_method: Mapped[str] = mapped_column(String(16))
    amount: Mapped[int] = mapped_column(Integer)
    currency: Mapped[str] = mapped_column(String(8))
    status: Mapped[str] = mapped_column(String(16), index=True)

    promo_code: Mapped[str | None] = mapped_column(String(64), nullable=True)

    telegram_payment_charge_id: Mapped[str | None] = mapped_column(
        String(256), unique=True, nullable=True
    )
    provider_payment_charge_id: Mapped[str | None] = mapped_column(
        String(256), nullable=True
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )
    paid_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    duration_seconds: Mapped[int | None] = mapped_column(Integer, nullable=True)


class License(Base):
    __tablename__ = "licenses"

    user_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    username: Mapped[str | None] = mapped_column(String(64), nullable=True)
    source_order_id: Mapped[int] = mapped_column(Integer, index=True)
    purchased_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    license_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    device_hash: Mapped[str | None] = mapped_column(String(128), nullable=True)
    activated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    binding_token: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_version_sent: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_release_sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class Release(Base):
    __tablename__ = "releases"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    version: Mapped[str] = mapped_column(String(64), index=True)
    file_path: Mapped[str] = mapped_column(String(512))
    file_name: Mapped[str] = mapped_column(String(256))
    notes: Mapped[str] = mapped_column(Text, default="")
    created_by: Mapped[int] = mapped_column(BigInteger)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    status: Mapped[str] = mapped_column(String(16), default="READY", index=True)
    total_count: Mapped[int] = mapped_column(Integer, default=0)
    sent_count: Mapped[int] = mapped_column(Integer, default=0)
    failed_count: Mapped[int] = mapped_column(Integer, default=0)


class ReleaseDelivery(Base):
    __tablename__ = "release_deliveries"

    release_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    status: Mapped[str] = mapped_column(String(16), index=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


engine = create_async_engine(f"sqlite+aiosqlite:///{DB_PATH}")
Session = async_sessionmaker(engine, expire_on_commit=False)
router = Router()
_background_tasks: set[asyncio.Task] = set()

WELCOME_EMOJI_ID = "6039779802741739617"  # ✏️
BUY_EMOJI_ID = "5904462880941545555"  # 🪙
SUPPORT_EMOJI_ID = "6035084557378654059"  # 👤
TITLE_EMOJI_ID = "5890925363067886150"  # ✨
ORACLE_1_ID = "5382208803506783383"  # 🔮
ORACLE_2_ID = "5384534897664751953"  # 🔮
ORACLE_3_ID = "5382250438919750173"  # 🔮
ORACLE_4_ID = "5381855057115381194"  # 🔮
PRICE_1_ID = "5375262247956260756"  # 😉
PRICE_2_ID = "5377816374812880138"  # 😉
PRICE_3_ID = "5377455408581455402"  # 😉
PRICE_4_ID = "5377301348104547236"  # 😉
PRICE_5_ID = "5377811929521728792"  # 😉
PRICE_6_ID = "5377557628803099645"  # 😉
STAR_BTN_EMOJI_ID = "5886685105065300941"  # ⭐️
TRANSFER_BTN_EMOJI_ID = "5890848474563352982"  # 🪙
PROMO_BTN_EMOJI_ID = "5805506958995758422"  # 📁
BACK_BTN_EMOJI_ID = "5960671702059848143"  # ⬅️
PAID_BTN_EMOJI_ID = "5836997023554870252"  # 🔨


def premium_emoji(emoji_id: str, fallback: str) -> str:
    return f'<tg-emoji emoji-id="{emoji_id}">{fallback}</tg-emoji>'


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def generate_license_id() -> str:
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    chunks = ["".join(secrets.choice(alphabet) for _ in range(4)) for _ in range(3)]
    return "DBR-" + "-".join(chunks)


def format_expiry(value: datetime | None) -> str:
    value = as_utc(value)
    return value.strftime("%d.%m.%Y %H:%M:%S UTC") if value else "—"


def format_duration(seconds: int | None) -> str:
    seconds = int(seconds or 0)
    if seconds <= 0:
        return "0 сек"
    if seconds % 86400 == 0:
        days = seconds // 86400
        return f"{days} дн."
    if seconds % 3600 == 0:
        return f"{seconds // 3600} ч."
    if seconds % 60 == 0:
        return f"{seconds // 60} мин."
    return f"{seconds} сек."


def parse_duration(text: str | None) -> int | None:
    raw = (text or "").strip().lower().replace(" ", "")
    if not raw:
        return None
    units = {
        "s": 1, "sec": 1, "сек": 1, "с": 1,
        "m": 60, "min": 60, "мин": 60, "м": 60,
        "h": 3600, "hour": 3600, "ч": 3600,
        "d": 86400, "day": 86400, "д": 86400,
    }
    for suffix in sorted(units, key=len, reverse=True):
        if raw.endswith(suffix):
            number = raw[:-len(suffix)]
            if number.isdigit():
                value = int(number) * units[suffix]
                return value if 1 <= value <= 10 * 365 * 86400 else None
    if raw.isdigit():
        value = int(raw) * 60  # plain number = minutes for admin convenience
        return value if 1 <= value <= 10 * 365 * 86400 else None
    return None


def tariff_amount(days: int, currency: str, has_promo: bool) -> int:
    tariff = TARIFFS[days]
    if currency == "RUB":
        return tariff["promo_rub" if has_promo else "rub"]
    return tariff["promo_xtr" if has_promo else "xtr"]


def license_is_valid(row: License, now: datetime | None = None) -> bool:
    now = now or utc_now()
    expires = as_utc(row.expires_at)
    return bool(row.active and expires and expires > now)


def valid_license_filters(now: datetime | None = None):
    now = now or utc_now()
    return (
        License.active.is_(True),
        License.expires_at.is_not(None),
        License.expires_at > now,
    )


def spawn_background(coro):
    task = asyncio.create_task(coro)
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)
    return task


class PromoState(StatesGroup):
    waiting_code = State()


class SupportState(StatesGroup):
    waiting_message = State()


class SupportReplyState(StatesGroup):
    waiting_reply = State()


class AdminReleaseState(StatesGroup):
    waiting_file = State()
    waiting_version = State()
    waiting_notes = State()
    waiting_confirmation = State()


class AdminLicenseDurationState(StatesGroup):
    waiting_duration = State()


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def get_banner_path() -> Path:
    return Path(START_IMAGE_PATH)


def home_caption() -> str:
    return (
        "<blockquote>"
        f"{premium_emoji(WELCOME_EMOJI_ID, '✏️')} "
        "Добро пожаловать в бот для покупки мода <b>Debris Release</b>."
        "</blockquote>"
    )


def support_caption() -> str:
    return (
        "<blockquote>"
        f"{premium_emoji(SUPPORT_EMOJI_ID, '👤')} "
        "<b>Поддержка</b>\n\n"
        "Опишите проблему одним сообщением. Ответ придёт сюда же, в бот."
        "</blockquote>"
    )


def privacy_text() -> str:
    return (
        "<b>Политика конфиденциальности Debris Release</b>\n"
        "Актуальная редакция: 06.09.2026\n\n"
        "1. <b>Общие положения</b>\n"
        "Эта политика описывает, какие данные получает Telegram-бот Debris Release и для чего они используются.\n\n"
        "2. <b>Какие данные обрабатываются</b>\n"
        "Бот получает Telegram ID, username и имя профиля, а также хранит сведения о заказах, обращениях в поддержку и статусах оплаты. "
        "Документы и адрес проживания бот не запрашивает.\n\n"
        "3. <b>Цели обработки</b>\n"
        "Эти данные нужны для оформления заказа, выдачи Debris Release, работы поддержки и истории покупок.\n\n"
        "4. <b>Платежи</b>\n"
        "Платёжные данные обрабатывает выбранный платёжный сервис. Полные данные банковской карты в боте не сохраняются.\n\n"
        "5. <b>Передача третьим лицам</b>\n"
        "Данные могут передаваться платёжному сервису в объёме, который нужен для проведения оплаты. В остальных случаях данные не передаются без законного основания.\n\n"
        "6. <b>Хранение и защита</b>\n"
        "Данные хранятся столько, сколько это нужно для работы заказов, поддержки и выполнения обязательств перед пользователем.\n\n"
        "7. <b>Обращения пользователя</b>\n"
        "По вопросам о своих данных можно написать через раздел «Поддержка».\n\n"
        "8. <b>Изменения Политики</b>\n"
        "Текущая версия политики всегда доступна в разделе «Документы»."
    )


def agreement_text() -> str:
    return (
        "<b>Пользовательское соглашение Debris Release</b>\n"
        "Актуальная редакция: 06.09.2026\n\n"
        "1. <b>Предмет соглашения</b>\n"
        "Через бот можно купить цифровой продукт Debris Release. Цена и условия доступа показываются до оплаты.\n\n"
        "2. <b>Принятие условий</b>\n"
        "Оформляя и оплачивая заказ, пользователь принимает условия этого соглашения.\n\n"
        "3. <b>Стоимость и оплата</b>\n"
        "Актуальная цена указана в разделе «Тарифы» и на экране оплаты. Платёж считается выполненным после подтверждения платёжной системой.\n\n"
        "4. <b>Получение цифрового товара</b>\n"
        "После подтверждения оплаты Debris Release выдаётся через бот. Если выдача задержалась, нужно обратиться в поддержку.\n\n"
        "5. <b>Правила использования</b>\n"
        "Нельзя перепродавать, публиковать или передавать Debris Release третьим лицам без разрешения правообладателя.\n\n"
        "6. <b>Работоспособность</b>\n"
        "Обновления игры и стороннего ПО могут влиять на совместимость клиента. Актуальная версия выдаётся через бот.\n\n"
        "7. <b>Возвраты</b>\n"
        "Если оплата прошла, а товар не был выдан из-за технической ошибки, нужно написать в поддержку. Запрос рассматривается по обстоятельствам платежа и заказа.\n\n"
        "8. <b>Поддержка</b>\n"
        "Связаться с поддержкой можно через кнопку «Поддержка» в главном меню.\n\n"
        "9. <b>Изменение условий</b>\n"
        "Текущая версия соглашения всегда доступна в разделе «Документы»."
    )


def tariffs_text() -> str:
    lines = ["<b>Цены и тарифы Debris Release</b>", ""]
    for days in sorted(TARIFFS):
        tariff = TARIFFS[days]
        stars = f" / {tariff['xtr']} ⭐" if tariff["xtr"] > 0 else ""
        promo_stars = f" / {tariff['promo_xtr']} ⭐" if tariff["promo_xtr"] > 0 else ""
        lines.append(
            f"<b>{days} дней</b> — {tariff['rub']} ₽{stars} "
            f"(с промокодом: {tariff['promo_rub']} ₽{promo_stars})"
        )
    lines.extend([
        "",
        "Промокод применяет специальную цену выбранного тарифа.",
        "При повторной покупке тот же Debris ID продлевается на выбранный срок.",
    ])
    return "\n".join(lines)


def home_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(
                text="Купить клиент",
                callback_data="buy",
                icon_custom_emoji_id=BUY_EMOJI_ID,
            )],
            [InlineKeyboardButton(text="🔑 Мой Debris ID", callback_data="my_license")],
            [InlineKeyboardButton(
                text="Поддержка",
                callback_data="support",
                icon_custom_emoji_id=SUPPORT_EMOJI_ID,
            )],
            [
                InlineKeyboardButton(text="📄 Документы", callback_data="documents"),
                InlineKeyboardButton(text="💳 Тарифы", callback_data="tariffs"),
            ],
        ]
    )


def tariff_keyboard(has_promo: bool) -> InlineKeyboardMarkup:
    rows = []
    for days in sorted(TARIFFS):
        rub = tariff_amount(days, "RUB", has_promo)
        xtr = tariff_amount(days, "XTR", has_promo)
        price = f"{rub} ₽" + (f" / {xtr} ⭐" if xtr > 0 else "")
        rows.append([InlineKeyboardButton(
            text=f"{days} дней — {price}",
            callback_data=f"tariff:{days}",
        )])
    rows.append([InlineKeyboardButton(
        text="Назад", callback_data="home", icon_custom_emoji_id=BACK_BTN_EMOJI_ID
    )])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def payment_keyboard(days: int, has_promo: bool) -> InlineKeyboardMarkup:
    xtr = tariff_amount(days, "XTR", has_promo)
    rub = tariff_amount(days, "RUB", has_promo)
    rows = []
    if xtr > 0:
        rows.append([InlineKeyboardButton(
            text=f"Telegram Stars — {xtr}",
            callback_data=f"paystars:{days}:{int(has_promo)}",
            icon_custom_emoji_id=STAR_BTN_EMOJI_ID,
        )])
    if platega_enabled():
        rows.append([InlineKeyboardButton(
            text=f"Карта / СБП — {rub} ₽",
            callback_data=f"payplatega:{days}:{int(has_promo)}",
            icon_custom_emoji_id=TRANSFER_BTN_EMOJI_ID,
        )])
    rows.extend([
        [InlineKeyboardButton(
            text="Ввести промокод",
            callback_data="promo",
            icon_custom_emoji_id=PROMO_BTN_EMOJI_ID,
        )],
        [InlineKeyboardButton(
            text="Выбрать другой срок",
            callback_data="buy",
            icon_custom_emoji_id=BACK_BTN_EMOJI_ID,
        )],
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def support_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(
                text="Создать обращение",
                callback_data="support_new",
                icon_custom_emoji_id=SUPPORT_EMOJI_ID,
            )],
            [InlineKeyboardButton(
                text="Назад",
                callback_data="home",
                icon_custom_emoji_id=BACK_BTN_EMOJI_ID,
            )],
        ]
    )


def documents_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Политика конфиденциальности", callback_data="privacy")],
            [InlineKeyboardButton(text="Пользовательское соглашение", callback_data="agreement")],
            [InlineKeyboardButton(
                text="Назад",
                callback_data="home",
                icon_custom_emoji_id=BACK_BTN_EMOJI_ID,
            )],
        ]
    )


def document_back_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(
                text="К документам",
                callback_data="documents",
                icon_custom_emoji_id=BACK_BTN_EMOJI_ID,
            )]
        ]
    )


def tariffs_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(
                text="Купить клиент",
                callback_data="buy",
                icon_custom_emoji_id=BUY_EMOJI_ID,
            )],
            [InlineKeyboardButton(
                text="Назад",
                callback_data="home",
                icon_custom_emoji_id=BACK_BTN_EMOJI_ID,
            )],
        ]
    )


def orders_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(
                text="Назад",
                callback_data="home",
                icon_custom_emoji_id=BACK_BTN_EMOJI_ID,
            )]
        ]
    )


def admin_order_keyboard(order_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Выдать заказ", callback_data=f"admindeliver:{order_id}"),
                InlineKeyboardButton(text="❌ Отклонить", callback_data=f"adminreject:{order_id}"),
            ]
        ]
    )


def admin_panel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="👥 Покупатели", callback_data="admin_buyers:0")],
            [InlineKeyboardButton(text="📦 Выпустить обновление", callback_data="admin_release_new")],
            [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")],
        ]
    )


def admin_back_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⬅️ В админ-панель", callback_data="admin_home")]]
    )


def release_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Разослать обновление", callback_data="admin_release_confirm")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_release_cancel")],
        ]
    )


def buyers_page_keyboard(page: int, has_prev: bool, has_next: bool, licenses: list[License] | None = None) -> InlineKeyboardMarkup:
    rows = []
    for license_row in licenses or []:
        label = license_row.username and f"@{license_row.username}" or str(license_row.user_id)
        rows.append([InlineKeyboardButton(text=f"🔑 {label}", callback_data=f"admin_license:{license_row.user_id}:{page}")])
    nav = []
    if has_prev:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"admin_buyers:{page - 1}"))
    if has_next:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"admin_buyers:{page + 1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton(text="⬅️ В админ-панель", callback_data="admin_home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_license_keyboard(user_id: int, page: int, active: bool) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🧪 Срок 2 мин", callback_data=f"admin_license_set:{user_id}:{page}:120"),
            InlineKeyboardButton(text="10 мин", callback_data=f"admin_license_set:{user_id}:{page}:600"),
        ],
        [
            InlineKeyboardButton(text="1 час", callback_data=f"admin_license_set:{user_id}:{page}:3600"),
            InlineKeyboardButton(text="1 день", callback_data=f"admin_license_set:{user_id}:{page}:86400"),
        ],
        [InlineKeyboardButton(text=f"➕ +{LICENSE_DAYS} дней", callback_data=f"admin_license_extend:{user_id}:{page}")],
        [InlineKeyboardButton(text="✏️ Свой срок", callback_data=f"admin_license_custom:{user_id}:{page}")],
        [InlineKeyboardButton(text="🖥 Сбросить устройство", callback_data=f"admin_license_reset:{user_id}:{page}")],
        [InlineKeyboardButton(text=("⛔ Заблокировать" if active else "✅ Разблокировать"), callback_data=f"admin_license_toggle:{user_id}:{page}")],
        [InlineKeyboardButton(text="⬅️ К покупателям", callback_data=f"admin_buyers:{page}")],
    ])


async def active_license_count() -> int:
    async with Session() as session:
        value = await session.scalar(
            select(func.count()).select_from(License).where(*valid_license_filters())
        )
        return int(value or 0)


async def latest_release() -> Release | None:
    async with Session() as session:
        result = await session.execute(
            select(Release).order_by(Release.id.desc()).limit(1)
        )
        return result.scalar_one_or_none()


async def admin_panel_text() -> str:
    count = await active_license_count()
    latest = await latest_release()
    version = html.escape(latest.version) if latest else "ещё не выпускалось"
    return (
        "<b>Админ-панель Debris Release</b>\n\n"
        f"Покупателей с лицензией: <b>{count}</b>\n"
        f"Последняя версия: <b>{version}</b>\n\n"
        "В рассылку попадают только пользователи, которым бот уже выдал файл после покупки."
    )


async def buyers_page_text(page: int, page_size: int = 10) -> tuple[str, bool, bool, list[License]]:
    page = max(0, page)
    async with Session() as session:
        total = int(await session.scalar(select(func.count()).select_from(License)) or 0)
        result = await session.execute(
            select(License)
            .order_by(License.purchased_at.asc(), License.user_id.asc())
            .offset(page * page_size)
            .limit(page_size)
        )
        licenses = list(result.scalars())

    lines = [f"<b>Покупатели с Debris ID: {total}</b>", ""]
    if not licenses:
        lines.append("Список пуст.")
    else:
        now = utc_now()
        for index, license_row in enumerate(licenses, start=page * page_size + 1):
            username = f"@{license_row.username}" if license_row.username else "без username"
            state = "✅" if license_is_valid(license_row, now) else ("⛔" if not license_row.active else "⌛")
            device = "привязан" if license_row.device_hash else "не привязан"
            lines.append(
                f"{index}. {state} {html.escape(username)} · <code>{license_row.user_id}</code>\n"
                f"   ID: <code>{html.escape(license_row.license_id or '—')}</code>\n"
                f"   до: {format_expiry(license_row.expires_at)} · устройство: {device}"
            )
    has_prev = page > 0
    has_next = (page + 1) * page_size < total
    return "\n".join(lines), has_prev, has_next, licenses


async def stats_text() -> str:
    async with Session() as session:
        active = int(await session.scalar(
            select(func.count()).select_from(License).where(*valid_license_filters())
        ) or 0)
        delivered_orders = int(await session.scalar(
            select(func.count()).select_from(Order).where(Order.status == "DELIVERED")
        ) or 0)
        releases = int(await session.scalar(select(func.count()).select_from(Release)) or 0)
        latest_result = await session.execute(select(Release).order_by(Release.id.desc()).limit(1))
        latest = latest_result.scalar_one_or_none()

    text = (
        "<b>Статистика</b>\n\n"
        f"Покупателей с лицензией: <b>{active}</b>\n"
        f"Выданных заказов: <b>{delivered_orders}</b>\n"
        f"Выпущено обновлений: <b>{releases}</b>"
    )
    if latest:
        text += (
            f"\n\nПоследняя версия: <b>{html.escape(latest.version)}</b>\n"
            f"Отправлено: <b>{latest.sent_count}</b>\n"
            f"Ошибок: <b>{latest.failed_count}</b>"
        )
    return text


def release_caption(version: str, notes: str) -> str:
    safe_version = html.escape(version)
    short_notes = notes.strip().encode('utf-16-le')[:1500].decode('utf-16-le',errors='ignore')
    safe_notes = html.escape(short_notes) if short_notes else "Без описания изменений."
    return (
        f"{premium_emoji(TITLE_EMOJI_ID, '✨')} <b>Обновление Debris Release</b>\n\n"
        f"Версия: <b>{safe_version}</b>\n\n"
        f"<b>Что нового:</b>\n{safe_notes}"
    )


async def create_release_record(*, version: str, file_path: str, file_name: str, notes: str, created_by: int) -> Release:
    async with Session() as session:
        release = Release(
            version=version,
            file_path=file_path,
            file_name=file_name,
            notes=notes,
            created_by=created_by,
            status="READY",
        )
        session.add(release)
        await session.commit()
        await session.refresh(release)
        return release


async def set_release_delivery(release_id: int, user_id: int, *, status: str, error: str | None = None):
    async with Session() as session:
        async with session.begin():
            row = await session.get(ReleaseDelivery, (release_id, user_id))
            if row is None:
                row = ReleaseDelivery(release_id=release_id, user_id=user_id, status=status)
                session.add(row)
            row.status = status
            row.error = error
            if status == "SENT":
                row.sent_at = datetime.now(timezone.utc)


async def mark_license_version_sent(user_id: int, version: str):
    async with Session() as session:
        async with session.begin():
            license_row = await session.get(License, user_id)
            if license_row:
                license_row.last_version_sent = version
                license_row.last_release_sent_at = datetime.now(timezone.utc)


async def broadcast_release(bot: Bot, release_id: int, admin_id: int):
    owner = secrets.token_hex(16)
    if not await asyncio.to_thread(store.broadcast_claim,release_id,owner):
        return
    async def keep_claim():
        while True:
            await asyncio.sleep(20)
            if not await asyncio.to_thread(store.broadcast_claim,release_id,owner):
                return
    heartbeat = asyncio.create_task(keep_claim())
    try:
        async with Session() as session:
            release = await session.get(Release,release_id)
            if release is None or release.status not in {'READY','SENDING'}:
                return
            file_path = await asyncio.to_thread(prepare_jar,release.file_path)
            if release.status == 'READY':
                rows = list((await session.execute(select(License).where(*valid_license_filters()))).scalars())
                for row in rows:
                    if await session.get(ReleaseDelivery,(release_id,row.user_id)) is None:
                        session.add(ReleaseDelivery(release_id=release_id,user_id=row.user_id,status='PENDING'))
                release.total_count = len(rows)
            release.status = 'SENDING'
            await session.commit()
            version,notes = release.version,release.notes
            recipients = list((await session.execute(select(ReleaseDelivery).where(
                ReleaseDelivery.release_id==release_id,ReleaseDelivery.status=='PENDING').order_by(ReleaseDelivery.user_id))).scalars())
        for recipient in recipients:
            if not await asyncio.to_thread(store.broadcast_claim,release_id,owner):
                return
            async def send_if_active():
                if not await asyncio.to_thread(store.eligible,recipient.user_id):
                    return False
                await asyncio.wait_for(bot.send_document(recipient.user_id,
                    FSInputFile(file_path,filename=f'Debris-Release-{version}.jar'),
                    caption=release_caption(version,notes),parse_mode=ParseMode.HTML),60)
                return True
            try:
                try:
                    sent = await send_if_active()
                except TelegramRetryAfter as exc:
                    await asyncio.sleep(float(exc.retry_after)+1)
                    sent = await send_if_active()
                await set_release_delivery(release_id,recipient.user_id,status='SENT' if sent else 'SKIPPED')
                if sent:
                    await mark_license_version_sent(recipient.user_id,version)
            except Exception as exc:
                await set_release_delivery(release_id,recipient.user_id,status='FAILED',error=type(exc).__name__)
            await asyncio.sleep(0.08)
        async with Session() as session:
            counts = dict((await session.execute(select(ReleaseDelivery.status,func.count()).where(
                ReleaseDelivery.release_id==release_id).group_by(ReleaseDelivery.status))).all())
            release = await session.get(Release,release_id)
            release.status = 'DONE'
            release.sent_count = counts.get('SENT',0)
            release.failed_count = counts.get('FAILED',0)
            await session.commit()
        await bot.send_message(admin_id,
            f"Рассылка {version} завершена. Отправлено: {counts.get('SENT',0)}. "
            f"Пропущено (нет подписки): {counts.get('SKIPPED',0)}. Ошибок: {counts.get('FAILED',0)}.",
            reply_markup=admin_panel_keyboard())
    except asyncio.CancelledError:
        raise
    except Exception:
        logging.error('Release %s interrupted; progress is saved',release_id)
        await bot.send_message(admin_id,f'Рассылка #{release_id} прервана. Проверьте файл и настройки API; прогресс сохранён.')
    finally:
        heartbeat.cancel()
        await asyncio.gather(heartbeat,return_exceptions=True)
        await asyncio.to_thread(store.broadcast_release_claim,release_id,owner)


async def resume_release_broadcasts(bot: Bot):
    async with Session() as session:
        result = await session.execute(
            select(Release).where(Release.status.in_(["READY", "SENDING"])).order_by(Release.id.asc())
        )
        releases = list(result.scalars())

    for release in releases:
        spawn_background(broadcast_release(bot, release.id, release.created_by))


def decorative_oracles() -> str:
    return (
        premium_emoji(ORACLE_1_ID, '🔮')
        + premium_emoji(ORACLE_2_ID, '🔮')
        + premium_emoji(ORACLE_3_ID, '🔮')
        + premium_emoji(ORACLE_4_ID, '🔮')
    )


def decorative_price_line() -> str:
    return (
        premium_emoji(PRICE_1_ID, '😉')
        + premium_emoji(PRICE_2_ID, '😉')
        + premium_emoji(PRICE_3_ID, '😉')
        + premium_emoji(PRICE_4_ID, '😉')
        + premium_emoji(PRICE_5_ID, '😉')
        + premium_emoji(PRICE_6_ID, '😉')
    )


def price_text(days: int, has_promo: bool) -> str:
    rub = tariff_amount(days, "RUB", has_promo)
    xtr = tariff_amount(days, "XTR", has_promo)
    stars_part = f" / <b>{xtr} ⭐</b>" if xtr > 0 else ""
    promo_hint = (
        f"<b>Промокод применён.</b>\nЦена: <b>{rub} ₽</b>{stars_part}"
        if has_promo
        else (
            f"Цена: <b>{rub} ₽</b>{stars_part}\n\n"
            f"Есть промокод? Нажмите «{premium_emoji(PROMO_BTN_EMOJI_ID, '📁')} Ввести промокод»."
        )
    )
    return (
        f"{premium_emoji(TITLE_EMOJI_ID, '✨')} <b>Debris — {days} дней</b>\n\n"
        f"{decorative_oracles()}\n"
        f"{decorative_price_line()}\n\n"
        f"{promo_hint}\n\n"
        "Выберите способ оплаты:"
    )


async def send_photo_or_text(message: Message, text: str, keyboard: InlineKeyboardMarkup | None = None):
    banner_path = get_banner_path()
    if len(text) <= 900 and banner_path.exists() and banner_path.is_file():
        await message.answer_photo(
            photo=FSInputFile(banner_path),
            caption=text,
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard,
        )
    else:
        await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)


async def render_menu(message: Message, text: str, keyboard: InlineKeyboardMarkup | None = None):
    try:
        if message.photo and len(text) <= 900:
            await message.edit_caption(
                caption=text,
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard,
            )
            return
        if not message.photo:
            await message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
            return
    except TelegramBadRequest:
        pass
    except Exception:
        logging.exception("Failed to edit menu message")

    try:
        await message.delete()
    except Exception:
        pass

    await send_photo_or_text(message, text, keyboard)


def platega_enabled() -> bool:
    return bool(PLATEGA_MERCHANT_ID and PLATEGA_SECRET)


def platega_headers() -> dict[str, str]:
    return {
        "X-MerchantId": PLATEGA_MERCHANT_ID,
        "X-Secret": PLATEGA_SECRET,
        "Content-Type": "application/json",
    }


async def platega_request(method: str, path: str, *, json_data: dict | None = None) -> dict:
    url = f"{PLATEGA_BASE_URL}{path}"
    timeout = aiohttp.ClientTimeout(total=20)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.request(method, url, headers=platega_headers(), json=json_data) as response:
            body = await response.text()
            if response.status >= 400:
                raise RuntimeError(f"Platega HTTP {response.status}")
            if not body.strip():
                return {}
            try:
                return await response.json(content_type=None)
            except Exception as exc:
                raise RuntimeError("Некорректный ответ Platega") from exc


async def set_order_provider_transaction(order_id: int, transaction_id: str, status: str = "INVOICE"):
    async with Session() as session:
        async with session.begin():
            order = await session.get(Order, order_id)
            if order is None:
                return
            order.provider_payment_charge_id = transaction_id
            order.status = status


async def find_order_by_provider_transaction(transaction_id: str) -> Order | None:
    async with Session() as session:
        result = await session.execute(
            select(Order).where(Order.provider_payment_charge_id == transaction_id).limit(1)
        )
        return result.scalar_one_or_none()


async def create_platega_payment(bot: Bot, order: Order, username: str | None) -> tuple[str, str]:
    me = await bot.get_me()
    return_url = f"https://t.me/{me.username}?start=payment_{order.id}"
    failed_url = f"https://t.me/{me.username}?start=payment_failed_{order.id}"
    payload = {
        "paymentDetails": {
            "amount": order.amount,
            "currency": "RUB",
        },
        "description": f"Debris Release ({format_duration(order.duration_seconds)}), заказ #{order.id}",
        "return": return_url,
        "failedUrl": failed_url,
        "payload": f"order:{order.id}",
        "metadata": {
            "userId": str(order.user_id),
            "userName": f"@{username}" if username else "",
        },
    }
    data = await platega_request("POST", "/v2/transaction/process", json_data=payload)
    transaction_id = str(data.get("transactionId") or "").strip()
    payment_url = str(data.get("url") or data.get("redirect") or "").strip()
    if not transaction_id or not payment_url:
        raise RuntimeError("Platega не вернула transactionId или ссылку на оплату.")
    return transaction_id, payment_url


async def get_platega_status(transaction_id: str) -> dict:
    return await platega_request("GET", f"/transaction/{transaction_id}")


def platega_payment_keyboard(order_id: int, payment_url: str, amount: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(
                text=f"Оплатить {amount} ₽",
                url=payment_url,
                icon_custom_emoji_id=TRANSFER_BTN_EMOJI_ID,
            )],
            [InlineKeyboardButton(text="Проверить оплату", callback_data=f"checkplatega:{order_id}")],
            [InlineKeyboardButton(
                text="Назад",
                callback_data="buy",
                icon_custom_emoji_id=BACK_BTN_EMOJI_ID,
            )],
        ]
    )


async def mark_platega_failed(order_id: int, status: str):
    await asyncio.to_thread(store.cancel_payment,order_id,status=='CHARGEBACK')


async def confirm_platega_order(bot: Bot, order_id: int, charge_id: str, amount, currency) -> tuple[bool, str]:
    try:
        await asyncio.to_thread(store.confirm,order_id,'platega',charge_id,amount,currency)
    except (ValueError, sqlite3.IntegrityError):
        return False, "Платёж не соответствует заказу."
    return await deliver_order(bot,order_id)


async def process_platega_status(bot: Bot, order: Order, data: dict) -> tuple[bool, str]:
    if not isinstance(data,dict) or order.payment_method != 'platega':
        return False, "Неверные данные платежа."
    charge_id = str(data.get('id') or '').strip()
    if not charge_id or charge_id != order.provider_payment_charge_id:
        return False, "Транзакция не соответствует заказу."
    payment = data.get('paymentDetails') or {}
    if not isinstance(payment,dict):
        return False, "Неверные данные платежа."
    currency = str(payment.get('currency',data.get('currency',''))).upper()
    try:
        amount = Decimal(str(payment.get('amount',data.get('amount'))))
        matches = amount.is_finite() and amount == Decimal(order.amount)
    except (InvalidOperation,TypeError,ValueError):
        matches = False
    if not matches or currency != order.currency:
        return False, "Сумма или валюта платежа не совпадает."
    status = str(data.get('status') or '').upper()
    if status == 'CONFIRMED':
        return await confirm_platega_order(bot,order.id,charge_id,amount,currency)
    if status == 'CANCELED':
        await asyncio.to_thread(store.cancel_payment,order.id)
        return False, "Платёж отменён."
    if status == 'CHARGEBACKED':
        changed = await asyncio.to_thread(store.cancel_payment,order.id,True)
        if changed:
            await notify_admins(bot,f"Возврат по заказу #{order.id}. Лицензия заблокирована до проверки администратором.")
        return False, "По платежу оформлен возврат."
    return False, "Платёж пока не подтверждён."


async def platega_poll_loop(bot: Bot):
    while True:
        try:
            async with Session() as session:
                result = await session.execute(
                    select(Order)
                    .where(
                        Order.payment_method == "platega",
                        Order.status.in_(["INVOICE", "PAID"]),
                        Order.provider_payment_charge_id.is_not(None),
                    )
                    .order_by(Order.id.asc())
                    .limit(50)
                )
                orders = list(result.scalars())

            for order in orders:
                try:
                    data = await get_platega_status(order.provider_payment_charge_id)
                    await process_platega_status(bot, order, data)
                except Exception:
                    logging.exception("Platega status check failed for order %s", order.id)
        except asyncio.CancelledError:
            raise
        except Exception:
            logging.exception("Platega poll loop failed")

        await asyncio.sleep(PLATEGA_POLL_INTERVAL)


async def platega_webhook(request: web.Request) -> web.Response:
    if not platega_enabled():
        return web.Response(status=503)
    if not secrets.compare_digest(request.headers.get('X-MerchantId',''),PLATEGA_MERCHANT_ID):
        return web.Response(status=401)
    if not secrets.compare_digest(request.headers.get('X-Secret',''),PLATEGA_SECRET):
        return web.Response(status=401)
    try:
        data = await request.json()
    except Exception:
        return web.Response(status=400)
    if not isinstance(data,dict) or not isinstance(data.get('id'),str) or len(data['id'])>256:
        return web.Response(status=400)
    order = await find_order_by_provider_transaction(data['id'])
    if order is None:
        return web.Response(status=200)
    try:
        # The callback is a notification; confirm using authenticated provider GET.
        verified = await get_platega_status(data['id'])
        await process_platega_status(request.app['bot'],order,verified)
    except Exception:
        logging.error('Unable to verify payment notification for order %s',order.id)
        return web.Response(status=503)
    return web.Response(status=200)


async def start_platega_webhook_server(bot: Bot) -> web.AppRunner | None:
    if not (PLATEGA_WEBHOOK_ENABLED and platega_enabled()):
        return None
    app = web.Application(client_max_size=16384)
    app["bot"] = bot
    app.router.add_post("/platega/paymentStatus", platega_webhook)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, PLATEGA_WEBHOOK_HOST, PLATEGA_WEBHOOK_PORT)
    await site.start()
    logging.info("Platega webhook listening on %s:%s", PLATEGA_WEBHOOK_HOST, PLATEGA_WEBHOOK_PORT)
    return runner


async def create_order(*, user_id: int, username: str | None, payment_method: str,
                       amount: int, currency: str, promo_code: str | None, status: str,
                       duration_seconds: int) -> Order:
    async with Session() as session:
        order = Order(
            user_id=user_id,
            username=username,
            payment_method=payment_method,
            amount=amount,
            currency=currency,
            promo_code=promo_code,
            status=status,
            duration_seconds=duration_seconds,
        )
        session.add(order)
        await session.commit()
        await session.refresh(order)
        return order


async def get_order(order_id: int) -> Order | None:
    async with Session() as session:
        return await session.get(Order, order_id)


async def notify_admins(bot: Bot, text: str, keyboard: InlineKeyboardMarkup | None = None):
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
        except Exception:
            logging.exception("Failed to notify admin %s", admin_id)


async def deliver_order(bot: Bot, order_id: int, *, expected_user_id: int | None = None, manual=False) -> tuple[bool, str]:
    try:
        claimed = await asyncio.to_thread(store.claim_delivery,order_id,expected_user_id,manual)
    except CloudflareLicenseError:
        return False, 'Оплата сохранена. Cloudflare временно недоступен; выдача повторится автоматически.'
    except ValueError as exc:
        return False,str(exc)
    if claimed is None:
        return True,"Заказ уже был выдан ранее."
    order, license_row, claim = claimed
    try:
        file_path = await asyncio.to_thread(prepare_jar,MOD_FILE_PATH)
        caption = (
            f"✅ <b>Debris — заказ #{order_id}</b>\n\n"
            f"Ваш ID: <code>{html.escape(license_row['license_id'])}</code>\n"
            f"Срок покупки: {format_duration(order['duration_seconds'] or 30*86400)}\n"
            f"Доступ до: <b>{format_expiry(datetime.fromisoformat(license_row['expires_at']))}</b>\n\n"
            "Установите JAR в mods и введите ID при запуске Debris. "
            "ID также доступен в разделе «Мой Debris ID»."
        )
        if not license_row['active']:
            caption += "\n⛔ Лицензия заблокирована. Обратитесь в поддержку."
        await asyncio.wait_for(bot.send_document(order['user_id'],
            FSInputFile(file_path,filename='Debris-1.21.8.jar'),caption=caption,parse_mode=ParseMode.HTML),60)
    except Exception as exc:
        # Persist retry without storing exception text that may contain Telegram URLs/tokens.
        await asyncio.to_thread(store.finish_delivery,order_id,claim,type(exc).__name__,
                                float(getattr(exc,'retry_after',0))+1)
        logging.warning('Delivery order %s deferred (%s)',order_id,type(exc).__name__)
        return False,"Оплата сохранена. Бот автоматически повторит выдачу."
    await asyncio.to_thread(store.finish_delivery,order_id,claim)
    return True,"JAR и Debris ID успешно выданы."


@router.message(CommandStart())
async def start(message: Message, state: FSMContext):
    await state.clear()
    await send_photo_or_text(message, home_caption(), home_keyboard())


@router.message(Command("cancel"))
async def cancel(message: Message, state: FSMContext):
    data = await state.get_data()
    pending_path = Path(str(data.get("release_file_path") or ""))
    if pending_path.is_file() and pending_path.name.startswith("pending_"):
        try:
            pending_path.unlink()
        except OSError:
            pass
    await state.clear()
    if is_admin(message.from_user.id):
        await message.answer(
            await admin_panel_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=admin_panel_keyboard(),
        )
    else:
        await send_photo_or_text(message, "Действие отменено.", home_keyboard())


@router.message(Command("checkup"))
async def checkup_cmd(message: Message):
    await message.answer("Кодовое слово проекта: <code>чекап</code>", parse_mode=ParseMode.HTML)


@router.message(F.text == "чекап")
async def checkup_word(message: Message):
    await message.answer("Проверка проекта: <b>чекап</b> ✅", parse_mode=ParseMode.HTML)


@router.message(Command("admin"))
async def admin_cmd(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("Вы не администратор.")
        return
    await state.clear()
    await message.answer(
        await admin_panel_text(),
        parse_mode=ParseMode.HTML,
        reply_markup=admin_panel_keyboard(),
    )


@router.callback_query(F.data == "admin_home")
async def admin_home_callback(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    await state.clear()
    await callback.message.edit_text(
        await admin_panel_text(),
        parse_mode=ParseMode.HTML,
        reply_markup=admin_panel_keyboard(),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("admin_buyers:"))
async def admin_buyers_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    try:
        page = max(0, int(callback.data.split(":", 1)[1]))
    except (TypeError, ValueError):
        page = 0
    text, has_prev, has_next, licenses = await buyers_page_text(page)
    await callback.message.edit_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=buyers_page_keyboard(page, has_prev, has_next, licenses),
    )
    await callback.answer()


@router.callback_query(F.data == "admin_stats")
async def admin_stats_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    await callback.message.edit_text(
        await stats_text(),
        parse_mode=ParseMode.HTML,
        reply_markup=admin_back_keyboard(),
    )
    await callback.answer()


@router.callback_query(F.data == "admin_release_new")
async def admin_release_new_callback(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    await state.clear()
    await state.set_state(AdminReleaseState.waiting_file)
    await callback.message.answer(
        "Отправьте новый файл Debris в формате <b>.jar</b>.",
        parse_mode=ParseMode.HTML,
    )
    await callback.answer()


@router.message(AdminReleaseState.waiting_file)
async def admin_release_file(message: Message, bot: Bot, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    if not message.document:
        await message.answer("Нужен файл .jar.")
        return

    original_name = Path(message.document.file_name or "debris-release.jar").name
    if not original_name.lower().endswith(".jar"):
        await message.answer("Нужен именно файл с расширением .jar.")
        return

    release_dir = Path(RELEASES_DIR)
    release_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    destination = release_dir / f"pending_{message.from_user.id}_{timestamp}_{original_name}"

    try:
        tg_file = await bot.get_file(message.document.file_id)
        await bot.download_file(tg_file.file_path, destination=destination)
    except Exception:
        logging.exception("Failed to download release file")
        await message.answer("Не получилось сохранить файл. Отправьте его ещё раз.")
        return

    await state.update_data(
        release_file_path=str(destination),
        release_file_name=original_name,
    )
    await state.set_state(AdminReleaseState.waiting_version)
    await message.answer("Введите номер версии, например <code>1.2.0</code>.", parse_mode=ParseMode.HTML)


@router.message(AdminReleaseState.waiting_version)
async def admin_release_version(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    version = (message.text or "").strip()
    if not version or len(version.encode('utf-16-le')) // 2 > 64:
        await message.answer("Введите короткий номер версии, например 1.2.0.")
        return
    await state.update_data(release_version=version)
    await state.set_state(AdminReleaseState.waiting_notes)
    await message.answer(
        "Напишите, что изменилось в этой версии. Если описания нет — отправьте <code>-</code>.",
        parse_mode=ParseMode.HTML,
    )


@router.message(AdminReleaseState.waiting_notes)
async def admin_release_notes(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    notes = (message.text or "").strip()
    if notes == "-":
        notes = ""
    if len(notes.encode('utf-16-le')) // 2 > 750:
        await message.answer("Описание слишком длинное. До 750 символов (эмодзи могут занимать два).")
        return

    await state.update_data(release_notes=notes)
    data = await state.get_data()
    count = await active_license_count()
    version = html.escape(str(data.get("release_version") or ""))
    file_name = html.escape(str(data.get("release_file_name") or ""))
    notes_preview = html.escape(notes) if notes else "Без описания изменений."

    await state.set_state(AdminReleaseState.waiting_confirmation)
    await message.answer(
        (
            "<b>Проверка перед рассылкой</b>\n\n"
            f"Версия: <b>{version}</b>\n"
            f"Файл: <code>{file_name}</code>\n"
            f"Получателей: <b>{count}</b>\n\n"
            f"<b>Что нового:</b>\n{notes_preview}\n\n"
            "После подтверждения этот файл станет текущей версией для новых покупателей и будет отправлен покупателям с действующей подпиской."
        ),
        parse_mode=ParseMode.HTML,
        reply_markup=release_confirm_keyboard(),
    )


@router.callback_query(F.data == "admin_release_cancel")
async def admin_release_cancel_callback(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    data = await state.get_data()
    path = Path(str(data.get("release_file_path") or ""))
    if path.is_file() and path.name.startswith("pending_"):
        try:
            path.unlink()
        except OSError:
            pass
    await state.clear()
    await callback.message.edit_text(
        await admin_panel_text(),
        parse_mode=ParseMode.HTML,
        reply_markup=admin_panel_keyboard(),
    )
    await callback.answer("Отменено")


@router.callback_query(F.data == "admin_release_confirm")
async def admin_release_confirm_callback(callback: CallbackQuery, bot: Bot, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return

    current_state = await state.get_state()
    if current_state != AdminReleaseState.waiting_confirmation.state:
        await callback.answer("Эта рассылка уже подтверждена или отменена.", show_alert=True)
        return

    data = await state.get_data()
    version = str(data.get("release_version") or "").strip()
    notes = str(data.get("release_notes") or "")
    file_name = str(data.get("release_file_name") or "debris-release.jar")
    temp_path = Path(str(data.get("release_file_path") or ""))

    if not version or not temp_path.is_file():
        await callback.answer("Не найден файл релиза. Начните заново.", show_alert=True)
        await state.clear()
        return

    release_dir = Path(RELEASES_DIR)
    release_dir.mkdir(parents=True, exist_ok=True)
    final_name = f"release_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{Path(file_name).name}"
    final_path = release_dir / final_name
    try:
        await asyncio.to_thread(inspect_jar,temp_path)
        temp_path.replace(final_path)
        current_mod = Path(MOD_FILE_PATH)
        if current_mod.parent != Path("."):
            current_mod.parent.mkdir(parents=True, exist_ok=True)
        staged = current_mod.with_name(current_mod.name + '.' + secrets.token_hex(6) + '.tmp')
        try:
            shutil.copy2(final_path, staged)
            staged.replace(current_mod)
        finally:
            staged.unlink(missing_ok=True)
    except Exception:
        logging.exception("Failed to activate release file")
        await callback.answer("Не удалось сохранить новую версию.", show_alert=True)
        return

    release = await create_release_record(
        version=version,
        file_path=str(final_path),
        file_name=Path(file_name).name,
        notes=notes,
        created_by=callback.from_user.id,
    )
    await state.clear()

    count = await active_license_count()
    await callback.message.edit_text(
        (
            f"<b>Версия {html.escape(version)} запущена в рассылку.</b>\n\n"
            f"Получателей: <b>{count}</b>\n"
            "После завершения бот пришлёт отчёт."
        ),
        parse_mode=ParseMode.HTML,
        reply_markup=admin_panel_keyboard(),
    )
    await callback.answer("Рассылка запущена")
    spawn_background(broadcast_release(bot, release.id, callback.from_user.id))


async def get_user_license(user_id: int) -> License | None:
    if await asyncio.to_thread(store.refresh, user_id) is None:
        return None
    async with Session() as session:
        return await session.get(License, user_id)


def user_license_text(row: License | None) -> str:
    if row is None:
        return "<b>Debris ID</b>\n\nУ вас пока нет лицензии. После покупки ID появится здесь."
    now = utc_now()
    if not row.active:
        status = "⛔ заблокирован"
    elif not license_is_valid(row, now):
        status = "⌛ срок истёк"
    else:
        remaining = max(0, int((as_utc(row.expires_at) - now).total_seconds()))
        if remaining < 60:
            remaining_text = f"{remaining} сек."
        elif remaining < 3600:
            remaining_text = f"{(remaining + 59) // 60} мин."
        elif remaining < 86400:
            remaining_text = f"{(remaining + 3599) // 3600} ч."
        else:
            remaining_text = f"{(remaining + 86399) // 86400} дн."
        status = f"✅ активен · осталось примерно {remaining_text}"
    return (
        "<b>Ваш Debris ID</b>\n\n"
        f"ID: <code>{html.escape(row.license_id or '—')}</code>\n"
        f"Статус: {status}\n"
        f"Доступ до: <b>{format_expiry(row.expires_at)}</b>\n"
        f"Устройство: {'привязано' if row.device_hash else 'ещё не привязано'}"
    )


@router.callback_query(F.data == "my_license")
async def my_license_callback(callback: CallbackQuery):
    if callback.message.chat.type != "private":
        await callback.answer("Откройте бота в личных сообщениях.",show_alert=True)
        return
    row = await get_user_license(callback.from_user.id)
    await render_menu(callback.message, user_license_text(row), orders_keyboard())
    await callback.answer()


def admin_license_text(row: License) -> str:
    now = utc_now()
    state = "✅ активен" if license_is_valid(row, now) else ("⛔ заблокирован" if not row.active else "⌛ истёк")
    username = f"@{row.username}" if row.username else "без username"
    return (
        "<b>Debris лицензия</b>\n\n"
        f"Пользователь: {html.escape(username)}\n"
        f"Telegram ID: <code>{row.user_id}</code>\n"
        f"Debris ID: <code>{html.escape(row.license_id or '—')}</code>\n"
        f"Статус: {state}\n"
        f"До: <b>{format_expiry(row.expires_at)}</b>\n"
        f"Осталось: <b>{max(0, int((as_utc(row.expires_at) - now).total_seconds())) if row.expires_at else 0} сек.</b>\n"
        f"Устройство: {'привязано' if row.device_hash else 'не привязано'}\n"
        f"Последняя проверка: {format_expiry(row.last_seen_at)}"
    )


@router.callback_query(F.data.startswith("admin_license:"))
async def admin_license_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    _, user_id_s, page_s = callback.data.split(":", 2)
    row = await get_user_license(int(user_id_s))
    if row is None:
        await callback.answer("Лицензия не найдена.", show_alert=True)
        return
    await callback.message.edit_text(admin_license_text(row), parse_mode=ParseMode.HTML, reply_markup=admin_license_keyboard(row.user_id, int(page_s), row.active))
    await callback.answer()


@router.callback_query(F.data.startswith("admin_license_extend:"))
async def admin_license_extend_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    _, user_id_s, page_s = callback.data.split(":", 2)
    await asyncio.to_thread(store.edit,int(user_id_s),'extend',LICENSE_DAYS*86400)
    row = await get_user_license(int(user_id_s))
    await callback.message.edit_text(admin_license_text(row), parse_mode=ParseMode.HTML, reply_markup=admin_license_keyboard(row.user_id, int(page_s), row.active))
    await callback.answer(f"Продлено на {LICENSE_DAYS} дней", show_alert=True)


@router.callback_query(F.data.startswith("admin_license_set:"))
async def admin_license_set_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    _, user_id_s, page_s, seconds_s = callback.data.split(":", 3)
    seconds = int(seconds_s)
    await asyncio.to_thread(store.edit,int(user_id_s),'set',seconds)
    row = await get_user_license(int(user_id_s))
    await callback.message.edit_text(admin_license_text(row), parse_mode=ParseMode.HTML, reply_markup=admin_license_keyboard(row.user_id, int(page_s), row.active))
    await callback.answer(f"Срок установлен: {format_duration(seconds)} от текущего момента", show_alert=True)


@router.callback_query(F.data.startswith("admin_license_custom:"))
async def admin_license_custom_callback(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    _, user_id_s, page_s = callback.data.split(":", 2)
    await state.set_state(AdminLicenseDurationState.waiting_duration)
    await state.update_data(admin_license_user_id=int(user_id_s), admin_license_page=int(page_s))
    await callback.message.answer(
        "Введите срок от текущего момента. Примеры: <code>30s</code>, <code>2m</code>, "
        "<code>3h</code>, <code>7d</code>. Если отправить просто число — это минуты.\n\nОтмена: /cancel",
        parse_mode=ParseMode.HTML,
    )
    await callback.answer()


@router.message(AdminLicenseDurationState.waiting_duration)
async def admin_license_custom_duration(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    seconds = parse_duration(message.text)
    if seconds is None:
        await message.answer("Не понял срок. Примеры: 30s, 2m, 3h, 7d или просто 2 (две минуты).")
        return
    data = await state.get_data()
    user_id = int(data.get("admin_license_user_id") or 0)
    page = int(data.get("admin_license_page") or 0)
    await state.clear()
    await asyncio.to_thread(store.edit,user_id,'set',seconds)
    row = await get_user_license(user_id)
    await message.answer(admin_license_text(row), parse_mode=ParseMode.HTML, reply_markup=admin_license_keyboard(row.user_id, page, row.active))


@router.callback_query(F.data.startswith("admin_license_reset:"))
async def admin_license_reset_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    _, user_id_s, page_s = callback.data.split(":", 2)
    await asyncio.to_thread(store.edit,int(user_id_s),'reset')
    row = await get_user_license(int(user_id_s))
    await callback.message.edit_text(admin_license_text(row), parse_mode=ParseMode.HTML, reply_markup=admin_license_keyboard(row.user_id, int(page_s), row.active))
    await callback.answer("Привязка устройства сброшена", show_alert=True)


@router.callback_query(F.data.startswith("admin_license_toggle:"))
async def admin_license_toggle_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    _, user_id_s, page_s = callback.data.split(":", 2)
    await asyncio.to_thread(store.edit,int(user_id_s),'toggle')
    row = await get_user_license(int(user_id_s))
    await callback.message.edit_text(admin_license_text(row), parse_mode=ParseMode.HTML, reply_markup=admin_license_keyboard(row.user_id, int(page_s), row.active))
    await callback.answer("Статус изменён", show_alert=True)


async def start_license_api_server():
    return None  # License requests go directly to the deployed Cloudflare Worker.


@router.callback_query(F.data == "home")
async def home_callback(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    if callback.message.photo:
        await render_menu(callback.message, home_caption(), home_keyboard())
    else:
        try:
            await callback.message.delete()
        except Exception:
            pass
        await send_photo_or_text(callback.message, home_caption(), home_keyboard())
    await callback.answer()


@router.callback_query(F.data == "documents")
async def documents_callback(callback: CallbackQuery):
    await render_menu(
        callback.message,
        "<b>Документы Debris Release</b>",
        documents_keyboard(),
    )
    await callback.answer()


@router.callback_query(F.data == "privacy")
async def privacy_callback(callback: CallbackQuery):
    await render_menu(callback.message, privacy_text(), document_back_keyboard())
    await callback.answer()


@router.callback_query(F.data == "agreement")
async def agreement_callback(callback: CallbackQuery):
    await render_menu(callback.message, agreement_text(), document_back_keyboard())
    await callback.answer()


@router.callback_query(F.data == "tariffs")
async def tariffs_callback(callback: CallbackQuery):
    await render_menu(callback.message, tariffs_text(), tariffs_keyboard())
    await callback.answer()


@router.callback_query(F.data.startswith("payplatega:"))
async def pay_platega(callback: CallbackQuery, bot: Bot, state: FSMContext):
    if not PAYMENTS_ENABLED:
        await callback.answer('Приём новых оплат временно выключен.', show_alert=True)
        return
    try:
        await asyncio.to_thread(prepare_jar,MOD_FILE_PATH)
    except Exception:
        await callback.answer("Выдача временно недоступна. Обратитесь в поддержку.",show_alert=True)
        return
    if not platega_enabled():
        await callback.answer("Оплата временно недоступна.", show_alert=True)
        return
    try:
        _, days_s, promo_s = callback.data.split(":", 2)
        days = int(days_s)
    except (TypeError, ValueError):
        await callback.answer("Некорректный тариф.", show_alert=True)
        return
    if days not in TARIFFS:
        await callback.answer("Тариф больше недоступен.", show_alert=True)
        return

    wants_promo = promo_s == "1"
    data = await state.get_data()
    promo_code = data.get("promo_code") if wants_promo and data.get("promo_valid") else None
    has_promo = bool(promo_code)
    amount = tariff_amount(days, "RUB", has_promo)

    order = await create_order(
        user_id=callback.from_user.id,
        username=callback.from_user.username,
        payment_method="platega",
        amount=amount,
        currency="RUB",
        promo_code=promo_code,
        status="CREATING",
        duration_seconds=days * 86400,
    )

    try:
        transaction_id, payment_url = await create_platega_payment(bot, order, callback.from_user.username)
        await set_order_provider_transaction(order.id, transaction_id, "INVOICE")
    except Exception:
        logging.exception("Failed to create Platega payment for order %s", order.id)
        await mark_platega_failed(order.id, "REJECTED")
        await callback.answer("Не удалось создать платёж. Попробуйте ещё раз позже.", show_alert=True)
        return

    promo_line = f"\nПромокод: <code>{promo_code}</code>" if promo_code else ""
    text = (
        f"{premium_emoji(TRANSFER_BTN_EMOJI_ID, '🪙')} <b>Оплата заказа #{order.id}</b>\n\n"
        f"Товар: <b>Debris Release — {days} дней</b>\n"
        f"Сумма: <b>{amount} ₽</b>"
        f"{promo_line}\n\n"
        "Нажмите кнопку ниже и выберите карту или СБП. После подтверждения платежа товар будет выдан автоматически."
    )
    await render_menu(callback.message, text, platega_payment_keyboard(order.id, payment_url, amount))
    await callback.answer()


@router.callback_query(F.data.startswith("checkplatega:"))
async def check_platega(callback: CallbackQuery, bot: Bot):
    order_id = int(callback.data.split(":", 1)[1])
    order = await get_order(order_id)
    if order is None or order.user_id != callback.from_user.id:
        await callback.answer("Заказ не найден.", show_alert=True)
        return
    if order.status == "DELIVERED":
        await callback.answer("Оплата подтверждена, заказ уже выдан.", show_alert=True)
        return
    if not order.provider_payment_charge_id:
        await callback.answer("Платёж ещё не создан.", show_alert=True)
        return

    try:
        data = await get_platega_status(order.provider_payment_charge_id)
        ok, info = await process_platega_status(bot, order, data)
    except Exception:
        logging.exception("Manual Platega status check failed for order %s", order.id)
        await callback.answer("Не удалось проверить платёж. Попробуйте чуть позже.", show_alert=True)
        return

    await callback.answer(info, show_alert=True)


@router.callback_query(F.data == "support")
async def support_callback(callback: CallbackQuery):
    await render_menu(callback.message, support_caption(), support_keyboard())
    await callback.answer()


@router.callback_query(F.data == "support_new")
async def support_new_callback(callback: CallbackQuery, state: FSMContext):
    await state.set_state(SupportState.waiting_message)
    await callback.message.answer(
        "Опишите вопрос одним сообщением. Можно отправить текст, фото или файл.\n\n"
        "Отмена: /cancel"
    )
    await callback.answer()


@router.message(SupportState.waiting_message)
async def support_message(message: Message, bot: Bot, state: FSMContext):
    await state.clear()

    if not ADMIN_IDS:
        await message.answer("Поддержка временно недоступна. Попробуйте позже.")
        return

    username = f"@{message.from_user.username}" if message.from_user.username else "без username"
    info = (
        "<b>Новое обращение в поддержку</b>\n\n"
        f"Пользователь: {username}\n"
        f"Telegram ID: <code>{message.from_user.id}</code>"
    )

    delivered = False
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(
                admin_id,
                info,
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[[
                        InlineKeyboardButton(
                            text="Ответить пользователю",
                            callback_data=f"supportreply:{message.from_user.id}",
                        )
                    ]]
                ),
            )
            await bot.copy_message(
                chat_id=admin_id,
                from_chat_id=message.chat.id,
                message_id=message.message_id,
            )
            delivered = True
        except Exception:
            logging.exception("Failed to deliver support ticket to admin %s", admin_id)

    if delivered:
        await message.answer(
            "Сообщение отправлено. Ответ поддержки придёт в этот чат.",
            reply_markup=home_keyboard(),
        )
    else:
        await message.answer("Не удалось отправить обращение. Попробуйте позже.")


@router.callback_query(F.data.startswith("supportreply:"))
async def support_reply_callback(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Вы не администратор.", show_alert=True)
        return

    user_id = int(callback.data.split(":", 1)[1])
    await state.set_state(SupportReplyState.waiting_reply)
    await state.update_data(support_reply_user_id=user_id)
    await callback.message.answer(
        f"Отправьте ответ пользователю <code>{user_id}</code> одним сообщением.\n"
        "Для отмены: /cancel",
        parse_mode=ParseMode.HTML,
    )
    await callback.answer()


@router.message(SupportReplyState.waiting_reply)
async def support_reply_message(message: Message, bot: Bot, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return

    data = await state.get_data()
    user_id = data.get("support_reply_user_id")
    await state.clear()
    if not user_id:
        await message.answer("Не найден получатель ответа.")
        return

    try:
        await bot.send_message(user_id, "<b>Ответ поддержки Debris Release:</b>", parse_mode=ParseMode.HTML)
        await bot.copy_message(
            chat_id=user_id,
            from_chat_id=message.chat.id,
            message_id=message.message_id,
        )
        await message.answer("Ответ отправлен пользователю.")
    except Exception:
        logging.exception("Failed to send support reply to user %s", user_id)
        await message.answer("Не удалось отправить ответ пользователю.")


@router.callback_query(F.data == "buy")
async def buy_callback(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    has_promo = bool(data.get("promo_valid"))
    await render_menu(
        callback.message,
        "<b>Выберите срок подписки Debris:</b>",
        tariff_keyboard(has_promo),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("tariff:"))
async def tariff_callback(callback: CallbackQuery, state: FSMContext):
    try:
        days = int(callback.data.split(":", 1)[1])
    except (TypeError, ValueError):
        await callback.answer("Некорректный тариф.", show_alert=True)
        return
    if days not in TARIFFS:
        await callback.answer("Тариф больше недоступен.", show_alert=True)
        return
    data = await state.get_data()
    has_promo = bool(data.get("promo_valid"))
    await state.update_data(selected_tariff_days=days)
    await render_menu(callback.message, price_text(days, has_promo), payment_keyboard(days, has_promo))
    await callback.answer()


@router.callback_query(F.data == "promo")
async def promo_callback(callback: CallbackQuery, state: FSMContext):
    await state.set_state(PromoState.waiting_code)
    await callback.message.answer(
        "Введите промокод одним сообщением.\n\nДля отмены: /cancel"
    )
    await callback.answer()


@router.message(PromoState.waiting_code)
async def promo_input(message: Message, state: FSMContext):
    code = (message.text or "").strip().lower()

    if code in PROMO_CODES:
        await state.update_data(promo_valid=True, promo_code=code)
        await state.set_state(None)
        data = await state.get_data()
        days = int(data.get("selected_tariff_days") or DEFAULT_TARIFF_DAYS)
        if days not in TARIFFS:
            days = DEFAULT_TARIFF_DAYS
        await send_photo_or_text(
            message,
            price_text(days, True),
            payment_keyboard(days, True),
        )
    else:
        await message.answer(
            "❌ Неверный промокод.\nПопробуйте ещё раз или отправьте /cancel."
        )


@router.callback_query(F.data.startswith("paystars:"))
async def pay_stars(callback: CallbackQuery, bot: Bot, state: FSMContext):
    if not PAYMENTS_ENABLED:
        await callback.answer('Приём новых оплат временно выключен.', show_alert=True)
        return
    try:
        await asyncio.to_thread(prepare_jar,MOD_FILE_PATH)
    except Exception:
        await callback.answer("Выдача временно недоступна. Обратитесь в поддержку.",show_alert=True)
        return
    try:
        _, days_s, promo_s = callback.data.split(":", 2)
        days = int(days_s)
    except (TypeError, ValueError):
        await callback.answer("Некорректный тариф.", show_alert=True)
        return
    if days not in TARIFFS:
        await callback.answer("Тариф больше недоступен.", show_alert=True)
        return
    wants_promo = promo_s == "1"
    data = await state.get_data()
    promo_code = data.get("promo_code") if wants_promo and data.get("promo_valid") else None
    has_promo = bool(promo_code)
    amount = tariff_amount(days, "XTR", has_promo)
    if amount <= 0:
        await callback.answer("Оплата Stars для этого тарифа пока не настроена.", show_alert=True)
        return

    order = await create_order(
        user_id=callback.from_user.id,
        username=callback.from_user.username,
        payment_method="stars",
        amount=amount,
        currency="XTR",
        promo_code=promo_code,
        status="INVOICE",
        duration_seconds=days * 86400,
    )

    await bot.send_invoice(
        chat_id=callback.from_user.id,
        title=f"Debris — {days} дней",
        description=f"Подписка Debris на {days} дней. Заказ #{order.id}",
        payload=f"order:{order.id}",
        currency="XTR",
        prices=[LabeledPrice(label=f"Debris {days} дней", amount=amount)],
        provider_token="",
    )
    await callback.answer()


@router.pre_checkout_query()
async def pre_checkout(pre_checkout_query: PreCheckoutQuery):
    payload = pre_checkout_query.invoice_payload or ""
    if not payload.startswith("order:"):
        await pre_checkout_query.answer(ok=False, error_message="Некорректный заказ.")
        return

    try:
        order_id = int(payload.split(":", 1)[1])
    except ValueError:
        await pre_checkout_query.answer(ok=False, error_message="Некорректный заказ.")
        return

    order = await get_order(order_id)
    if (
        order is None
        or order.user_id != pre_checkout_query.from_user.id
        or order.payment_method != "stars"
        or order.status != "INVOICE"
        or order.currency != "XTR"
        or order.amount != pre_checkout_query.total_amount
    ):
        await pre_checkout_query.answer(ok=False, error_message="Заказ устарел или сумма не совпадает.")
        return

    await pre_checkout_query.answer(ok=True)


@router.message(F.successful_payment)
async def successful_payment(message: Message, bot: Bot):
    payment = message.successful_payment
    payload = payment.invoice_payload or ''
    if payment.currency != 'XTR' or not payload.startswith('order:'):
        return
    try:
        order_id = int(payload.split(':',1)[1])
        await asyncio.to_thread(store.confirm,order_id,'stars',payment.telegram_payment_charge_id,
            payment.total_amount,payment.currency,message.from_user.id)
    except (ValueError,sqlite3.IntegrityError):
        logging.warning('Rejected mismatched successful_payment')
        return
    ok,info = await deliver_order(bot,order_id,expected_user_id=message.from_user.id)
    if not ok:
        await message.answer(info)
        await notify_admins(bot,f"Оплачен заказ #{order_id}. {html.escape(info)}",admin_order_keyboard(order_id))


@router.message(F.refunded_payment)
async def refunded_payment(message: Message):
    payment = message.refunded_payment
    try:
        if not payment.invoice_payload.startswith('order:'):
            return
        order_id = int(payment.invoice_payload.split(':',1)[1])
    except (ValueError,AttributeError):
        return
    order = await get_order(order_id)
    if (order is None or order.payment_method != 'stars'
        or order.telegram_payment_charge_id != payment.telegram_payment_charge_id
        or order.amount != payment.total_amount or order.currency != payment.currency):
        return
    await asyncio.to_thread(store.cancel_payment,order_id,True)


@router.callback_query(F.data.startswith("admindeliver:"))
async def admin_deliver(callback: CallbackQuery, bot: Bot):
    if not is_admin(callback.from_user.id):
        await callback.answer("Вы не администратор.", show_alert=True)
        return

    order_id = int(callback.data.split(":", 1)[1])
    order = await get_order(order_id)

    if order is None:
        await callback.answer("Заказ не найден.", show_alert=True)
        return
    if order.status == "DELIVERED":
        await callback.answer("Этот заказ уже выдан.", show_alert=True)
        return
    if order.status == "REJECTED":
        await callback.answer("Этот заказ отклонён.", show_alert=True)
        return

    if order.payment_method == "stars" and order.status != "PAID":
        await callback.answer("Stars ещё не подтверждены Telegram.", show_alert=True)
        return
    if order.payment_method == "platega" and order.status != "PAID":
        await callback.answer("Платёж Platega ещё не подтверждён.", show_alert=True)
        return

    ok, info = await deliver_order(bot, order_id, manual=True)
    await callback.answer(info, show_alert=True)

    if ok:
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except TelegramBadRequest:
            pass


@router.callback_query(F.data.startswith("adminreject:"))
async def admin_reject(callback: CallbackQuery, bot: Bot):
    if not is_admin(callback.from_user.id):
        await callback.answer("Вы не администратор.", show_alert=True)
        return

    order_id = int(callback.data.split(":", 1)[1])

    try:
        user_id = await asyncio.to_thread(store.reject_manual,order_id)
    except ValueError as exc:
        await callback.answer(str(exc),show_alert=True)
        return

    await bot.send_message(user_id, f"❌ Заказ #{order_id} отклонён администратором.")
    await callback.answer("Заказ отклонён.", show_alert=True)
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except TelegramBadRequest:
        pass


@router.callback_query(F.data == "orders")
async def my_orders(callback: CallbackQuery):
    async with Session() as session:
        result = await session.execute(
            select(Order)
            .where(Order.user_id == callback.from_user.id)
            .order_by(Order.id.desc())
            .limit(10)
        )
        orders = list(result.scalars())

    if not orders:
        text = "У вас пока нет заказов."
    else:
        status_names = {
            "WAITING": "⏳ ожидает проверки",
            "CREATING": "🧾 создаётся",
            "INVOICE": "🧾 ждёт оплаты",
            "PAID": "💰 оплачен",
            "DELIVERED": "✅ выдан",
            "REJECTED": "❌ отклонён",
            "CANCELED": "❌ отменён",
            "CHARGEBACK": "↩️ возврат",
        }
        lines = ["<b>Последние заказы:</b>\n"]
        for o in orders:
            amount = f"{o.amount} ⭐" if o.currency == "XTR" else f"{o.amount} ₽"
            promo = f" • промо {o.promo_code}" if o.promo_code else ""
            lines.append(
                f"#{o.id} — Debris {format_duration(o.duration_seconds or LICENSE_DAYS * 86400)} — {amount}{promo} — "
                f"{status_names.get(o.status, o.status)}"
            )
        text = "\n".join(lines)

    await render_menu(callback.message, text, orders_keyboard())
    await callback.answer()


async def migrate_license_schema():
    await asyncio.to_thread(store.migrate)


async def init_db():
    await migrate_license_schema()


async def main():
    from preflight import validate_settings
    validate_settings()
    store.client.validate_config()
    await init_db()
    await asyncio.to_thread(prepare_jar,MOD_FILE_PATH)
    bot = Bot(BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)
    poll_task = None
    delivery_task = None
    webhook_runner = None
    license_api_runner = None
    try:
        license_api_runner = await start_license_api_server()
        if not await asyncio.to_thread(store.client.health):
            raise RuntimeError('Cloudflare DBR-v1 /health check failed.')
        await asyncio.to_thread(store.client.get, next(iter(ADMIN_IDS)))
        me = await bot.get_me()
        logging.info('Started @%s',me.username)
        await resume_release_broadcasts(bot)
        delivery_task = asyncio.create_task(delivery_retry_loop(bot))
        if platega_enabled():
            poll_task = asyncio.create_task(platega_poll_loop(bot))
            webhook_runner = await start_platega_webhook_server(bot)
        await dp.start_polling(bot)
    finally:
        tasks = [t for t in (poll_task,delivery_task,*tuple(_background_tasks)) if t]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks,return_exceptions=True)
        if webhook_runner:
            await webhook_runner.cleanup()
        if license_api_runner:
            await license_api_runner.cleanup()
        await bot.session.close()
        await engine.dispose()


async def delivery_retry_loop(bot):
    while True:
        try:
            await asyncio.to_thread(store.sync_revocations)
            for order_id in await asyncio.to_thread(store.pending_deliveries):
                await deliver_order(bot,order_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            logging.error('Delivery retry interrupted; will retry')
        await asyncio.sleep(5)



class LicenseFailureMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        try:
            return await handler(event, data)
        except CloudflareLicenseError as exc:
            logging.warning('Cloudflare request failed: %s', exc.code)
            text = 'Cloudflare не подтвердил операцию. Обновите данные лицензии перед повтором. Оплаченные заказы сохраняются.'
            if isinstance(event, CallbackQuery):
                await event.answer(text, show_alert=True)
            elif isinstance(event, Message):
                await event.answer(text)


router.callback_query.outer_middleware(LicenseFailureMiddleware())
router.message.outer_middleware(LicenseFailureMiddleware())


@router.message(Command('license'))
async def issue_license_command(message: Message):
    if not is_admin(message.from_user.id) or message.chat.type != 'private':
        return
    parts = (message.text or '').split()
    seconds = parse_duration(parts[2]) if len(parts) == 3 else None
    if len(parts) != 3 or not parts[1].isdigit() or not seconds:
        await message.answer('Выдать ID: /license TELEGRAM_ID 30d (также 2h, 10m). Существующая лицензия сохраняет свой срок; продление — в админке.')
        return
    await asyncio.to_thread(store.issue, int(parts[1]), seconds)
    row = await get_user_license(int(parts[1]))
    await message.answer(admin_license_text(row), parse_mode=ParseMode.HTML,
                         reply_markup=admin_license_keyboard(row.user_id, 0, row.active))


if __name__ == "__main__":
    import sys
    from preflight import run_check, SingleInstance
    try:
        with SingleInstance(DB_PATH):
            if '--check' in sys.argv:
                asyncio.run(run_check(sys.modules[__name__], online='--online' in sys.argv))
            else:
                asyncio.run(main())
    except (ValueError, RuntimeError) as exc:
        logging.error('%s', exc)
        raise SystemExit(1) from None
