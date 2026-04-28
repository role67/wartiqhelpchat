import asyncio
import html
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

import psycopg
from aiohttp import web
from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, Update
from telegram.error import ChatMigrated
from telegram.ext import (
    Application,
    ApplicationHandlerStop,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"
load_dotenv(dotenv_path=ENV_PATH)

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_FILE = os.getenv("LOG_FILE", "logs/bot.log")
PORT = int(os.getenv("PORT", "10000"))
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")

COMMUNITY_CHAT_ID = int(os.getenv("COMMUNITY_CHAT_ID", "-1003786067871"))
SUPPORT_CHAT_ID = int(os.getenv("SUPPORT_CHAT_ID", "-1003912146373"))
ADMIN_IDS = {
    int(x.strip())
    for x in os.getenv("ADMIN_IDS", "").split(",")
    if x.strip().isdigit()
}

SECTION_COMMUNITY = "community"
SECTION_SUPPORT = "support"
SECTION_COMMUNITY_LABEL = "💬 Общение"
SECTION_SUPPORT_LABEL = "🛟 Поддержка"

USER_SECTION_KEY = "selected_section"
ROUTE_MAP_KEY = "route_map"
HTTP_RUNNER_KEY = "http_runner"
DB_CONN_KEY = "db_conn"
DB_LAST_CHECK_KEY = "db_last_check"

SECTION_PATTERN = re.compile(r"^(💬 Общение|🛟 Поддержка)$")
FEEDBACK_CALLBACK_PATTERN = re.compile(r"^fb:(up|down):(\d+)$")


@dataclass(frozen=True)
class SectionConfig:
    title: str
    emoji: str
    chat_id: int
    label: str


SECTIONS: dict[str, SectionConfig] = {
    SECTION_COMMUNITY: SectionConfig(
        title="Общение",
        emoji="💬",
        chat_id=COMMUNITY_CHAT_ID,
        label=SECTION_COMMUNITY_LABEL,
    ),
    SECTION_SUPPORT: SectionConfig(
        title="Поддержка",
        emoji="🛟",
        chat_id=SUPPORT_CHAT_ID,
        label=SECTION_SUPPORT_LABEL,
    ),
}

SECTION_BY_LABEL = {
    SECTION_COMMUNITY_LABEL: SECTION_COMMUNITY,
    SECTION_SUPPORT_LABEL: SECTION_SUPPORT,
}


def configure_logging() -> logging.Logger:
    log_dir = os.path.dirname(LOG_FILE)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%d.%m.%Y %H:%M:%S",
    )

    root = logging.getLogger()
    root.setLevel(LOG_LEVEL)
    root.handlers.clear()

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    root.addHandler(stream_handler)

    file_handler = RotatingFileHandler(LOG_FILE, maxBytes=1_000_000, backupCount=3, encoding="utf-8")
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("telegram.vendor.ptb_urllib3.urllib3").setLevel(logging.WARNING)

    return logging.getLogger("support-bot")


logger = configure_logging()


def section_name(section_key: str) -> str:
    section = SECTIONS.get(section_key)
    return section.title if section else section_key


def section_prompt(section: SectionConfig) -> str:
    return f"<blockquote>{section.emoji} <b>{section.title}</b></blockquote>\n\nНапиши свое сообщение админам 👇"


def start_links_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📣 Наш канал", url="https://t.me/EchoFromInoChannel")],
            [InlineKeyboardButton("⭐ Отзывы", url="https://t.me/EchoChannelReviews")],
        ]
    )


def support_feedback_keyboard(ticket_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("👍", callback_data=f"fb:up:{ticket_id}"),
            InlineKeyboardButton("👎", callback_data=f"fb:down:{ticket_id}"),
        ]]
    )


def section_reply_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [[SECTION_COMMUNITY_LABEL, SECTION_SUPPORT_LABEL]],
        resize_keyboard=True,
        one_time_keyboard=False,
        input_field_placeholder="Выбери раздел...",
    )


def format_message_datetime(dt: Any) -> str:
    local_dt = dt.astimezone()
    return f"{local_dt.day:02d}.{local_dt.month:02d}.{local_dt.year % 100:02d}, {local_dt.hour}:{local_dt.minute:02d}"


def admin_message_notification(text: str) -> str:
    return (
        "<blockquote>📢 <b>Сообщение от администрации</b></blockquote>\n\n"
        f"💬 {html.escape(text)}"
    )


def admin_message_notification_for_section(section: SectionConfig, text: str) -> str:
    return (
        f"<blockquote>{section.emoji} <b>Сообщение от администрации</b></blockquote>\n\n"
        f"💬 {html.escape(text)}"
    )


def normalize_target(raw: str) -> str:
    return raw.strip().split()[0]


def parse_reason(raw: str) -> str:
    parts = raw.strip().split(maxsplit=1)
    return parts[1].strip() if len(parts) > 1 else ""


def parse_target_and_reason(args: list[str]) -> tuple[str, str] | None:
    if not args:
        return None

    target_token = args[0].strip()
    reason = " ".join(args[1:]).strip()
    return target_token, reason


def parse_numeric_id(value: str) -> int | None:
    text = value.strip()
    if text.lower().startswith("id") and text[2:].isdigit():
        return int(text[2:])
    if text.isdigit():
        return int(text)
    return None


def open_db_connection() -> psycopg.Connection:
    conn = psycopg.connect(
        DATABASE_URL,
        autocommit=True,
        connect_timeout=10,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )
    return conn


def reset_db_connection(application: Application) -> psycopg.Connection:
    old_conn: psycopg.Connection | None = application.bot_data.pop(DB_CONN_KEY, None)
    if old_conn is not None and not old_conn.closed:
        try:
            old_conn.close()
        except psycopg.Error:
            logger.warning("Failed to close stale database connection.", exc_info=True)

    conn = open_db_connection()
    application.bot_data[DB_CONN_KEY] = conn
    return conn


def db_conn(application: Application) -> psycopg.Connection:
    conn: psycopg.Connection | None = application.bot_data.get(DB_CONN_KEY)
    if conn is None:
        raise RuntimeError("Database is not initialized")
    if conn.closed:
        logger.warning("Database connection was closed; reconnecting.")
        conn = reset_db_connection(application)
        application.bot_data[DB_LAST_CHECK_KEY] = datetime.now()
        return conn

    # Validate long-lived connection periodically to avoid using stale SSL sessions.
    last_check: datetime | None = application.bot_data.get(DB_LAST_CHECK_KEY)
    should_check = last_check is None or (datetime.now() - last_check).total_seconds() >= 30
    if should_check:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            application.bot_data[DB_LAST_CHECK_KEY] = datetime.now()
        except psycopg.OperationalError:
            logger.warning("Database connection became stale; reconnecting.", exc_info=True)
            conn = reset_db_connection(application)
            application.bot_data[DB_LAST_CHECK_KEY] = datetime.now()
    return conn


def init_db(application: Application) -> None:
    conn = open_db_connection()
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                updated_at TIMESTAMP NOT NULL
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users (LOWER(username))")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS bans (
                user_id BIGINT PRIMARY KEY,
                reason TEXT,
                banned_at TIMESTAMP NOT NULL,
                banned_by BIGINT,
                banned_in_chat BIGINT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS routes (
                admin_chat_id BIGINT NOT NULL,
                admin_message_id BIGINT NOT NULL,
                user_id BIGINT NOT NULL,
                user_message_id BIGINT,
                created_at TIMESTAMP NOT NULL,
                PRIMARY KEY (admin_chat_id, admin_message_id)
            )
            """
        )
        cur.execute("ALTER TABLE routes ADD COLUMN IF NOT EXISTS user_message_id BIGINT")
        cur.execute("CREATE SEQUENCE IF NOT EXISTS routes_ticket_id_seq")
        cur.execute("ALTER TABLE routes ADD COLUMN IF NOT EXISTS ticket_id BIGINT")
        cur.execute("ALTER TABLE routes ADD COLUMN IF NOT EXISTS answered_at TIMESTAMP")
        cur.execute("ALTER TABLE routes ADD COLUMN IF NOT EXISTS answered_by BIGINT")
        cur.execute("ALTER TABLE routes ADD COLUMN IF NOT EXISTS answer_message_id BIGINT")
        cur.execute("ALTER TABLE routes ALTER COLUMN ticket_id SET DEFAULT nextval('routes_ticket_id_seq')")
        cur.execute("UPDATE routes SET ticket_id = nextval('routes_ticket_id_seq') WHERE ticket_id IS NULL")
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_routes_ticket_id ON routes(ticket_id)")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS feedback_votes (
                ticket_id BIGINT NOT NULL,
                user_id BIGINT NOT NULL,
                vote TEXT NOT NULL,
                created_at TIMESTAMP NOT NULL,
                updated_at TIMESTAMP NOT NULL,
                PRIMARY KEY (ticket_id, user_id)
            )
            """
        )

    application.bot_data[DB_CONN_KEY] = conn


def close_db(application: Application) -> None:
    conn: psycopg.Connection | None = application.bot_data.pop(DB_CONN_KEY, None)
    if conn is not None:
        conn.close()


def upsert_user(application: Application, user: Any) -> None:
    if user is None:
        return
    payload = (user.id, user.username, user.first_name, user.last_name, datetime.now())
    query = """
            INSERT INTO users (user_id, username, first_name, last_name, updated_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT(user_id) DO UPDATE SET
                username=excluded.username,
                first_name=excluded.first_name,
                last_name=excluded.last_name,
                updated_at=excluded.updated_at
            """

    try:
        conn = db_conn(application)
        with conn.cursor() as cur:
            cur.execute(query, payload)
    except psycopg.OperationalError:
        logger.warning("upsert_user failed due to OperationalError, retrying with fresh connection.", exc_info=True)
        try:
            conn = reset_db_connection(application)
            application.bot_data[DB_LAST_CHECK_KEY] = datetime.now()
            with conn.cursor() as cur:
                cur.execute(query, payload)
        except psycopg.OperationalError:
            # Don't break user-facing /start if the database is temporarily unavailable.
            logger.error("upsert_user retry failed; skipping profile sync for this update.", exc_info=True)


def find_user_id_by_username(application: Application, username: str) -> int | None:
    normalized = username.lstrip("@").lower()
    conn = db_conn(application)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT user_id FROM users
            WHERE username IS NOT NULL AND LOWER(username)=%s
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (normalized,),
        )
        row = cur.fetchone()

    return int(row[0]) if row else None


def is_banned(application: Application, user_id: int) -> tuple[bool, str]:
    conn = db_conn(application)
    with conn.cursor() as cur:
        cur.execute("SELECT reason FROM bans WHERE user_id=%s", (user_id,))
        row = cur.fetchone()

    if not row:
        return False, ""

    reason = row[0] or "без причины"
    return True, reason


def set_ban(application: Application, user_id: int, reason: str, by_user_id: int, by_chat_id: int) -> None:
    conn = db_conn(application)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO bans (user_id, reason, banned_at, banned_by, banned_in_chat)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT(user_id) DO UPDATE SET
                reason=excluded.reason,
                banned_at=excluded.banned_at,
                banned_by=excluded.banned_by,
                banned_in_chat=excluded.banned_in_chat
            """,
            (user_id, reason or "", datetime.now(), by_user_id, by_chat_id),
        )


def remove_ban(application: Application, user_id: int) -> bool:
    conn = db_conn(application)
    with conn.cursor() as cur:
        cur.execute("DELETE FROM bans WHERE user_id=%s", (user_id,))
        return cur.rowcount > 0


def save_route(
    application: Application,
    admin_chat_id: int,
    admin_message_id: int,
    user_id: int,
    user_message_id: int,
) -> int:
    route_map: dict[str, dict[str, int]] = application.bot_data.setdefault(ROUTE_MAP_KEY, {})

    conn = db_conn(application)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO routes (admin_chat_id, admin_message_id, user_id, user_message_id, created_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (admin_chat_id, admin_message_id) DO UPDATE SET
                user_id=excluded.user_id,
                user_message_id=excluded.user_message_id,
                created_at=excluded.created_at
            RETURNING ticket_id
            """,
            (admin_chat_id, admin_message_id, user_id, user_message_id, datetime.now()),
        )
        row = cur.fetchone()

    ticket_id = int(row[0]) if row else admin_message_id
    route_map[f"{admin_chat_id}:{admin_message_id}"] = {
        "user_id": user_id,
        "ticket_id": ticket_id,
        "user_message_id": user_message_id,
    }
    route_map[f"ticket:{admin_chat_id}:{ticket_id}"] = {
        "user_id": user_id,
        "message_id": admin_message_id,
        "user_message_id": user_message_id,
    }
    return ticket_id


def get_route_user_id(application: Application, admin_chat_id: int, admin_message_id: int) -> int | None:
    route_map: dict[str, dict[str, int]] = application.bot_data.get(ROUTE_MAP_KEY, {})
    key = f"{admin_chat_id}:{admin_message_id}"
    cached = route_map.get(key)
    if cached:
        return cached["user_id"]

    conn = db_conn(application)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT user_id, ticket_id, user_message_id FROM routes WHERE admin_chat_id=%s AND admin_message_id=%s",
            (admin_chat_id, admin_message_id),
        )
        row = cur.fetchone()

    if not row:
        return None

    user_id = int(row[0])
    ticket_id = int(row[1]) if row[1] is not None else None
    user_message_id = int(row[2]) if row[2] is not None else None
    route_map[key] = {"user_id": user_id, "ticket_id": ticket_id, "user_message_id": user_message_id}
    if ticket_id is not None:
        route_map[f"ticket:{admin_chat_id}:{ticket_id}"] = {
            "user_id": user_id,
            "message_id": admin_message_id,
            "user_message_id": user_message_id,
        }
    return user_id


def get_ticket_id_by_admin_message(application: Application, admin_chat_id: int, admin_message_id: int) -> int | None:
    route_map: dict[str, dict[str, int]] = application.bot_data.get(ROUTE_MAP_KEY, {})
    key = f"{admin_chat_id}:{admin_message_id}"
    cached = route_map.get(key)
    if cached and "ticket_id" in cached:
        return int(cached["ticket_id"])

    conn = db_conn(application)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT ticket_id, user_id, user_message_id FROM routes WHERE admin_chat_id=%s AND admin_message_id=%s",
            (admin_chat_id, admin_message_id),
        )
        row = cur.fetchone()
    if not row:
        return None

    ticket_id = int(row[0])
    user_id = int(row[1])
    user_message_id = int(row[2]) if row[2] is not None else None
    route_map[key] = {"user_id": user_id, "ticket_id": ticket_id, "user_message_id": user_message_id}
    route_map[f"ticket:{admin_chat_id}:{ticket_id}"] = {
        "user_id": user_id,
        "message_id": admin_message_id,
        "user_message_id": user_message_id,
    }
    return ticket_id


def get_route_user_id_by_ticket(application: Application, admin_chat_id: int, ticket_id: int) -> int | None:
    route_map: dict[str, dict[str, int]] = application.bot_data.get(ROUTE_MAP_KEY, {})
    key = f"ticket:{admin_chat_id}:{ticket_id}"
    cached = route_map.get(key)
    if cached:
        return cached["user_id"]

    conn = db_conn(application)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT user_id, admin_message_id, user_message_id FROM routes WHERE admin_chat_id=%s AND ticket_id=%s",
            (admin_chat_id, ticket_id),
        )
        row = cur.fetchone()

    if not row:
        return None

    user_id = int(row[0])
    admin_message_id = int(row[1])
    user_message_id = int(row[2]) if row[2] is not None else None
    route_map[key] = {"user_id": user_id, "message_id": admin_message_id, "user_message_id": user_message_id}
    route_map[f"{admin_chat_id}:{admin_message_id}"] = {
        "user_id": user_id,
        "ticket_id": ticket_id,
        "user_message_id": user_message_id,
    }
    return user_id


def get_user_message_id_by_ticket(application: Application, admin_chat_id: int, ticket_id: int) -> int | None:
    route_map: dict[str, dict[str, int]] = application.bot_data.get(ROUTE_MAP_KEY, {})
    cached = route_map.get(f"ticket:{admin_chat_id}:{ticket_id}")
    if cached and cached.get("user_message_id") is not None:
        return int(cached["user_message_id"])

    conn = db_conn(application)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT user_message_id FROM routes WHERE admin_chat_id=%s AND ticket_id=%s",
            (admin_chat_id, ticket_id),
        )
        row = cur.fetchone()
    if not row or row[0] is None:
        return None
    return int(row[0])


def get_user_message_id_by_admin_message(application: Application, admin_chat_id: int, admin_message_id: int) -> int | None:
    route_map: dict[str, dict[str, int]] = application.bot_data.get(ROUTE_MAP_KEY, {})
    cached = route_map.get(f"{admin_chat_id}:{admin_message_id}")
    if cached and cached.get("user_message_id") is not None:
        return int(cached["user_message_id"])

    conn = db_conn(application)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT user_message_id FROM routes WHERE admin_chat_id=%s AND admin_message_id=%s",
            (admin_chat_id, admin_message_id),
        )
        row = cur.fetchone()
    if not row or row[0] is None:
        return None
    return int(row[0])


def claim_route_answer(
    application: Application,
    admin_chat_id: int,
    admin_message_id: int,
    answered_by: int,
    answer_message_id: int,
) -> tuple[str, int | None, int | None, int | None]:
    conn = db_conn(application)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE routes
            SET answered_at=%s,
                answered_by=%s,
                answer_message_id=%s
            WHERE admin_chat_id=%s
              AND admin_message_id=%s
              AND answered_at IS NULL
            RETURNING user_id, ticket_id, user_message_id
            """,
            (datetime.now(), answered_by, answer_message_id, admin_chat_id, admin_message_id),
        )
        row = cur.fetchone()
        if row:
            return "claimed", int(row[0]), int(row[1]), int(row[2]) if row[2] is not None else None

        cur.execute(
            "SELECT answered_at FROM routes WHERE admin_chat_id=%s AND admin_message_id=%s",
            (admin_chat_id, admin_message_id),
        )
        existing = cur.fetchone()

    if existing:
        return "answered", None, None, None
    return "not_found", None, None, None


def save_feedback_vote(application: Application, ticket_id: int, user_id: int, vote: str) -> None:
    conn = db_conn(application)
    now = datetime.now()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO feedback_votes (ticket_id, user_id, vote, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (ticket_id, user_id) DO UPDATE SET
                vote=excluded.vote,
                updated_at=excluded.updated_at
            """,
            (ticket_id, user_id, vote, now, now),
        )


async def is_moderator(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user = update.effective_user
    chat = update.effective_chat
    if user is None or chat is None:
        return False

    if chat.type in {"group", "supergroup"}:
        member = await context.bot.get_chat_member(chat.id, user.id)
        return member.status in {"administrator", "creator"}

    if chat.type == "private":
        # If ADMIN_IDS is not configured, allow moderation commands in private chat.
        if not ADMIN_IDS:
            return True
        return user.id in ADMIN_IDS

    return False


def is_moderation_chat(chat_id: int) -> bool:
    allowed_chat_ids = {
        COMMUNITY_CHAT_ID,
        SUPPORT_CHAT_ID,
        SECTIONS[SECTION_COMMUNITY].chat_id,
        SECTIONS[SECTION_SUPPORT].chat_id,
    }
    return chat_id in allowed_chat_ids


def resolve_target_user_id(application: Application, raw_target: str) -> int | None:
    target = raw_target.strip()
    numeric = parse_numeric_id(target)
    if numeric is not None:
        return numeric

    if target.startswith("@") or re.fullmatch(r"[A-Za-z0-9_]{4,}", target):
        return find_user_id_by_username(application, target)

    return None


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat is None or update.effective_chat.type != "private":
        return

    context.user_data.pop(USER_SECTION_KEY, None)
    user = update.effective_user
    if user:
        upsert_user(context.application, user)
        logger.info("Пользователь %s запустил бота (/start).", user.id)

    is_user_banned, reason = is_banned(context.application, user.id if user else 0)
    if is_user_banned:
        await update.effective_message.reply_text(f"Доступ ограничен. Причина: {reason}")
        return

    first_name = user.first_name if user and user.first_name else "друг"
    await update.effective_message.reply_text(
        f"Привет, {html.escape(first_name)}! Выберите раздел ниже 👇",
        parse_mode="HTML",
        reply_markup=start_links_keyboard(),
    )
    await update.effective_message.reply_text(
        "Для связи с нами нажми нужную кнопку:",
        reply_markup=section_reply_keyboard(),
    )


async def choose_section(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    user = update.effective_user
    if message is None or user is None or not message.text:
        return

    upsert_user(context.application, user)

    is_user_banned, reason = is_banned(context.application, user.id)
    if is_user_banned:
        await message.reply_text(f"Доступ ограничен. Причина: {reason}")
        return

    section_key = SECTION_BY_LABEL.get(message.text.strip())
    if section_key is None:
        return

    context.user_data[USER_SECTION_KEY] = section_key
    section = SECTIONS[section_key]
    logger.info("Пользователь %s выбрал раздел: %s.", user.id, section_name(section_key))
    await message.reply_text(section_prompt(section), parse_mode="HTML")


async def handle_ban_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    user = update.effective_user
    chat = update.effective_chat
    if message is None or user is None or chat is None:
        return

    if not is_moderation_chat(chat.id):
        logger.warning("Отклонен /ban: недопустимый чат %s, user=%s", chat.id, user.id)
        await message.reply_text(
            "⛔ Команда /ban доступна только в чатах поддержки и общения.",
            reply_to_message_id=message.message_id,
        )
        raise ApplicationHandlerStop

    parsed = parse_target_and_reason(context.args)
    if parsed is None:
        await message.reply_text(
            "Формат: /ban @username или /ban 123456789\nПричину можно добавить после ID/username.",
            reply_to_message_id=message.message_id,
        )
        return

    logger.info("Команда /ban получена: chat=%s user=%s args=%s", chat.id, user.id, context.args)

    if not await is_moderator(update, context):
        logger.warning("Отклонен бан: нет прав (chat=%s user=%s)", chat.id, user.id)
        await message.reply_text("⛔ У вас нет прав для команды бана.")
        raise ApplicationHandlerStop

    target_token, reason = parsed

    target_user_id = resolve_target_user_id(context.application, target_token)
    if target_user_id is None:
        logger.warning("Отклонен бан: не удалось определить target='%s' (chat=%s user=%s)", target_token, chat.id, user.id)
        await message.reply_text("Не удалось определить пользователя. Используй @username или числовой ID.")
        raise ApplicationHandlerStop

    set_ban(context.application, target_user_id, reason, user.id, chat.id)
    logger.info("Модератор %s забанил пользователя %s. Причина: %s", user.id, target_user_id, reason or "без причины")

    await context.bot.send_message(
        chat_id=chat.id,
        text=(
            f"⛔ Пользователь <code>{target_user_id}</code> заблокирован навсегда."
            + (f"\nПричина: {html.escape(reason)}" if reason else "")
        ),
        parse_mode="HTML",
        reply_to_message_id=message.message_id,
    )
    raise ApplicationHandlerStop


async def handle_unban_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    user = update.effective_user
    chat = update.effective_chat
    if message is None or user is None or chat is None:
        return

    if not is_moderation_chat(chat.id):
        logger.warning("Отклонен /unban: недопустимый чат %s, user=%s", chat.id, user.id)
        await message.reply_text(
            "⛔ Команда /unban доступна только в чатах поддержки и общения.",
            reply_to_message_id=message.message_id,
        )
        raise ApplicationHandlerStop

    parsed = parse_target_and_reason(context.args)
    if parsed is None:
        await message.reply_text(
            "Формат: /unban @username или /unban 123456789",
            reply_to_message_id=message.message_id,
        )
        return

    logger.info("Команда /unban получена: chat=%s user=%s args=%s", chat.id, user.id, context.args)

    if not await is_moderator(update, context):
        logger.warning("Отклонен разбан: нет прав (chat=%s user=%s)", chat.id, user.id)
        await message.reply_text("⛔ У вас нет прав для команды разбана.")
        raise ApplicationHandlerStop

    target_token, _ = parsed
    target_user_id = resolve_target_user_id(context.application, target_token)
    if target_user_id is None:
        logger.warning("Отклонен разбан: не удалось определить target='%s' (chat=%s user=%s)", target_token, chat.id, user.id)
        await message.reply_text("Не удалось определить пользователя. Используй @username или числовой ID.")
        raise ApplicationHandlerStop

    removed = remove_ban(context.application, target_user_id)
    if removed:
        logger.info("Модератор %s разбанил пользователя %s.", user.id, target_user_id)
        text = f"✅ Пользователь <code>{target_user_id}</code> разблокирован."
    else:
        text = f"ℹ️ Пользователь <code>{target_user_id}</code> не найден в бан-листе."

    await context.bot.send_message(
        chat_id=chat.id,
        text=text,
        parse_mode="HTML",
        reply_to_message_id=message.message_id,
    )
    raise ApplicationHandlerStop


async def route_user_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat is None or update.effective_user is None or update.effective_message is None:
        return

    if update.effective_chat.type != "private":
        return

    user = update.effective_user
    message = update.effective_message
    upsert_user(context.application, user)

    if message.text and SECTION_PATTERN.match(message.text.strip()):
        return

    is_user_banned, reason = is_banned(context.application, user.id)
    if is_user_banned:
        await message.reply_text(f"Доступ ограничен. Причина: {reason}")
        return

    section_key = context.user_data.get(USER_SECTION_KEY)
    if section_key not in SECTIONS:
        logger.info("Пользователь %s отправил сообщение без выбора раздела.", user.id)
        await message.reply_text("Сначала выберите раздел 👇", reply_markup=section_reply_keyboard())
        return

    section = SECTIONS[section_key]
    full_name = " ".join(x for x in [user.first_name, user.last_name] if x) or "Без имени"
    user_ref = f"@{user.username} | {user.id}" if user.username else str(user.id)
    message_text = message.text or message.caption or "(медиа/вложение без текста)"
    formatted_date = format_message_datetime(message.date)

    text_header = (
        "<blockquote>✨ <b>Новое сообщение</b></blockquote>\n\n"
        f"🧭 <b>Тип:</b> {html.escape(section.title)}\n"
        f"👤 {html.escape(full_name)}\n"
        f"🆔 {html.escape(user_ref)}\n"
        f"💬 {html.escape(message_text)}\n\n"
        "📌 <b>ID обращения:</b> <code>{ticket_id_placeholder}</code>\n"
        f"<blockquote>📅 {formatted_date.replace(', ', ' • ')}</blockquote>\n\n"
        "💡 Ответьте на это сообщение через reply.\n"
        "После первого ответа обращение будет закрыто."
    )

    target_chat_id = section.chat_id
    try:
        sent = await context.bot.send_message(
            chat_id=target_chat_id,
            text=text_header.replace("{ticket_id_placeholder}", "..."),
            parse_mode="HTML",
        )
    except ChatMigrated as exc:
        target_chat_id = exc.new_chat_id
        SECTIONS[section_key] = SectionConfig(
            title=section.title,
            emoji=section.emoji,
            chat_id=target_chat_id,
            label=section.label,
        )
        logger.warning(
            "Группа раздела '%s' была мигрирована: %s -> %s.",
            section_name(section_key),
            section.chat_id,
            target_chat_id,
        )
        sent = await context.bot.send_message(
            chat_id=target_chat_id,
            text=text_header.replace("{ticket_id_placeholder}", "..."),
            parse_mode="HTML",
        )

    ticket_id = save_route(
        context.application,
        target_chat_id,
        sent.message_id,
        user.id,
        message.message_id,
    )
    final_header = text_header.replace("{ticket_id_placeholder}", str(ticket_id))
    await context.bot.edit_message_text(
        chat_id=target_chat_id,
        message_id=sent.message_id,
        text=final_header,
        parse_mode="HTML",
    )

    logger.info(
        "Новое сообщение от %s отправлено в раздел '%s' (чат %s, ticket %s, msg %s).",
        user.id,
        section_name(section_key),
        target_chat_id,
        ticket_id,
        sent.message_id,
    )

    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="Сообщение отправлено администраторам ✅",
        reply_to_message_id=message.message_id,
    )


async def route_admin_reply(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    chat = update.effective_chat
    if message is None or chat is None:
        return

    if chat.id not in {COMMUNITY_CHAT_ID, SUPPORT_CHAT_ID}:
        return

    if message.reply_to_message is None:
        return

    status, user_id, ticket_id, user_message_id = claim_route_answer(
        context.application,
        chat.id,
        message.reply_to_message.message_id,
        message.from_user.id if message.from_user else 0,
        message.message_id,
    )
    if status == "answered":
        await message.reply_text(
            "По этому обращению уже ответили. Повторный ответ не отправлен.",
            reply_to_message_id=message.message_id,
        )
        return
    if status == "not_found" or user_id is None:
        return

    section_key = SECTION_SUPPORT if chat.id == SUPPORT_CHAT_ID else SECTION_COMMUNITY
    section = SECTIONS[section_key]
    admin_text = message.text or message.caption or "(сообщение без текста)"

    if chat.id == SUPPORT_CHAT_ID:
        if ticket_id is None:
            ticket_id = message.reply_to_message.message_id
        await context.bot.send_message(
            chat_id=user_id,
            text=admin_message_notification_for_section(section, admin_text),
            parse_mode="HTML",
            reply_markup=support_feedback_keyboard(ticket_id),
            reply_to_message_id=user_message_id,
        )
    else:
        await context.bot.send_message(
            chat_id=user_id,
            text=admin_message_notification_for_section(section, admin_text),
            parse_mode="HTML",
            reply_to_message_id=user_message_id,
        )
    await message.reply_text("Ответ пользователю отправлен ✅", reply_to_message_id=message.message_id)
    logger.info("Админ ответил пользователю %s из чата %s (reply, ticket %s).", user_id, chat.id, ticket_id)


async def handle_feedback_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if query is None or user is None or query.data is None:
        return

    match = FEEDBACK_CALLBACK_PATTERN.match(query.data)
    if not match:
        return

    vote_raw, ticket_raw = match.groups()
    ticket_id = int(ticket_raw)

    route_user_id = get_route_user_id_by_ticket(context.application, SUPPORT_CHAT_ID, ticket_id)
    if route_user_id is None or route_user_id != user.id:
        await query.answer("Это голосование не для вас.", show_alert=True)
        return

    vote = "👍" if vote_raw == "up" else "👎"
    save_feedback_vote(context.application, ticket_id, user.id, vote_raw)

    await query.answer("Спасибо за отзыв!")
    await query.edit_message_reply_markup(reply_markup=None)
    if query.message is not None:
        await query.message.reply_text(f"Оценка отправлена: {vote}")

    logger.info("Пользователь %s оценил ответ по ticket %s: %s", user.id, ticket_id, vote)
    await context.bot.send_message(
        chat_id=SUPPORT_CHAT_ID,
        text=(
            "<blockquote>📊 <b>Новый отзыв на ответ администрации</b></blockquote>\n\n"
            f"👤 Пользователь: <code>{user.id}</code>\n"
            f"📌 ID обращения: <code>{ticket_id}</code>\n"
            f"Оценка: {vote}"
        ),
        parse_mode="HTML",
    )


async def health(_: web.Request) -> web.Response:
    return web.Response(text="ok", status=200)


async def start_http_server(application: Application) -> None:
    app = web.Application()
    app.router.add_get("/", health, allow_head=True)
    app.router.add_get("/health", health, allow_head=True)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=PORT)
    await site.start()

    application.bot_data[HTTP_RUNNER_KEY] = runner
    logger.info("HTTP health-сервер запущен: порт %s, маршруты / и /health.", PORT)


async def stop_http_server(application: Application) -> None:
    runner: web.AppRunner | None = application.bot_data.get(HTTP_RUNNER_KEY)
    if runner is not None:
        await runner.cleanup()


async def post_init(application: Application) -> None:
    init_db(application)
    await start_http_server(application)


async def post_shutdown(application: Application) -> None:
    await stop_http_server(application)
    close_db(application)


async def log_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    error = context.error
    exc_info = (type(error), error, error.__traceback__) if error is not None else None
    logger.error("Unhandled exception while processing update: %s", update, exc_info=exc_info)
    if isinstance(error, psycopg.OperationalError):
        try:
            reset_db_connection(context.application)
        except Exception:
            logger.exception("Failed to reconnect database after OperationalError.")


if __name__ == "__main__":
    if not BOT_TOKEN:
        raise RuntimeError(f"Set BOT_TOKEN in {ENV_PATH} before running the bot.")
    if not DATABASE_URL:
        raise RuntimeError(f"Set DATABASE_URL in {ENV_PATH} before running the bot.")

    bot_app = (
        Application.builder()
        .token(BOT_TOKEN)
        .job_queue(None)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    bot_app.add_handler(CommandHandler("start", start, filters=filters.ChatType.PRIVATE))
    bot_app.add_handler(CommandHandler("ban", handle_ban_command))
    bot_app.add_handler(CommandHandler("unban", handle_unban_command))

    # Private UX flow.
    bot_app.add_handler(
        MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & filters.Regex(r"^(💬 Общение|🛟 Поддержка)$"), choose_section)
    )
    bot_app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND, route_user_message))

    # Admin chat responses.
    bot_app.add_handler(MessageHandler(filters.ChatType.GROUPS & ~filters.COMMAND, route_admin_reply), group=1)
    bot_app.add_handler(CallbackQueryHandler(handle_feedback_callback, pattern=r"^fb:(up|down):\d+$"))
    bot_app.add_error_handler(log_error)

    logger.info("Запуск бота...")
    logger.info("Важно: для команд без '/' в группах отключите privacy mode у бота в BotFather (/setprivacy -> Disable).")
    asyncio.set_event_loop(asyncio.new_event_loop())
    bot_app.run_polling(allowed_updates=Update.ALL_TYPES)
