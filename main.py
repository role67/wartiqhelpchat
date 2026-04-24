import html
import logging
import os
from dataclasses import dataclass
from logging.handlers import RotatingFileHandler
from typing import Any

from aiohttp import web
from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

load_dotenv()

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_FILE = os.getenv("LOG_FILE", "logs/bot.log")


def configure_logging() -> logging.Logger:
    log_dir = os.path.dirname(LOG_FILE)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    root = logging.getLogger()
    root.setLevel(LOG_LEVEL)
    root.handlers.clear()

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    root.addHandler(stream_handler)

    file_handler = RotatingFileHandler(LOG_FILE, maxBytes=1_000_000, backupCount=3, encoding="utf-8")
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    return logging.getLogger("support-bot")


logger = configure_logging()

SECTION_COMMUNITY = "community"
SECTION_SUPPORT = "support"
USER_SECTION_KEY = "selected_section"
ROUTE_MAP_KEY = "route_map"
HTTP_RUNNER_KEY = "http_runner"

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
COMMUNITY_CHAT_ID = int(os.getenv("COMMUNITY_CHAT_ID", "-5272986859"))
SUPPORT_CHAT_ID = int(os.getenv("SUPPORT_CHAT_ID", "-5067851134"))
PORT = int(os.getenv("PORT", "10000"))


@dataclass(frozen=True)
class SectionConfig:
    title: str
    emoji: str
    chat_id: int


SECTIONS: dict[str, SectionConfig] = {
    SECTION_COMMUNITY: SectionConfig(title="Общение", emoji="💬", chat_id=COMMUNITY_CHAT_ID),
    SECTION_SUPPORT: SectionConfig(title="Поддержка", emoji="🛟", chat_id=SUPPORT_CHAT_ID),
}


def main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("💬 Общение", callback_data=SECTION_COMMUNITY)],
            [InlineKeyboardButton("🛟 Поддержка", callback_data=SECTION_SUPPORT)],
        ]
    )


def section_prompt(section: SectionConfig) -> str:
    return (
        f"{section.emoji} *{section.title}*\n\n"
        "> Напишите свое сообщение админам"
    )


def format_user_info(user: Any) -> str:
    full_name = " ".join(x for x in [user.first_name, user.last_name] if x) or "Без имени"
    username = f"@{user.username}" if user.username else "без username"
    safe_name = html.escape(full_name)
    safe_username = html.escape(username)
    return f"👤 {safe_name} ({safe_username}) | ID: <code>{user.id}</code>"


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop(USER_SECTION_KEY, None)
    await update.effective_message.reply_text(
        "Привет! Выберите раздел, и я передам ваше сообщение администраторам 👇",
        reply_markup=main_keyboard(),
    )


async def select_section(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query is None or query.data not in SECTIONS:
        return

    await query.answer()
    section_key = query.data
    context.user_data[USER_SECTION_KEY] = section_key
    section = SECTIONS[section_key]

    await query.message.reply_text(
        section_prompt(section),
        parse_mode=ParseMode.MARKDOWN,
    )


async def route_user_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat is None or update.effective_user is None or update.effective_message is None:
        return

    if update.effective_chat.type != "private":
        return

    section_key = context.user_data.get(USER_SECTION_KEY)
    if section_key not in SECTIONS:
        await update.effective_message.reply_text(
            "Сначала выберите раздел 👇",
            reply_markup=main_keyboard(),
        )
        return

    section = SECTIONS[section_key]
    user = update.effective_user
    text_header = (
        f"{section.emoji} <b>{html.escape(section.title)}</b>\n"
        f"{format_user_info(user)}\n"
        "💡 Ответьте на пересланное сообщение, чтобы бот доставил ответ пользователю."
    )

    await context.bot.send_message(
        chat_id=section.chat_id,
        text=text_header,
        parse_mode=ParseMode.HTML,
    )

    copied = await context.bot.copy_message(
        chat_id=section.chat_id,
        from_chat_id=update.effective_chat.id,
        message_id=update.effective_message.message_id,
    )

    route_map: dict[str, dict[str, int]] = context.application.bot_data.setdefault(ROUTE_MAP_KEY, {})
    route_map[f"{section.chat_id}:{copied.message_id}"] = {
        "user_id": user.id,
    }

    await update.effective_message.reply_text("Сообщение отправлено администраторам ✅")


async def route_admin_reply(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    chat = update.effective_chat
    if message is None or chat is None:
        return

    if chat.id not in {COMMUNITY_CHAT_ID, SUPPORT_CHAT_ID}:
        return

    if message.reply_to_message is None:
        return

    route_map: dict[str, dict[str, int]] = context.application.bot_data.get(ROUTE_MAP_KEY, {})
    key = f"{chat.id}:{message.reply_to_message.message_id}"
    route = route_map.get(key)
    if not route:
        return

    user_id = route["user_id"]
    await context.bot.copy_message(
        chat_id=user_id,
        from_chat_id=chat.id,
        message_id=message.message_id,
    )


async def health(_: web.Request) -> web.Response:
    return web.Response(text="ok", status=200)


async def start_http_server(application: Application) -> None:
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=PORT)
    await site.start()

    application.bot_data[HTTP_RUNNER_KEY] = runner
    logger.info("HTTP health server started on port %s", PORT)


async def stop_http_server(application: Application) -> None:
    runner: web.AppRunner | None = application.bot_data.get(HTTP_RUNNER_KEY)
    if runner is not None:
        await runner.cleanup()


async def post_init(application: Application) -> None:
    await start_http_server(application)


async def post_shutdown(application: Application) -> None:
    await stop_http_server(application)


if __name__ == "__main__":
    if not BOT_TOKEN:
        raise RuntimeError("Set BOT_TOKEN environment variable before running the bot.")

    bot_app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    bot_app.add_handler(CommandHandler("start", start))
    bot_app.add_handler(CallbackQueryHandler(select_section, pattern=f"^({SECTION_COMMUNITY}|{SECTION_SUPPORT})$"))
    bot_app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND, route_user_message))
    bot_app.add_handler(MessageHandler(filters.ChatType.GROUPS & ~filters.COMMAND, route_admin_reply))

    logger.info("Bot is starting...")
    bot_app.run_polling(allowed_updates=Update.ALL_TYPES)
