import json
import logging
import os
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

from aiohttp import web
from nacl.exceptions import BadSignatureError
from nacl.signing import VerifyKey
from dotenv import load_dotenv

БАЗОВАЯ_ПАПКА = Path(__file__).resolve().parent
ПУТЬ_ОКРУЖЕНИЯ = БАЗОВАЯ_ПАПКА / ".env"
load_dotenv(dotenv_path=ПУТЬ_ОКРУЖЕНИЯ)

УРОВЕНЬ_ЛОГА = os.getenv("УРОВЕНЬ_ЛОГА", os.getenv("LOG_LEVEL", "INFO")).upper()
ФАЙЛ_ЛОГА = os.getenv("ФАЙЛ_ЛОГА", os.getenv("LOG_FILE", "logs/bot.log"))
ПОРТ = int(os.getenv("PORT", "10000"))
ОТКРЫТЫЙ_КЛЮЧ_ДИСКОРД = os.getenv("DISCORD_PUBLIC_KEY", "").strip()


def настроить_логирование() -> logging.Logger:
    папка_лога = os.path.dirname(ФАЙЛ_ЛОГА)
    if папка_лога:
        os.makedirs(папка_лога, exist_ok=True)

    форматтер = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%d.%m.%Y %H:%M:%S",
    )

    корень = logging.getLogger()
    корень.setLevel(УРОВЕНЬ_ЛОГА)
    корень.handlers.clear()

    поток = logging.StreamHandler()
    поток.setFormatter(форматтер)
    корень.addHandler(поток)

    файл = RotatingFileHandler(ФАЙЛ_ЛОГА, maxBytes=1_000_000, backupCount=3, encoding="utf-8")
    файл.setFormatter(форматтер)
    корень.addHandler(файл)

    return logging.getLogger("бот-дискорд")


логгер = настроить_логирование()


def проверить_подпись(запрос: web.Request, тело: bytes) -> bool:
    подпись = запрос.headers.get("X-Signature-Ed25519", "")
    метка_времени = запрос.headers.get("X-Signature-Timestamp", "")

    if not подпись or not метка_времени or not ОТКРЫТЫЙ_КЛЮЧ_ДИСКОРД:
        return False

    try:
        ключ_проверки = VerifyKey(bytes.fromhex(ОТКРЫТЫЙ_КЛЮЧ_ДИСКОРД))
        ключ_проверки.verify(f"{метка_времени}".encode("utf-8") + тело, bytes.fromhex(подпись))
        return True
    except (BadSignatureError, ValueError):
        return False


async def корневой_обработчик(_: web.Request) -> web.Response:
    return web.Response(text="ok", status=200)


async def обработчик_вебхука(запрос: web.Request) -> web.Response:
    тело = await запрос.read()

    if not проверить_подпись(запрос, тело):
        логгер.warning("Отклонен запрос с неверной подписью Discord.")
        return web.Response(text="неверная подпись", status=401)

    try:
        данные = json.loads(тело.decode("utf-8"))
    except json.JSONDecodeError:
        return web.Response(text="неверный json", status=400)

    тип = данные.get("type")

    if тип == 1:
        return web.json_response({"type": 1})

    if тип == 2:
        имя = "команда"
        поле_данных = данные.get("data")
        if isinstance(поле_данных, dict):
            имя = str(поле_данных.get("name", "команда"))

        ответ = {
            "type": 4,
            "data": {
                "content": f"Принято: /{имя}. Время сервера: {datetime.now(timezone.utc).strftime('%d.%m.%Y %H:%M:%S UTC')}"
            },
        }
        return web.json_response(ответ)

    return web.json_response({"type": 4, "data": {"content": "Событие получено."}})


def создать_приложение() -> web.Application:
    приложение = web.Application()
    приложение.router.add_get("/", корневой_обработчик, allow_head=True)
    приложение.router.add_get("/здоровье", корневой_обработчик, allow_head=True)
    приложение.router.add_post("/вебхук", обработчик_вебхука)
    return приложение


def проверить_настройки() -> None:
    if not ОТКРЫТЫЙ_КЛЮЧ_ДИСКОРД:
        raise RuntimeError("Укажите DISCORD_PUBLIC_KEY в переменных окружения Render.")


if __name__ == "__main__":
    проверить_настройки()
    логгер.info("Запуск Discord вебхука на порту %s", ПОРТ)
    web.run_app(создать_приложение(), host="0.0.0.0", port=ПОРТ)
