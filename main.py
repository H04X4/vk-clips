import html
import re
import importlib
from typing import Dict
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
import config
from vk_worker import VKWorker

from aiogram.client.default import DefaultBotProperties

bot = Bot(
    token=config.TELEGRAM_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher()
worker = VKWorker()

# Хранилище состояния: chat_id -> {"message_id": int, "last_text": str, "last_reply_markup": InlineKeyboardMarkup, "busy": bool, "last_state": dict, "use_auto_delay": bool}
log_status: Dict[int, Dict] = {}
current_top_count = config.TOP_COUNT
current_processing_delay = config.PROCESSING_DELAY
current_use_group = getattr(config, 'USE_GROUP', False)

def reload_config():
    """Перезагружает модуль config.py и возвращает обновленные значения."""
    global current_top_count, current_processing_delay, current_use_group
    importlib.reload(config)
    current_top_count = config.TOP_COUNT
    current_processing_delay = config.PROCESSING_DELAY
    current_use_group = getattr(config, 'USE_GROUP', False)
    return config

def render_progress_bar(current: int, total: int, length: int = 10) -> str:
    """Создаёт визуальный прогресс-бар."""
    if total == 0:
        return "□" * length
    filled = int(current / total * length)
    return "■" * filled + "□" * (length - filled)

def render_log_text(state: Dict) -> str:
    """Форматирует лог с прогресс-барами и последними сообщениями."""
    lines = state.get("messages", [])[-config.MAX_LINES_IN_LOG:]
    total = state.get("total", 0)
    downloaded = state.get("downloaded", 0)
    published = state.get("published", 0)
    failed = state.get("failed", 0)

    stage_map = {
        "init": "Инициализация",
        "fetch_top": "Получение топа",
        "downloaded": "Скачивание",
        "uploaded": "Загрузка",
        "published": "Публикация",
        "done": "Завершено"
    }

    header = (
        f"<b>📊 Статус:</b> {stage_map.get(state.get('stage', 'init'), 'Инициализация')}\n"
        f"<b>Всего видео:</b> {total}\n"
        f"<b>Скачано:</b> {downloaded} {render_progress_bar(downloaded, total)}\n"
        f"<b>Опубликовано:</b> {published} {render_progress_bar(published, total)}\n"
        f"<b>Ошибок:</b> {failed}\n"
        f"{'─'*17}\n"
    )

    body = "\n".join(f"{html.escape(line)}" for line in lines)
    result = f"{header}<pre>{body}</pre>"

    if len(result) > 3900:
        result = "…\n" + result[-3800:]
    return result

def update_config_file(top_count: int, processing_delay: int, use_group: bool = None):
    """Обновляет значения TOP_COUNT, PROCESSING_DELAY и опционально USE_GROUP в config.py."""
    config_path = "config.py"
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            content = f.read()
        
        content = re.sub(r"TOP_COUNT\s*=\s*\d+", f"TOP_COUNT = {top_count}", content)
        content = re.sub(r"PROCESSING_DELAY\s*=\s*\d+", f"PROCESSING_DELAY = {processing_delay}", content)
        if use_group is not None:
            content = re.sub(r"USE_GROUP\s*=\s*(True|False)", f"USE_GROUP = {use_group}", content)
        
        with open(config_path, "w", encoding="utf-8") as f:
            f.write(content)
        reload_config()
    except Exception as e:
        print(f"Ошибка при обновлении config.py: {e}")
        bot.send_message(config.ADMIN_CHAT_ID, f"💥 Ошибка при обновлении config.py: {str(e)}")

def get_settings_menu() -> InlineKeyboardMarkup:
    """Создаёт меню настроек с текущими значениями и кнопками для изменения TOP_COUNT, PROCESSING_DELAY и режима публикации."""
    global current_top_count, current_processing_delay, current_use_group
    mode_text = "Сообщество" if current_use_group else "Профиль"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"Настройки: TOP_COUNT={current_top_count}, PROCESSING_DELAY={current_processing_delay}с", callback_data="noop")],
            [
                InlineKeyboardButton(text="⬅️ -1", callback_data="decrease_top_count"),
                InlineKeyboardButton(text=f"Видео: {current_top_count}", callback_data="noop"),
                InlineKeyboardButton(text="+1 ➡️", callback_data="increase_top_count")
            ],
            [
                InlineKeyboardButton(text="⬅️ -20с", callback_data="decrease_delay"),
                InlineKeyboardButton(text=f"Задержка: {current_processing_delay}с", callback_data="noop"),
                InlineKeyboardButton(text="+20с ➡️", callback_data="increase_delay")
            ],
            [InlineKeyboardButton(text="⏱ Авто (час на TOP_COUNT)", callback_data="auto_delay")],
            [InlineKeyboardButton(text=f"Режим публикации: {mode_text}", callback_data="toggle_publish_mode")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")]
        ]
    )

def get_main_menu() -> InlineKeyboardMarkup:
    """Создаёт главное меню с кнопками 'Запостить ещё' и 'Изменить настройки'."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🔁 Запостить ещё", callback_data="restart"),
                InlineKeyboardButton(text="⚙️ Изменить настройки", callback_data="change_settings")
            ]
        ]
    )

async def update_log_message(chat_id: int, text: str, menu: InlineKeyboardMarkup = None):
    """Создаёт или обновляет сообщение с логом, избегая ошибки 'message is not modified'."""
    kb = menu or get_main_menu()
    entry = log_status.get(chat_id)

    if not entry or "message_id" not in entry:
        msg = await bot.send_message(chat_id, text, reply_markup=kb)
        log_status[chat_id] = {
            "message_id": msg.message_id,
            "last_text": text,
            "last_reply_markup": kb,
            "busy": False,
            "last_state": {},
            "use_auto_delay": False
        }
    else:
        # Проверяем, изменились ли текст или клавиатура
        if text != entry.get("last_text") or str(kb.inline_keyboard) != str(entry.get("last_reply_markup", {}).inline_keyboard):
            try:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=entry["message_id"],
                    text=text,
                    reply_markup=kb
                )
                entry["last_text"] = text
                entry["last_reply_markup"] = kb
            except TelegramBadRequest as e:
                if "message is not modified" in str(e):
                    pass  # Игнорируем ошибку, если сообщение не изменилось
                else:
                    raise  # Пробрасываем другие ошибки
            except Exception as e:
                logger.error(f"Error updating message: {e}")

def progress_callback_factory(chat_id: int):
    """Создаёт функцию для обновления лога."""
    async def _cb(state: Dict):
        text = render_log_text(state)
        log_status[chat_id]["last_state"] = state
        await update_log_message(chat_id, text)
    return _cb

async def run_cycle_for_chat(chat_id: int):
    """Запускает цикл обработки с текущим TOP_COUNT и PROCESSING_DELAY."""
    global current_top_count, current_processing_delay, current_use_group
    entry = log_status.setdefault(chat_id, {})
    if entry.get("busy"):
        await bot.send_message(chat_id, "⏳ Уже выполняется задача, дождитесь завершения.")
        return

    entry["busy"] = True
    try:
        reload_config()
        worker.TOP_COUNT = current_top_count
        worker.group_id = config.GROUP_ID if current_use_group else None
        if entry.get("use_auto_delay", False) and current_top_count > 0:
            worker.PROCESSING_DELAY = 3600 // current_top_count
            await bot.send_message(chat_id, f"⏱ Используется авто-задержка: {worker.PROCESSING_DELAY}с на видео")
        else:
            worker.PROCESSING_DELAY = current_processing_delay
        cb = progress_callback_factory(chat_id)
        await worker.run_cycle(cb)
    except Exception as e:
        await bot.send_message(chat_id, f"💥 Ошибка: {str(e)}")
    finally:
        entry["busy"] = False
        entry["use_auto_delay"] = False

@dp.message(Command("start"))
async def start_cmd(message: Message):
    """Обрабатывает команду /start, показывая меню настроек."""
    if config.ADMIN_CHAT_ID and message.chat.id != config.ADMIN_CHAT_ID:
        await message.answer("🚫 Доступ ограничен.")
        return
    reload_config()
    text = "🤖 Бот готов! Используйте /run или кнопку «Запостить ещё» для запуска цикла.\n⚙️ Настройки:"
    await update_log_message(message.chat.id, text, get_settings_menu())

@dp.message(Command("run"))
async def run_cmd(message: Message):
    """Обрабатывает команду /run, запуская цикл обработки."""
    if config.ADMIN_CHAT_ID and message.chat.id != config.ADMIN_CHAT_ID:
        await message.answer("🚫 Доступ ограничен.")
        return
    await run_cycle_for_chat(message.chat.id)

@dp.callback_query(F.data == "restart")
async def restart_btn(query: CallbackQuery):
    """Обрабатывает кнопку 'Запостить ещё'."""
    if config.ADMIN_CHAT_ID and query.message.chat.id != config.ADMIN_CHAT_ID:
        return
    await query.answer("🔁 Запускаю новый цикл...")
    await run_cycle_for_chat(query.message.chat.id)

@dp.callback_query(F.data == "change_settings")
async def change_settings_btn(query: CallbackQuery):
    """Обрабатывает кнопку 'Изменить настройки'."""
    if config.ADMIN_CHAT_ID and query.message.chat.id != config.ADMIN_CHAT_ID:
        return
    reload_config()
    await query.answer("⚙️ Открываю настройки")
    text = render_log_text(log_status.get(query.message.chat.id, {}).get("last_state", {}))
    await bot.edit_message_text(
        chat_id=query.message.chat.id,
        message_id=query.message.message_id,
        text=text or "⚙️ Настройки",
        reply_markup=get_settings_menu()
    )

@dp.callback_query(F.data == "decrease_top_count")
async def decrease_top_count_btn(query: CallbackQuery):
    """Уменьшает TOP_COUNT на 1 и обновляет config.py."""
    global current_top_count
    if config.ADMIN_CHAT_ID and query.message.chat.id != config.ADMIN_CHAT_ID:
        return
    if current_top_count > 1:
        current_top_count -= 1
        update_config_file(current_top_count, current_processing_delay)
        await query.answer(f"TOP_COUNT уменьшен до {current_top_count}")
    else:
        await query.answer("TOP_COUNT не может быть меньше 1")
    text = render_log_text(log_status.get(query.message.chat.id, {}).get("last_state", {}))
    await bot.edit_message_text(
        chat_id=query.message.chat.id,
        message_id=query.message.message_id,
        text=text or "⚙️ Настройки",
        reply_markup=get_settings_menu()
    )

@dp.callback_query(F.data == "increase_top_count")
async def increase_top_count_btn(query: CallbackQuery):
    """Увеличивает TOP_COUNT на 1 и обновляет config.py."""
    global current_top_count
    if config.ADMIN_CHAT_ID and query.message.chat.id != config.ADMIN_CHAT_ID:
        return
    current_top_count += 1
    update_config_file(current_top_count, current_processing_delay)
    await query.answer(f"TOP_COUNT увеличен до {current_top_count}")
    text = render_log_text(log_status.get(query.message.chat.id, {}).get("last_state", {}))
    await bot.edit_message_text(
        chat_id=query.message.chat.id,
        message_id=query.message.message_id,
        text=text or "⚙️ Настройки",
        reply_markup=get_settings_menu()
    )

@dp.callback_query(F.data == "decrease_delay")
async def decrease_delay_btn(query: CallbackQuery):
    """Уменьшает PROCESSING_DELAY на 20 секунд и обновляет config.py."""
    global current_processing_delay
    if config.ADMIN_CHAT_ID and query.message.chat.id != config.ADMIN_CHAT_ID:
        return
    if current_processing_delay >= 20:
        current_processing_delay -= 20
        update_config_file(current_top_count, current_processing_delay)
        await query.answer(f"PROCESSING_DELAY уменьшен до {current_processing_delay}с")
    else:
        await query.answer("PROCESSING_DELAY не может быть меньше 0")
    text = render_log_text(log_status.get(query.message.chat.id, {}).get("last_state", {}))
    await bot.edit_message_text(
        chat_id=query.message.chat.id,
        message_id=query.message.message_id,
        text=text or "⚙️ Настройки",
        reply_markup=get_settings_menu()
    )

@dp.callback_query(F.data == "increase_delay")
async def increase_delay_btn(query: CallbackQuery):
    """Увеличивает PROCESSING_DELAY на 20 секунд и обновляет config.py."""
    global current_processing_delay
    if config.ADMIN_CHAT_ID and query.message.chat.id != config.ADMIN_CHAT_ID:
        return
    current_processing_delay += 20
    update_config_file(current_top_count, current_processing_delay)
    await query.answer(f"PROCESSING_DELAY увеличен до {current_processing_delay}с")
    text = render_log_text(log_status.get(query.message.chat.id, {}).get("last_state", {}))
    await bot.edit_message_text(
        chat_id=query.message.chat.id,
        message_id=query.message.message_id,
        text=text or "⚙️ Настройки",
        reply_markup=get_settings_menu()
    )

@dp.callback_query(F.data == "auto_delay")
async def auto_delay_btn(query: CallbackQuery):
    """Устанавливает флаг для автоматической задержки (1 час / TOP_COUNT)."""
    if config.ADMIN_CHAT_ID and query.message.chat.id != config.ADMIN_CHAT_ID:
        return
    if current_top_count == 0:
        await query.answer("TOP_COUNT должен быть больше 0 для авто-задержки")
        return
    log_status[query.message.chat.id]["use_auto_delay"] = True
    auto_delay = 3600 // current_top_count
    await query.answer(f"Установлена авто-задержка: {auto_delay}с на видео")
    text = render_log_text(log_status.get(query.message.chat.id, {}).get("last_state", {}))
    await bot.edit_message_text(
        chat_id=query.message.chat.id,
        message_id=query.message.message_id,
        text=text or "⚙️ Настройки",
        reply_markup=get_settings_menu()
    )

@dp.callback_query(F.data == "toggle_publish_mode")
async def toggle_publish_mode_btn(query: CallbackQuery):
    """Переключает режим публикации между 'Профиль' и 'Сообщество'."""
    global current_use_group
    if config.ADMIN_CHAT_ID and query.message.chat.id != config.ADMIN_CHAT_ID:
        return
    current_use_group = not current_use_group
    update_config_file(current_top_count, current_processing_delay, current_use_group)
    mode_text = "Сообщество" if current_use_group else "Профиль"
    await query.answer(f"Режим публикации изменен на: {mode_text}")
    text = render_log_text(log_status.get(query.message.chat.id, {}).get("last_state", {}))
    await bot.edit_message_text(
        chat_id=query.message.chat.id,
        message_id=query.message.message_id,
        text=text or "⚙️ Настройки",
        reply_markup=get_settings_menu()
    )

@dp.callback_query(F.data == "back_to_main")
async def back_to_main_btn(query: CallbackQuery):
    """Возвращает к главному меню."""
    if config.ADMIN_CHAT_ID and query.message.chat.id != config.ADMIN_CHAT_ID:
        return
    await query.answer("🔙 Возвращаюсь к главному меню")
    text = render_log_text(log_status.get(query.message.chat.id, {}).get("last_state", {}))
    await bot.edit_message_text(
        chat_id=query.message.chat.id,
        message_id=query.message.message_id,
        text=text or "📊 Статус не обновлён",
        reply_markup=get_main_menu()
    )

@dp.callback_query(F.data == "noop")
async def noop_btn(query: CallbackQuery):
    """Пустой callback для неинтерактивных кнопок."""
    await query.answer()

async def main():
    """Запускает бота."""
    try:
        await dp.start_polling(bot, allowed_updates=["message", "callback_query"])
    except Exception as e:
        logger.error(f"Error in main loop: {e}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
