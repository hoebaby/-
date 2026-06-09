import os
import asyncio
import random
import logging
import re
from datetime import datetime
from io import BytesIO
from typing import Dict, Tuple

from dotenv import load_dotenv
from telethon import TelegramClient, errors
from telethon.errors import (
    SessionPasswordNeededError, PhoneCodeInvalidError,
    ChannelPrivateError, ChatAdminRequiredError, InviteHashExpiredError
)
from telethon.sessions import StringSession
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
import aiosqlite

from aiogram import Bot, Dispatcher, types, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardButton, InlineKeyboardMarkup
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")

if not BOT_TOKEN or not API_ID or not API_HASH:
    raise ValueError("Не заполнен .env файл!")

DB_PATH = "data.db"
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

active_tasks: Dict[Tuple[int, int], asyncio.Task] = {}
connected_clients: Dict[int, TelegramClient] = {}
group_cache: Dict[Tuple[int, int], dict] = {}
history_queue: asyncio.Queue = asyncio.Queue()
db_lock = asyncio.Lock()

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA foreign_keys = ON")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                phone TEXT UNIQUE,
                session_string TEXT,
                first_name TEXT,
                last_name TEXT,
                username TEXT,
                added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        try:
            await db.execute("ALTER TABLE accounts ADD COLUMN user_id BIGINT NOT NULL DEFAULT 0")
        except:
            pass
        try:
            await db.execute("CREATE INDEX IF NOT EXISTS idx_accounts_user ON accounts(user_id)")
        except:
            pass

        await db.execute("""
            CREATE TABLE IF NOT EXISTS groups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id INTEGER NOT NULL,
                username TEXT,
                title TEXT,
                mailing_text TEXT DEFAULT '',
                interval_seconds INTEGER DEFAULT 60,
                is_active INTEGER DEFAULT 0,
                last_sent TIMESTAMP,
                guarantor_text TEXT DEFAULT '',
                FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE CASCADE
            )
        """)
        try:
            await db.execute("ALTER TABLE groups ADD COLUMN guarantor_text TEXT DEFAULT ''")
        except:
            pass

        await db.execute("""
            CREATE TABLE IF NOT EXISTS history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id INTEGER,
                group_username TEXT,
                group_title TEXT,
                message_text TEXT,
                sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE SET NULL
            )
        """)
        try:
            await db.execute("ALTER TABLE history ADD COLUMN user_id BIGINT NOT NULL DEFAULT 0")
        except:
            pass
        try:
            await db.execute("CREATE INDEX IF NOT EXISTS idx_history_user ON history(user_id)")
        except:
            pass

        await db.execute("""
            CREATE TABLE IF NOT EXISTS templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                text TEXT
            )
        """)
        try:
            await db.execute("ALTER TABLE templates ADD COLUMN user_id BIGINT NOT NULL DEFAULT 0")
        except:
            pass
        try:
            await db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_templates_user_name ON templates(user_id, name)")
        except:
            pass

        await db.commit()

async def history_writer():
    while True:
        batch = []
        try:
            record = await asyncio.wait_for(history_queue.get(), timeout=30)
            batch.append(record)
        except asyncio.TimeoutError:
            continue
        while not history_queue.empty():
            batch.append(history_queue.get_nowait())
        if batch:
            async with db_lock:
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.executemany(
                        "INSERT INTO history (user_id, account_id, group_username, group_title, message_text) VALUES (?,?,?,?,?)",
                        batch
                    )
                    await db.commit()

async def get_client(account_id: int) -> TelegramClient:
    if account_id in connected_clients:
        client = connected_clients[account_id]
        if client.is_connected():
            return client
        else:
            await client.connect()
            return client

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT session_string FROM accounts WHERE id = ?", (account_id,)) as cursor:
            row = await cursor.fetchone()
            if not row:
                raise ValueError("Аккаунт не найден")
            session_str = row[0]

    client = TelegramClient(StringSession(session_str), API_ID, API_HASH)
    await client.connect()
    connected_clients[account_id] = client
    return client

async def disconnect_client_after_delay(account_id: int, delay: int = 60):
    await asyncio.sleep(delay)
    for (acc_id, _), task in active_tasks.items():
        if acc_id == account_id and not task.done():
            return
    if account_id in connected_clients:
        client = connected_clients.pop(account_id)
        await client.disconnect()
        logger.info(f"Клиент аккаунта {account_id} отключен")

def main_reply_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📱 Мои аккаунты")],
            [KeyboardButton(text="👤 Добавить аккаунт"), KeyboardButton(text="➕ Добавить группу")],
            [KeyboardButton(text="📊 История рассылки"), KeyboardButton(text="📋 Шаблоны")],
            [KeyboardButton(text="📖 Инструкция"), KeyboardButton(text="🆘 Поддержка")],
        ],
        resize_keyboard=True
    )

class AddAccount(StatesGroup):
    phone = State()
    code = State()
    password = State()

class DeleteAccount(StatesGroup):
    confirm_phone = State()

class AddGroupManual(StatesGroup):
    choose_account = State()
    input = State()

class MassMailing(StatesGroup):
    choose_mode = State()
    choose_text_source = State()
    text = State()
    interval = State()
    random_min = State()
    random_max = State()

class SetGroupText(StatesGroup):
    choose_text_source = State()
    text = State()
    interval = State()

class SetGuarantor(StatesGroup):
    text = State()

class SubscribeGroups(StatesGroup):
    wait_for_links_or_file = State()

class TemplateCreate(StatesGroup):
    name = State()
    text = State()

async def universal_mailing_loop(account_id: int, group_id: int, interval_getter, text_override=None):
    client = await get_client(account_id)
    key = (account_id, group_id)
    if key not in group_cache:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT username, title, mailing_text, guarantor_text FROM groups WHERE id = ?", (group_id,)) as cursor:
                row = await cursor.fetchone()
        if not row:
            return
        group_cache[key] = {
            'username': row[0],
            'title': row[1],
            'mailing_text': row[2],
            'guarantor': row[3]
        }
    info = group_cache[key]
    username = info['username']
    title = info['title']
    guarantor = info['guarantor']
    base_text = text_override if text_override else info['mailing_text']
    if not base_text:
        return

    def build_message():
        msg = base_text
        if guarantor and not msg.rstrip().endswith(guarantor):
            msg = msg.rstrip() + "\n" + guarantor
        return msg

    msg = build_message()
    target = username
    try:
        await client.send_message(target, msg)
    except Exception as e:
        logger.error(f"Ошибка отправки в {target}: {e}")
        try:
            await client.send_message(int(target), msg)
        except Exception as e2:
            logger.error(f"Не удалось отправить: {e2}")
            return

    now = datetime.now().isoformat()
    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE groups SET last_sent = ?, is_active = 1 WHERE id = ?", (now, group_id))
            await db.commit()
    await history_queue.put((0, account_id, username, title, msg))

    try:
        while True:
            interval = interval_getter()
            await asyncio.sleep(interval)
            msg = build_message()
            try:
                await client.send_message(target, msg)
                now = datetime.now().isoformat()
                async with db_lock:
                    async with aiosqlite.connect(DB_PATH) as db:
                        await db.execute("UPDATE groups SET last_sent = ? WHERE id = ?", (now, group_id))
                        await db.commit()
                await history_queue.put((0, account_id, username, title, msg))
            except Exception as e:
                logger.error(f"Ошибка отправки в {username}: {e}")
    except asyncio.CancelledError:
        logger.info(f"Рассылка в {username} остановлена")
    finally:
        async with db_lock:
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("UPDATE groups SET is_active = 0 WHERE id = ?", (group_id,))
                await db.commit()
        active_tasks.pop(key, None)
        group_cache.pop(key, None)
        asyncio.create_task(disconnect_client_after_delay(account_id))

async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    welcome = (
        "🌸 Привет, солнышко! 🌸\n\n"
        "Я — твой верный помощник для автоматических рассылок в Telegram. 🤖\n"
        "Я помогу тебе отправлять сообщения в группы и каналы, красиво и без забот! ✨\n\n"
        "💌 Вот что я умею:\n"
        "👤 Добавлять и хранить твои аккаунты\n"
        "📋 Сканировать и сохранять группы\n"
        "📨 Отправлять сообщения с любым интервалом\n"
        "📝 Использовать готовые шаблоны\n"
        "🔒 Добавлять гаранта к сообщениям автоматически\n\n"
        "Скорее выбирай действие в меню снизу! ⬇️"
    )
    await message.answer(welcome, reply_markup=main_reply_kb())

async def callback_main_menu(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.delete()
    await callback.message.answer("Главное меню", reply_markup=main_reply_kb())

async def show_instruction(message: types.Message, state: FSMContext):
    text = (
        "📖 <b>Подробная инструкция</b> 📖\n\n"
        "<b>1. 👤 Добавление аккаунта</b>\n"
        "Нажмите «👤 Добавить аккаунт» или на кнопку в меню.\n"
        "• Введите номер телефона, начиная с «+» (например, +79001234567)\n"
        "• Дождитесь кода подтверждения в Telegram\n"
        "• Введите код\n"
        "• Если подключена двухфакторная аутентификация, введите пароль\n"
        "⚠️ <i>Используйте только свои аккаунты! Не нарушайте правила Telegram.</i>\n\n"
        "<b>2. ➕ Добавление групп</b>\n"
        "Есть несколько способов:\n"
        "• <b>Вручную:</b> нажмите «➕ Добавить группу» → выберите аккаунт → отправьте @username, ссылку (t.me/...) или загрузите .txt файл со списком\n"
        "• <b>Сканирование:</b> в карточке аккаунта → «➕ Добавить все группы» — бот найдёт все ваши группы и каналы\n"
        "• <b>Подписка:</b> в карточке аккаунта → «🔗 Подписаться на группы» → введите ссылки или загрузите .txt файл. Бот вступит в них с задержкой 10 сек.\n"
        "⚠️ <i>Не более 20-30 вступлений в день для новых аккаунтов!</i>\n\n"
        "<b>3. 📝 Настройка текста и интервала</b>\n"
        "В списке групп выберите нужную → «📝 Текст и интервал» → выберите источник (вручную или шаблон) → введите текст или выберите заготовку → укажите интервал в секундах.\n"
        "⚠️ <i>Не вписывайте гаранта в текст! Используйте отдельную кнопку «🔒 Гарант».</i>\n\n"
        "<b>4. 🚀 Запуск рассылки</b>\n"
        "• Для одной группы: в меню группы → «▶️ Начать»\n"
        "• Для всех групп аккаунта: в карточке аккаунта → «🚀 Начать рассылку во все»\n"
        "⏹️ Остановить рассылку можно в любой момент там же.\n\n"
        "<b>5. 📋 Шаблоны</b>\n"
        "Нажмите «📋 Шаблоны» в главном меню. Создавайте до 50 заготовок. Они доступны при настройке текста.\n\n"
        "<b>6. 🔒 Гарант</b>\n"
        "В меню группы → «🔒 Гарант» → укажите @username или любой текст. Он будет добавляться в конец каждого сообщения. Для удаления отправьте «-».\n\n"
        "<b>7. 📊 История</b>\n"
        "«📊 История рассылки» покажет последние 10 отправленных сообщений.\n\n"
        "Если что-то идёт не так — обращайтесь в поддержку: @hoebredim"
    )
    await message.answer(text, parse_mode="HTML")

async def show_support(message: types.Message, state: FSMContext):
    await message.answer(
        "🆘 <b>Поддержка</b>\n\n"
        "Разработчик: @hoebredim\n"
        "Пишите, если возникли вопросы или пожелания!",
        parse_mode="HTML"
    )

async def check_accounts_limit(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM accounts WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            return row[0] < 1000

async def add_account_start(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    if not await check_accounts_limit(user_id):
        await callback.answer("Лимит аккаунтов (1000).", show_alert=True)
        return
    await state.set_state(AddAccount.phone)
    await callback.message.edit_text("📲 Введите номер телефона (+79001234567):",
                                     reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                         [InlineKeyboardButton(text="🔙 Отмена", callback_data="main_menu")]]))

async def add_account_text(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not await check_accounts_limit(user_id):
        await message.answer("❌ Лимит аккаунтов (1000).")
        return
    await state.set_state(AddAccount.phone)
    await message.answer("📲 Введите номер телефона (+79001234567):",
                         reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                             [InlineKeyboardButton(text="🔙 Отмена", callback_data="main_menu")]]))

async def add_account_phone(message: types.Message, state: FSMContext):
    phone = message.text.strip()
    if not phone.startswith("+"):
        await message.answer("❌ Номер должен начинаться с '+'")
        return
    client = TelegramClient(StringSession(), API_ID, API_HASH)
    await client.connect()
    try:
        sent_code = await client.send_code_request(phone)
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")
        await client.disconnect()
        await state.clear()
        return
    await state.update_data(phone=phone, client=client, sent_code=sent_code)
    await state.set_state(AddAccount.code)
    await message.answer("✉️ Введите код подтверждения:",
                         reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                             [InlineKeyboardButton(text="🔙 Отмена", callback_data="main_menu")]]))

async def add_account_code(message: types.Message, state: FSMContext):
    code = message.text.strip()
    data = await state.get_data()
    client: TelegramClient = data["client"]
    phone = data["phone"]
    try:
        await client.sign_in(phone, code)
    except SessionPasswordNeededError:
        await state.set_state(AddAccount.password)
        await message.answer("🔐 Введите пароль 2FA:",
                             reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                 [InlineKeyboardButton(text="🔙 Отмена", callback_data="main_menu")]]))
        return
    except PhoneCodeInvalidError:
        await message.answer("❌ Неверный код")
        return
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")
        await client.disconnect()
        await state.clear()
        return
    await save_session(message, state, client)

async def add_account_password(message: types.Message, state: FSMContext):
    password = message.text.strip()
    data = await state.get_data()
    client: TelegramClient = data["client"]
    try:
        await client.sign_in(password=password)
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")
        return
    await save_session(message, state, client)

async def save_session(message: types.Message, state: FSMContext, client: TelegramClient):
    data = await state.get_data()
    phone = data["phone"]
    session_str = client.session.save()
    me = await client.get_me()
    first = me.first_name or ""
    last = me.last_name or ""
    username = me.username or ""
    user_id = message.from_user.id
    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT OR REPLACE INTO accounts (user_id, phone, session_string, first_name, last_name, username) VALUES (?,?,?,?,?,?)",
                (user_id, phone, session_str, first, last, username)
            )
            await db.commit()
    await client.disconnect()
    await state.clear()
    await message.answer("✅ Аккаунт добавлен!", reply_markup=main_reply_kb())

async def my_accounts(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    await show_accounts_list(message, state, user_id)

async def my_accounts_callback(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    await show_accounts_list(callback, state, user_id, edit=True)

async def show_accounts_list(message_or_query, state: FSMContext, user_id, edit=False):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT id, phone, first_name, last_name, username FROM accounts WHERE user_id = ?", (user_id,)) as cursor:
            accounts = await cursor.fetchall()
    if not accounts:
        text = "Нет аккаунтов"
        kb = main_reply_kb() if not edit else back_to_main_menu_kb()
    else:
        text = "📱 Ваши аккаунты:"
        builder = InlineKeyboardBuilder()
        for acc in accounts:
            builder.row(InlineKeyboardButton(text=f"📱 {acc[1]}", callback_data=f"account_{acc[0]}"))
        builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu"))
        kb = builder.as_markup()

    if isinstance(message_or_query, types.CallbackQuery):
        await message_or_query.message.edit_text(text, reply_markup=kb)
    else:
        await message_or_query.answer(text, reply_markup=kb)

def back_to_main_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Главное меню", callback_data="main_menu")]])

async def account_info(callback: types.CallbackQuery, state: FSMContext):
    account_id = int(callback.data.split("_")[1])
    user_id = callback.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT phone, first_name, last_name, username FROM accounts WHERE id = ? AND user_id = ?",
            (account_id, user_id)
        ) as cursor:
            acc = await cursor.fetchone()
    if not acc:
        await callback.message.edit_text("Аккаунт не найден", reply_markup=back_to_main_menu_kb())
        return
    phone, first, last, uname = acc[0], acc[1], acc[2], acc[3]
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM groups WHERE account_id = ?", (account_id,)) as cursor:
            group_count = (await cursor.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM groups WHERE account_id = ? AND is_active = 1", (account_id,)) as cursor:
            active_count = (await cursor.fetchone())[0]

    text = (f"👤 *Аккаунт:* {first} {last} (@{uname or 'нет'})\n"
            f"📞 Телефон: {phone}\n"
            f"👥 Групп: {group_count}\n"
            f"📤 Активных: {active_count}\n")
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="📋 Список групп", callback_data=f"groups_{account_id}"))
    builder.row(InlineKeyboardButton(text="➕ Добавить все группы", callback_data=f"add_all_groups_{account_id}"))
    builder.row(InlineKeyboardButton(text="🔗 Подписаться на группы", callback_data=f"subscribe_menu_{account_id}"))
    builder.row(InlineKeyboardButton(text="🚀 Начать рассылку во все", callback_data=f"mass_start_{account_id}"))
    builder.row(InlineKeyboardButton(text="⏹️ Остановить общую", callback_data=f"mass_stop_{account_id}"))
    builder.row(InlineKeyboardButton(text="❌ Удалить все группы", callback_data=f"delete_all_groups_{account_id}"))
    builder.row(InlineKeyboardButton(text="❌ Удалить аккаунт", callback_data=f"delete_account_{account_id}"))
    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data="my_accounts"))
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="Markdown")

async def delete_all_groups(callback: types.CallbackQuery, state: FSMContext):
    account_id = int(callback.data.split("_")[3])
    for (aid, gid), task in list(active_tasks.items()):
        if aid == account_id:
            task.cancel()
    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("DELETE FROM groups WHERE account_id = ?", (account_id,))
            await db.commit()
    await callback.message.edit_text("✅ Все группы удалены",
                                     reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                         [InlineKeyboardButton(text="🔙 К аккаунту", callback_data=f"account_{account_id}")]]))

async def delete_account_start(callback: types.CallbackQuery, state: FSMContext):
    account_id = int(callback.data.split("_")[2])
    user_id = callback.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT phone FROM accounts WHERE id = ? AND user_id = ?", (account_id, user_id)) as cursor:
            acc = await cursor.fetchone()
    if not acc:
        await callback.message.edit_text("Аккаунт не найден")
        return
    phone = acc[0]
    await state.update_data(delete_account_id=account_id, delete_phone=phone)
    await state.set_state(DeleteAccount.confirm_phone)
    await callback.message.edit_text(f"⚠️ Для удаления аккаунта {phone} введите номер телефона:",
                                     reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                         [InlineKeyboardButton(text="🔙 Отмена", callback_data="main_menu")]]))

async def delete_account_confirm(message: types.Message, state: FSMContext):
    phone_input = message.text.strip()
    data = await state.get_data()
    if phone_input != data["delete_phone"]:
        await message.answer("❌ Номер не совпадает. Отмена.")
        await state.clear()
        return
    account_id = data["delete_account_id"]
    for (aid, gid), task in list(active_tasks.items()):
        if aid == account_id:
            task.cancel()
    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("DELETE FROM accounts WHERE id = ?", (account_id,))
            await db.commit()
    if account_id in connected_clients:
        cl = connected_clients.pop(account_id)
        await cl.disconnect()
    await state.clear()
    await message.answer("✅ Аккаунт удалён", reply_markup=main_reply_kb())

async def list_groups(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    account_id = int(parts[1])
    page = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
    await show_groups_page(callback.message, state, account_id, page)

async def show_groups_page(message, state, account_id, page=0):
    per_page = 5
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM groups WHERE account_id = ?", (account_id,)) as cursor:
            total = (await cursor.fetchone())[0]
        async with db.execute("SELECT id, username, title, is_active FROM groups WHERE account_id = ? ORDER BY id LIMIT ? OFFSET ?",
                              (account_id, per_page, page * per_page)) as cursor:
            groups = await cursor.fetchall()
    if total == 0:
        await message.edit_text("Нет групп", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data=f"account_{account_id}")]]))
        return

    builder = InlineKeyboardBuilder()
    for g in groups:
        status = "🟢" if g[3] else "🔴"
        if g[1] and (g[1].startswith('@') or g[1].startswith('https://')):
            display_name = g[1]
        else:
            display_name = g[2] if g[2] else g[1]
        if not display_name:
            display_name = "Без названия"
        builder.row(InlineKeyboardButton(text=f"{status} {display_name[:30]}", callback_data=f"group_{account_id}_{g[0]}"))

    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(text="⬅️", callback_data=f"groups_{account_id}_{page-1}"))
    if (page + 1) * per_page < total:
        nav_buttons.append(InlineKeyboardButton(text="➡️", callback_data=f"groups_{account_id}_{page+1}"))
    if nav_buttons:
        builder.row(*nav_buttons)
    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data=f"account_{account_id}"))
    await message.edit_text(f"Список групп (страница {page+1} из { (total-1)//per_page + 1 }):", reply_markup=builder.as_markup())

async def group_detail(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    if len(parts) < 3:
        return
    account_id = int(parts[1])
    group_id = int(parts[2])
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT * FROM groups WHERE id = ?", (group_id,)) as cursor:
            g = await cursor.fetchone()
    if not g:
        await callback.message.edit_text("Группа не найдена",
                                         reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                             [InlineKeyboardButton(text="🔙 Назад", callback_data=f"groups_{account_id}_0")]]))
        return
    status = "🟢 Активна" if g[6] else "🔴 Неактивна"
    guarantor_info = f"\n🔒 Гарант: {g[8]}" if g[8] else ""
    username_display = g[2] if g[2] else 'нет'
    if username_display.startswith('-') or username_display.isdigit():
        username_display = g[3] if g[3] else 'нет'
    text = (f"📌 *Группа:* {g[3]}\n"
            f"🆔 Username: {username_display}\n"
            f"💬 Текст: {g[4] or 'не задан'}\n"
            f"⏱ Интервал: {g[5]} сек\n"
            f"📊 Статус: {status}{guarantor_info}\n")
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="📝 Текст и интервал", callback_data=f"settext_{account_id}_{group_id}"))
    builder.row(InlineKeyboardButton(text="🔒 Гарант", callback_data=f"guarantor_{account_id}_{group_id}"))
    builder.row(InlineKeyboardButton(text="▶️ Начать", callback_data=f"start_group_{account_id}_{group_id}"))
    builder.row(InlineKeyboardButton(text="⏹️ Остановить", callback_data=f"stop_group_{account_id}_{group_id}"))
    builder.row(InlineKeyboardButton(text="❌ Удалить группу", callback_data=f"delete_group_{account_id}_{group_id}"))
    builder.row(InlineKeyboardButton(text="🔙 К списку", callback_data=f"groups_{account_id}_0"))
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="Markdown")

async def delete_group(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    if len(parts) < 4 or parts[0] != "delete" or parts[1] != "group":
        return
    account_id = int(parts[2])
    group_id = int(parts[3])
    task = active_tasks.pop((account_id, group_id), None)
    if task:
        task.cancel()
    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("DELETE FROM groups WHERE id = ?", (group_id,))
            await db.commit()
    await callback.message.edit_text("✅ Группа удалена",
                                     reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                         [InlineKeyboardButton(text="🔙 К списку", callback_data=f"groups_{account_id}_0")]]))

async def guarantor_menu(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    if len(parts) < 3:
        return
    account_id = int(parts[1])
    group_id = int(parts[2])
    await state.update_data(guarantor_account=account_id, guarantor_group=group_id)
    await state.set_state(SetGuarantor.text)
    await callback.message.edit_text(
        "🔒 Введите текст гаранта (например, @username). Для удаления отправьте '-'.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data=f"group_{account_id}_{group_id}")]])
    )

async def guarantor_text_input(message: types.Message, state: FSMContext):
    data = await state.get_data()
    account_id = data["guarantor_account"]
    group_id = data["guarantor_group"]
    text = message.text.strip()
    if text == "-":
        text = ""
        confirm = "✅ Гарант удалён"
    else:
        confirm = "✅ Гарант сохранён"
    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE groups SET guarantor_text = ? WHERE id = ?", (text, group_id))
            await db.commit()
    await state.clear()
    await message.answer(confirm,
                         reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                             [InlineKeyboardButton(text="🔙 К группе", callback_data=f"group_{account_id}_{group_id}")]]))

async def add_all_groups(callback: types.CallbackQuery, state: FSMContext):
    account_id = int(callback.data.split("_")[3])
    await callback.message.edit_text("⏳ Сканирую группы аккаунта...")
    client = await get_client(account_id)
    try:
        dialogs = await client.get_dialogs()
        count = 0
        async with db_lock:
            async with aiosqlite.connect(DB_PATH) as db:
                for dialog in dialogs:
                    if dialog.is_group or dialog.is_channel:
                        entity = dialog.entity
                        username = getattr(entity, 'username', None)
                        title = dialog.name
                        if username:
                            await db.execute("INSERT OR IGNORE INTO groups (account_id, username, title) VALUES (?,?,?)",
                                             (account_id, username, title))
                            count += 1
                        else:
                            chat_id = str(entity.id)
                            async with db.execute("SELECT id FROM groups WHERE account_id = ? AND username = ?", (account_id, chat_id)) as cur:
                                existing = await cur.fetchone()
                            if not existing:
                                await db.execute("INSERT INTO groups (account_id, username, title) VALUES (?,?,?)",
                                                 (account_id, chat_id, title))
                                count += 1
                await db.commit()
        await callback.message.edit_text(f"✅ Добавлено групп: {count}",
                                         reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                             [InlineKeyboardButton(text="🔙 Назад", callback_data=f"account_{account_id}")]]))
    except Exception as e:
        await callback.message.edit_text(f"❌ Ошибка: {e}",
                                         reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                             [InlineKeyboardButton(text="🔙 Назад", callback_data=f"account_{account_id}")]]))

async def add_group_manual_start(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT id, phone FROM accounts WHERE user_id = ?", (user_id,)) as cursor:
            accounts = await cursor.fetchall()
    if not accounts:
        await message.answer("Нет аккаунтов. Сначала добавьте аккаунт.", reply_markup=main_reply_kb())
        return
    builder = InlineKeyboardBuilder()
    for acc in accounts:
        builder.row(InlineKeyboardButton(text=f"📱 {acc[1]}", callback_data=f"manual_group_acc_{acc[0]}"))
    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu"))
    await message.answer("Выберите аккаунт:", reply_markup=builder.as_markup())

async def manual_group_choose_account(callback: types.CallbackQuery, state: FSMContext):
    account_id = int(callback.data.split("_")[3])
    await state.update_data(manual_group_account=account_id)
    await state.set_state(AddGroupManual.input)
    await callback.message.edit_text(
        "📎 Отправьте @username, ссылку (t.me/...) или .txt файл.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Отмена", callback_data="main_menu")]])
    )

async def manual_group_input(message: types.Message, state: FSMContext):
    data = await state.get_data()
    account_id = data["manual_group_account"]
    client = await get_client(account_id)

    if message.document:
        if not message.document.file_name.endswith('.txt'):
            await message.answer("❌ Текстовый файл .txt")
            return
        file = await message.bot.get_file(message.document.file_id)
        downloaded: BytesIO = await message.bot.download_file(file.file_path)
        content = downloaded.getvalue().decode('utf-8')
        lines = [line.strip() for line in content.splitlines() if line.strip()]
    else:
        lines = [message.text.strip()]

    if not lines:
        await message.answer("❌ Пустой запрос.")
        return

    added = []
    errors = []
    inserts = []
    for entry in lines:
        entry = entry.strip()
        if not entry:
            continue
        try:
            if entry.startswith('@'):
                try:
                    entity = await client.get_entity(entry)
                    if not hasattr(entity, 'broadcast') and not hasattr(entity, 'megagroup'):
                        errors.append(f"{entry}: это не группа/канал")
                        continue
                except Exception as e:
                    errors.append(f"{entry}: {str(e)}")
                    continue
            elif entry.startswith('https://t.me/+') or entry.startswith('t.me/+'):
                try:
                    invite_hash = entry.split('/')[-1].split('?')[0]
                    await client(ImportChatInviteRequest(invite_hash))
                    entity = await client.get_entity(entry)
                except InviteHashExpiredError:
                    errors.append(f"{entry}: ссылка истекла")
                    continue
                except Exception as e:
                    errors.append(f"{entry}: {str(e)}")
                    continue
            else:
                try:
                    entity = await client.get_entity(entry)
                except Exception as e:
                    errors.append(f"{entry}: {str(e)}")
                    continue

            username = getattr(entity, 'username', None)
            title = getattr(entity, 'title', None) or entry
            if username:
                inserts.append((account_id, username, title))
                added.append(f"@{username}")
            else:
                chat_id = str(entity.id)
                inserts.append((account_id, chat_id, title))
                added.append(title)
        except Exception as e:
            errors.append(f"{entry}: {str(e)}")

    if inserts:
        async with db_lock:
            async with aiosqlite.connect(DB_PATH) as db:
                await db.executemany(
                    "INSERT OR IGNORE INTO groups (account_id, username, title) VALUES (?,?,?)",
                    inserts
                )
                await db.commit()

    await state.clear()
    report = ""
    if added:
        report += f"✅ Добавлено групп: {len(added)}\n"
    if errors:
        report += f"❌ Ошибки:\n" + "\n".join(errors)
    if not report:
        report = "Не удалось добавить ни одной группы."
    await message.answer(report, reply_markup=main_reply_kb())

async def subscribe_menu(callback: types.CallbackQuery, state: FSMContext):
    account_id = int(callback.data.split("_")[2])
    await state.update_data(subscribe_account=account_id)
    await state.set_state(SubscribeGroups.wait_for_links_or_file)
    await callback.message.edit_text(
        "🔗 Отправьте список ссылок или @username'ов, либо .txt файл.\nЗадержка 10 секунд.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Отмена", callback_data=f"account_{account_id}")]])
    )

async def process_subscribe_input(message: types.Message, state: FSMContext):
    data = await state.get_data()
    account_id = data["subscribe_account"]
    client = await get_client(account_id)

    if message.document:
        if not message.document.file_name.endswith('.txt'):
            await message.answer("❌ Текстовый файл .txt")
            return
        file = await message.bot.get_file(message.document.file_id)
        downloaded: BytesIO = await message.bot.download_file(file.file_path)
        content = downloaded.getvalue().decode('utf-8')
    else:
        content = message.text

    if not content:
        await message.answer("❌ Пустое сообщение.")
        return

    links = []
    for token in re.split(r'[\s,]+', content):
        token = token.strip()
        if not token:
            continue
        if token.startswith('@'):
            links.append(token)
        elif 't.me/' in token or 'telegram.me/' in token:
            links.append(token)
        elif re.match(r'^[a-zA-Z][\w]{3,31}$', token):
            links.append('@' + token)

    if not links:
        await message.answer("❌ Не найдено ни одной ссылки или @username.")
        return

    total = len(links)
    await message.answer(f"⏳ Подписка на {total} групп...")
    success = 0
    failed = []
    for i, link in enumerate(links, 1):
        try:
            if link.startswith('@'):
                entity = await client.get_entity(link)
                if not hasattr(entity, 'broadcast') and not hasattr(entity, 'megagroup'):
                    failed.append(f"{link}: это не группа/канал")
                    continue
                await client(JoinChannelRequest(channel=entity))
            elif link.startswith('https://t.me/+') or link.startswith('t.me/+'):
                try:
                    invite_hash = link.split('/')[-1].split('?')[0]
                    await client(ImportChatInviteRequest(invite_hash))
                    entity = await client.get_entity(link)
                except InviteHashExpiredError:
                    failed.append(f"{link}: ссылка истекла")
                    continue
                except Exception as e:
                    failed.append(f"{link}: {str(e)}")
                    continue
            else:
                entity = await client.get_entity(link)
                await client(JoinChannelRequest(channel=entity))

            username = getattr(entity, 'username', None)
            title = getattr(entity, 'title', None) or link
            if username:
                async with db_lock:
                    async with aiosqlite.connect(DB_PATH) as db:
                        await db.execute("INSERT OR IGNORE INTO groups (account_id, username, title) VALUES (?,?,?)",
                                         (account_id, username, title))
                        await db.commit()
            else:
                chat_id = str(entity.id)
                async with db_lock:
                    async with aiosqlite.connect(DB_PATH) as db:
                        await db.execute("INSERT OR IGNORE INTO groups (account_id, username, title) VALUES (?,?,?)",
                                         (account_id, chat_id, title))
                        await db.commit()
            success += 1
            await message.answer(f"✅ [{i}/{total}] Подписан на {link}")
        except errors.FloodWaitError as e:
            await message.answer(f"⚠️ FloodWait {e.seconds} сек.")
            await asyncio.sleep(e.seconds)
            try:
                if link.startswith('@'):
                    entity = await client.get_entity(link)
                    await client(JoinChannelRequest(channel=entity))
                else:
                    try:
                        entity = await client.get_entity(link)
                        await client(JoinChannelRequest(channel=entity))
                    except Exception:
                        invite_hash = link.split('/')[-1].split('?')[0]
                        await client(ImportChatInviteRequest(invite_hash))
                entity = await client.get_entity(link)
                username = getattr(entity, 'username', None)
                title = getattr(entity, 'title', None) or link
                if username:
                    async with db_lock:
                        async with aiosqlite.connect(DB_PATH) as db:
                            await db.execute("INSERT OR IGNORE INTO groups (account_id, username, title) VALUES (?,?,?)",
                                             (account_id, username, title))
                            await db.commit()
                else:
                    chat_id = str(entity.id)
                    async with db_lock:
                        async with aiosqlite.connect(DB_PATH) as db:
                            await db.execute("INSERT OR IGNORE INTO groups (account_id, username, title) VALUES (?,?,?)",
                                             (account_id, chat_id, title))
                            await db.commit()
                success += 1
                await message.answer(f"✅ [{i}/{total}] Подписан на {link}")
            except Exception as e2:
                failed.append(f"{link}: {e2}")
        except Exception as e:
            failed.append(f"{link}: {e}")

        if i < total:
            await asyncio.sleep(10)

    await state.clear()
    report = f"📊 Подписка завершена: успешно {success} из {total}."
    if failed:
        report += f"\n❌ Ошибки:\n" + "\n".join(failed)
    await message.answer(report, reply_markup=main_reply_kb())

async def templates_menu(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    await show_templates_page(message, state, page=0, user_id=user_id)

async def show_templates_page(message_or_callback, state, page=0, user_id=None):
    if isinstance(message_or_callback, types.CallbackQuery):
        user_id = message_or_callback.from_user.id
        msg = message_or_callback.message
    else:
        user_id = message_or_callback.from_user.id
        msg = message_or_callback

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM templates WHERE user_id = ?", (user_id,)) as cursor:
            total = (await cursor.fetchone())[0]
        async with db.execute("SELECT id, name, text FROM templates WHERE user_id = ? ORDER BY id LIMIT 5 OFFSET ?", (user_id, page * 5)) as cursor:
            templates = await cursor.fetchall()
    if total == 0:
        text = "📋 У вас пока нет шаблонов."
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Создать шаблон", callback_data="create_template")],
            [InlineKeyboardButton(text="🔙 Главное меню", callback_data="main_menu")]
        ])
    else:
        text = f"📋 Шаблоны (страница {page+1} из {(total-1)//5 + 1}):"
        builder = InlineKeyboardBuilder()
        for tpl in templates:
            preview = tpl[2][:50] + ("..." if len(tpl[2]) > 50 else "")
            builder.row(InlineKeyboardButton(text=f"📝 {tpl[1]}", callback_data=f"tpl_view_{tpl[0]}"))
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"tpl_page_{page-1}"))
        if (page+1)*5 < total:
            nav_buttons.append(InlineKeyboardButton(text="➡️ Вперёд", callback_data=f"tpl_page_{page+1}"))
        if nav_buttons:
            builder.row(*nav_buttons)
        builder.row(InlineKeyboardButton(text="➕ Создать шаблон", callback_data="create_template"))
        builder.row(InlineKeyboardButton(text="🔙 Главное меню", callback_data="main_menu"))
        kb = builder.as_markup()

    if isinstance(message_or_callback, types.CallbackQuery):
        await message_or_callback.message.edit_text(text, reply_markup=kb)
    else:
        await message_or_callback.answer(text, reply_markup=kb)

async def view_template(callback: types.CallbackQuery, state: FSMContext):
    tpl_id = int(callback.data.split("_")[2])
    user_id = callback.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT * FROM templates WHERE id = ? AND user_id = ?", (tpl_id, user_id)) as cursor:
            tpl = await cursor.fetchone()
    if not tpl:
        await callback.answer("Шаблон не найден")
        return
    text = f"📌 *{tpl[1]}*\n💬 {tpl[2]}"
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="🗑 Удалить", callback_data=f"tpl_delete_{tpl_id}"))
    builder.row(InlineKeyboardButton(text="🔙 К списку", callback_data="tpl_page_0"))
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="Markdown")

async def delete_template(callback: types.CallbackQuery, state: FSMContext):
    tpl_id = int(callback.data.split("_")[2])
    user_id = callback.from_user.id
    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("DELETE FROM templates WHERE id = ? AND user_id = ?", (tpl_id, user_id))
            await db.commit()
    await callback.answer("Шаблон удалён")
    await show_templates_page(callback, state, page=0)

async def create_template_start(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM templates WHERE user_id = ?", (user_id,)) as cursor:
            count = (await cursor.fetchone())[0]
    if count >= 50:
        await callback.answer("Лимит шаблонов (50)", show_alert=True)
        return
    await state.set_state(TemplateCreate.name)
    await callback.message.edit_text("Введите название шаблона:",
                                     reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                         [InlineKeyboardButton(text="🔙 Отмена", callback_data="main_menu")]]))

async def template_name_input(message: types.Message, state: FSMContext):
    name = message.text.strip()
    if not name:
        await message.answer("Название не может быть пустым")
        return
    user_id = message.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT id FROM templates WHERE name = ? AND user_id = ?", (name, user_id)) as cursor:
            exists = await cursor.fetchone()
    if exists:
        await message.answer("Шаблон с таким названием уже существует")
        return
    await state.update_data(template_name=name)
    await state.set_state(TemplateCreate.text)
    await message.answer("Введите текст шаблона:",
                         reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                             [InlineKeyboardButton(text="🔙 Отмена", callback_data="main_menu")]]))

async def template_text_input(message: types.Message, state: FSMContext):
    text = message.text.strip()
    data = await state.get_data()
    name = data["template_name"]
    user_id = message.from_user.id
    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("INSERT INTO templates (user_id, name, text) VALUES (?,?,?)", (user_id, name, text))
            await db.commit()
    await state.clear()
    await message.answer(f"✅ Шаблон \"{name}\" создан.", reply_markup=main_reply_kb())
    await show_templates_page(message, state, page=0)

async def choose_text_source_for_group(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    if len(parts) < 3:
        return
    account_id = int(parts[1])
    group_id = int(parts[2])
    await state.update_data(set_group_account=account_id, set_group_id=group_id)
    await state.set_state(SetGroupText.choose_text_source)
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="📝 Ввести текст вручную", callback_data="text_manual"))
    builder.row(InlineKeyboardButton(text="📋 Выбрать шаблон", callback_data="text_template"))
    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data=f"group_{account_id}_{group_id}"))
    await callback.message.edit_text("Выберите источник текста:", reply_markup=builder.as_markup())

async def process_text_source_choice(callback: types.CallbackQuery, state: FSMContext):
    choice = callback.data
    if choice == "text_manual":
        await state.set_state(SetGroupText.text)
        await callback.message.edit_text("📝 Введите текст сообщения:",
                                         reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                             [InlineKeyboardButton(text="🔙 Отмена", callback_data="main_menu")]]))
    elif choice == "text_template":
        await state.set_state(SetGroupText.choose_text_source)
        await show_templates_for_selection(callback.message, state, page=0, user_id=callback.from_user.id)

async def show_templates_for_selection(message, state, page=0, user_id=None):
    if user_id is None:
        user_id = message.chat.id
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM templates WHERE user_id = ?", (user_id,)) as cursor:
            total = (await cursor.fetchone())[0]
        async with db.execute("SELECT id, name, text FROM templates WHERE user_id = ? ORDER BY id LIMIT 5 OFFSET ?", (user_id, page * 5)) as cursor:
            templates = await cursor.fetchall()
    if total == 0:
        await message.edit_text("Нет шаблонов. Создайте хотя бы один.",
                                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                    [InlineKeyboardButton(text="🔙 Назад", callback_data="text_manual")]]))
        return
    builder = InlineKeyboardBuilder()
    for tpl in templates:
        builder.row(InlineKeyboardButton(text=f"📝 {tpl[1]}", callback_data=f"tpl_select_{tpl[0]}"))
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(text="⬅️", callback_data=f"tpl_sel_page_{page-1}"))
    if (page+1)*5 < total:
        nav_buttons.append(InlineKeyboardButton(text="➡️", callback_data=f"tpl_sel_page_{page+1}"))
    if nav_buttons:
        builder.row(*nav_buttons)
    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data="text_manual"))
    await message.edit_text("Выберите шаблон:", reply_markup=builder.as_markup())

async def template_selected_for_group(callback: types.CallbackQuery, state: FSMContext):
    tpl_id = int(callback.data.split("_")[2])
    user_id = callback.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT text FROM templates WHERE id = ? AND user_id = ?", (tpl_id, user_id)) as cursor:
            tpl = await cursor.fetchone()
    if not tpl:
        await callback.answer("Шаблон не найден")
        return
    text = tpl[0]
    data = await state.get_data()
    account_id = data["set_group_account"]
    group_id = data["set_group_id"]
    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE groups SET mailing_text = ? WHERE id = ?", (text, group_id))
            await db.commit()
    await state.update_data(temp_group_text=text)
    await state.set_state(SetGroupText.interval)
    await callback.message.edit_text("✅ Текст из шаблона применён.\n⏱ Введите интервал в секундах:",
                                     reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                         [InlineKeyboardButton(text="🔙 Отмена", callback_data="main_menu")]]))

async def group_text_input(message: types.Message, state: FSMContext):
    await state.update_data(temp_group_text=message.text.strip())
    await state.set_state(SetGroupText.interval)
    await message.answer("⏱ Введите интервал в секундах:",
                         reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                             [InlineKeyboardButton(text="🔙 Отмена", callback_data="main_menu")]]))

async def group_interval_input(message: types.Message, state: FSMContext):
    try:
        interval = int(message.text.strip())
    except ValueError:
        await message.answer("❌ Введите целое число:")
        return
    data = await state.get_data()
    account_id = data["set_group_account"]
    group_id = data["set_group_id"]
    text = data["temp_group_text"]
    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE groups SET mailing_text = ?, interval_seconds = ? WHERE id = ?",
                             (text, interval, group_id))
            await db.commit()
    await state.clear()
    await message.answer("✅ Параметры сохранены.",
                         reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                             [InlineKeyboardButton(text="🔙 К группе", callback_data=f"group_{account_id}_{group_id}")]]))

async def mass_mailing_start(callback: types.CallbackQuery, state: FSMContext):
    account_id = int(callback.data.split("_")[2])
    await state.update_data(mass_account=account_id)
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="⏰ Одинаковый интервал", callback_data="mass_mode_same"))
    builder.row(InlineKeyboardButton(text="🎲 Разный интервал", callback_data="mass_mode_random"))
    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data=f"account_{account_id}"))
    await callback.message.edit_text("Выберите режим отправки:", reply_markup=builder.as_markup())

async def mass_mode_selected(callback: types.CallbackQuery, state: FSMContext):
    mode = callback.data.split("_")[2]
    await state.update_data(mass_mode=mode)
    await state.set_state(MassMailing.choose_text_source)
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="📝 Ввести текст вручную", callback_data="mass_text_manual"))
    builder.row(InlineKeyboardButton(text="📋 Выбрать шаблон", callback_data="mass_text_template"))
    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data=f"mass_start_{ (await state.get_data())['mass_account'] }"))
    await callback.message.edit_text("Выберите источник текста:", reply_markup=builder.as_markup())

async def mass_text_source_choice(callback: types.CallbackQuery, state: FSMContext):
    choice = callback.data
    if choice == "mass_text_manual":
        await state.set_state(MassMailing.text)
        await callback.message.edit_text("📝 Введите текст сообщения:",
                                         reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                             [InlineKeyboardButton(text="🔙 Отмена", callback_data="main_menu")]]))
    elif choice == "mass_text_template":
        await state.set_state(MassMailing.choose_text_source)
        await show_mass_templates_selection(callback.message, state, page=0, user_id=callback.from_user.id)

async def show_mass_templates_selection(message, state, page=0, user_id=None):
    if user_id is None:
        user_id = message.chat.id
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM templates WHERE user_id = ?", (user_id,)) as cursor:
            total = (await cursor.fetchone())[0]
        async with db.execute("SELECT id, name, text FROM templates WHERE user_id = ? ORDER BY id LIMIT 5 OFFSET ?", (user_id, page * 5)) as cursor:
            templates = await cursor.fetchall()
    if total == 0:
        await message.edit_text("Нет шаблонов.",
                                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                    [InlineKeyboardButton(text="🔙 Назад", callback_data="mass_text_manual")]]))
        return
    builder = InlineKeyboardBuilder()
    for tpl in templates:
        builder.row(InlineKeyboardButton(text=f"📝 {tpl[1]}", callback_data=f"mass_tpl_select_{tpl[0]}"))
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(text="⬅️", callback_data=f"mass_tpl_page_{page-1}"))
    if (page+1)*5 < total:
        nav_buttons.append(InlineKeyboardButton(text="➡️", callback_data=f"mass_tpl_page_{page+1}"))
    if nav_buttons:
        builder.row(*nav_buttons)
    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data="mass_text_manual"))
    await message.edit_text("Выберите шаблон:", reply_markup=builder.as_markup())

async def mass_template_selected(callback: types.CallbackQuery, state: FSMContext):
    tpl_id = int(callback.data.split("_")[3])
    user_id = callback.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT text FROM templates WHERE id = ? AND user_id = ?", (tpl_id, user_id)) as cursor:
            tpl = await cursor.fetchone()
    if not tpl:
        await callback.answer("Шаблон не найден")
        return
    text = tpl[0]
    await state.update_data(mass_text=text)
    data = await state.get_data()
    mode = data["mass_mode"]
    if mode == "same":
        await state.set_state(MassMailing.interval)
        await callback.message.edit_text("⏱ Введите интервал (сек):",
                                         reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                             [InlineKeyboardButton(text="🔙 Отмена", callback_data="main_menu")]]))
    else:
        await state.set_state(MassMailing.random_min)
        await callback.message.edit_text("🎲 Мин. интервал (сек):",
                                         reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                             [InlineKeyboardButton(text="🔙 Отмена", callback_data="main_menu")]]))

async def mass_text_input(message: types.Message, state: FSMContext):
    await state.update_data(mass_text=message.text.strip())
    data = await state.get_data()
    mode = data["mass_mode"]
    if mode == "same":
        await state.set_state(MassMailing.interval)
        await message.answer("⏱ Введите интервал (сек):",
                             reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                 [InlineKeyboardButton(text="🔙 Отмена", callback_data="main_menu")]]))
    else:
        await state.set_state(MassMailing.random_min)
        await message.answer("🎲 Мин. интервал (сек):",
                             reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                 [InlineKeyboardButton(text="🔙 Отмена", callback_data="main_menu")]]))

async def mass_interval_input(message: types.Message, state: FSMContext):
    data = await state.get_data()
    mode = data["mass_mode"]
    if mode == "same":
        try:
            interval = int(message.text.strip())
        except ValueError:
            await message.answer("❌ Введите целое число:")
            return
        await state.update_data(mass_interval=interval)
        await execute_mass_mailing(message, state, lambda: interval)
    else:
        try:
            min_val = int(message.text.strip())
        except ValueError:
            await message.answer("❌ Введите целое число:")
            return
        await state.update_data(mass_random_min=min_val)
        await state.set_state(MassMailing.random_max)
        await message.answer("🎲 Макс. интервал (сек):",
                             reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                 [InlineKeyboardButton(text="🔙 Отмена", callback_data="main_menu")]]))

async def mass_random_max_input(message: types.Message, state: FSMContext):
    try:
        max_val = int(message.text.strip())
    except ValueError:
        await message.answer("❌ Введите целое число:")
        return
    data = await state.get_data()
    min_val = data["mass_random_min"]
    if max_val < min_val:
        await message.answer("❌ Максимум меньше минимума")
        return
    def random_interval():
        return random.randint(min_val, max_val)
    await state.update_data(mass_interval=(min_val, max_val))
    await execute_mass_mailing(message, state, random_interval)

async def execute_mass_mailing(message: types.Message, state: FSMContext, interval_getter):
    data = await state.get_data()
    account_id = data["mass_account"]
    text = data["mass_text"]
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT id, username FROM groups WHERE account_id = ?", (account_id,)) as cursor:
            groups = await cursor.fetchall()
    if not groups:
        await message.answer("❌ Нет групп для рассылки.")
        await state.clear()
        return
    count = 0
    for g_id, username in groups:
        if (account_id, g_id) in active_tasks and not active_tasks[(account_id, g_id)].done():
            continue
        task = asyncio.create_task(universal_mailing_loop(account_id, g_id, interval_getter, text_override=text))
        active_tasks[(account_id, g_id)] = task
        count += 1
    await state.clear()
    await message.answer(f"✅ Рассылка запущена в {count} групп(ы).",
                         reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                             [InlineKeyboardButton(text="🔙 К аккаунту", callback_data=f"account_{account_id}")]]))

async def mass_mailing_stop(callback: types.CallbackQuery, state: FSMContext):
    account_id = int(callback.data.split("_")[2])
    stopped = 0
    for (aid, gid), task in list(active_tasks.items()):
        if aid == account_id:
            task.cancel()
            stopped += 1
    await callback.message.edit_text(f"⏹️ Остановлено рассылок: {stopped}",
                                     reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                         [InlineKeyboardButton(text="🔙 К аккаунту", callback_data=f"account_{account_id}")]]))

async def start_group_mailing(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    if len(parts) < 4 or parts[0] != "start" or parts[1] != "group":
        return
    account_id = int(parts[2])
    group_id = int(parts[3])
    if (account_id, group_id) in active_tasks and not active_tasks[(account_id, group_id)].done():
        await callback.answer("Рассылка уже активна", show_alert=True)
        return
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT interval_seconds, mailing_text FROM groups WHERE id = ?", (group_id,)) as cursor:
            row = await cursor.fetchone()
    if not row or not row[1]:
        await callback.message.edit_text("❌ Сначала задайте текст и интервал.")
        return
    interval_val = row[0]
    interval_getter = lambda: interval_val
    task = asyncio.create_task(universal_mailing_loop(account_id, group_id, interval_getter, text_override=row[1]))
    active_tasks[(account_id, group_id)] = task
    await callback.message.edit_text("✅ Рассылка запущена.",
                                     reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                         [InlineKeyboardButton(text="🔙 К группе", callback_data=f"group_{account_id}_{group_id}")]]))

async def stop_group_mailing(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    if len(parts) < 4 or parts[0] != "stop" or parts[1] != "group":
        return
    account_id = int(parts[2])
    group_id = int(parts[3])
    task = active_tasks.pop((account_id, group_id), None)
    if task:
        task.cancel()
        await callback.message.edit_text("⏹️ Рассылка остановлена.",
                                         reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                             [InlineKeyboardButton(text="🔙 К группе", callback_data=f"group_{account_id}_{group_id}")]]))
    else:
        await callback.message.edit_text("Рассылка не была активна.",
                                         reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                             [InlineKeyboardButton(text="🔙 К группе", callback_data=f"group_{account_id}_{group_id}")]]))

async def history_callback(callback: types.CallbackQuery, state: FSMContext):
    await show_history(callback.message, state, user_id=callback.from_user.id, edit=True)

async def history_message(message: types.Message, state: FSMContext):
    await show_history(message, state, user_id=message.from_user.id, edit=False)

async def show_history(message, state, user_id=None, edit=False):
    if user_id is None:
        user_id = message.from_user.id if isinstance(message, types.Message) else message.message.chat.id
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT h.group_title, h.group_username, h.message_text, h.sent_at, a.phone FROM history h "
            "LEFT JOIN accounts a ON h.account_id = a.id WHERE h.user_id = ? ORDER BY h.id DESC LIMIT 10",
            (user_id,)
        ) as cursor:
            rows = await cursor.fetchall()
    if not rows:
        text = "📊 История пуста."
    else:
        text = "📊 *Последние 10 отправок:*\n\n"
        for r in rows:
            group = r[1] or r[0]
            msg_text = r[2][:50] + ("..." if len(r[2]) > 50 else "")
            time = r[3]
            phone = r[4] or "?"
            text += f"📍 {group}\n⏰ {time}\n💬 {msg_text}\n👤 {phone}\n\n"
    kb = main_reply_kb()
    if edit and isinstance(message, types.CallbackQuery):
        try:
            await message.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
        except Exception:
            await message.message.answer(text, reply_markup=kb, parse_mode="Markdown")
    else:
        await message.answer(text, reply_markup=kb, parse_mode="Markdown")

async def cancel_action(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback_main_menu(callback, state)

async def main():
    await init_db()
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())

    asyncio.create_task(history_writer())

    dp.message.register(cmd_start, F.text == "/start")
    dp.message.register(my_accounts, F.text == "📱 Мои аккаунты")
    dp.message.register(add_group_manual_start, F.text == "➕ Добавить группу")
    dp.message.register(history_message, F.text == "📊 История рассылки")
    dp.message.register(templates_menu, F.text == "📋 Шаблоны")
    dp.message.register(show_instruction, F.text == "📖 Инструкция")
    dp.message.register(show_support, F.text == "🆘 Поддержка")

    dp.message.register(add_account_text, F.text == "👤 Добавить аккаунт")
    dp.callback_query.register(add_account_start, F.data == "add_account")

    dp.message.register(add_account_phone, AddAccount.phone, F.text)
    dp.message.register(add_account_code, AddAccount.code, F.text)
    dp.message.register(add_account_password, AddAccount.password, F.text)

    dp.callback_query.register(account_info, F.data.startswith("account_"))
    dp.callback_query.register(delete_account_start, F.data.startswith("delete_account_"))
    dp.message.register(delete_account_confirm, DeleteAccount.confirm_phone, F.text)

    dp.callback_query.register(list_groups, F.data.startswith("groups_"))
    dp.callback_query.register(group_detail, F.data.startswith("group_"))
    dp.callback_query.register(delete_group, F.data.startswith("delete_group_"))
    dp.callback_query.register(add_all_groups, F.data.startswith("add_all_groups_"))
    dp.callback_query.register(delete_all_groups, F.data.startswith("delete_all_groups_"))

    dp.callback_query.register(manual_group_choose_account, F.data.startswith("manual_group_acc_"))
    dp.message.register(manual_group_input, AddGroupManual.input)

    dp.callback_query.register(guarantor_menu, F.data.startswith("guarantor_"))
    dp.message.register(guarantor_text_input, SetGuarantor.text, F.text)

    dp.callback_query.register(subscribe_menu, F.data.startswith("subscribe_menu_"))
    dp.message.register(process_subscribe_input, SubscribeGroups.wait_for_links_or_file)

    dp.callback_query.register(choose_text_source_for_group, F.data.startswith("settext_"))
    dp.callback_query.register(process_text_source_choice, F.data == "text_manual")
    dp.callback_query.register(process_text_source_choice, F.data == "text_template")
    dp.callback_query.register(template_selected_for_group, F.data.startswith("tpl_select_"))
    dp.callback_query.register(lambda c, s: show_templates_for_selection(c.message, s, page=int(c.data.split("_")[3])),
                               F.data.startswith("tpl_sel_page_"))
    dp.message.register(group_text_input, SetGroupText.text, F.text)
    dp.message.register(group_interval_input, SetGroupText.interval, F.text)

    dp.callback_query.register(mass_mailing_start, F.data.startswith("mass_start_"))
    dp.callback_query.register(mass_mode_selected, F.data.startswith("mass_mode_"))
    dp.callback_query.register(mass_text_source_choice, F.data == "mass_text_manual")
    dp.callback_query.register(mass_text_source_choice, F.data == "mass_text_template")
    dp.callback_query.register(mass_template_selected, F.data.startswith("mass_tpl_select_"))
    dp.callback_query.register(lambda c, s: show_mass_templates_selection(c.message, s, page=int(c.data.split("_")[3])),
                               F.data.startswith("mass_tpl_page_"))
    dp.message.register(mass_text_input, MassMailing.text, F.text)
    dp.message.register(mass_interval_input, MassMailing.interval, F.text)
    dp.message.register(mass_interval_input, MassMailing.random_min, F.text)
    dp.message.register(mass_random_max_input, MassMailing.random_max, F.text)
    dp.callback_query.register(mass_mailing_stop, F.data.startswith("mass_stop_"))

    dp.callback_query.register(start_group_mailing, F.data.startswith("start_group_"))
    dp.callback_query.register(stop_group_mailing, F.data.startswith("stop_group_"))

    dp.callback_query.register(create_template_start, F.data == "create_template")
    dp.message.register(template_name_input, TemplateCreate.name, F.text)
    dp.message.register(template_text_input, TemplateCreate.text, F.text)
    dp.callback_query.register(view_template, F.data.startswith("tpl_view_"))
    dp.callback_query.register(delete_template, F.data.startswith("tpl_delete_"))
    dp.callback_query.register(lambda c, s: show_templates_page(c, s, page=int(c.data.split("_")[2])),
                               F.data.startswith("tpl_page_"))

    dp.callback_query.register(history_callback, F.data == "history")
    dp.callback_query.register(callback_main_menu, F.data == "main_menu")
    dp.callback_query.register(my_accounts_callback, F.data == "my_accounts")
    dp.callback_query.register(cancel_action, F.data == "cancel")

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())