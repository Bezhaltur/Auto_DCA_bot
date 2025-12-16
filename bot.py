import asyncio
import logging
import os
import hmac
import hashlib
import json
import time
import re
import requests
from dotenv import load_dotenv
import aiosqlite
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.fsm.storage.memory import MemoryStorage

# ============================================================================
# НАСТРОЙКА И КОНФИГУРАЦИЯ
# ============================================================================

# Настройка логирования - все операции бота логируются в файл и консоль
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Загрузка переменных окружения из .env файла
load_dotenv()

# API ключи для FixedFloat (сервис обмена криптовалют)
FF_API_KEY = os.getenv("FF_API_KEY")
FF_API_SECRET = os.getenv("FF_API_SECRET")
FF_API_URL = "https://ff.io/api/v2"  # базовый URL API FixedFloat

# Маппинг пользовательских названий сетей на коды FixedFloat API
# Обновляется при старте бота из реального списка валют
NETWORK_CODES = {
    "USDT-ARB": "USDTARBITRUM",
    "USDT-BSC": "USDTBSC", 
    "USDT-MATIC": "USDTMATIC",
}

# ============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================================

def validate_btc_address(address: str) -> bool:
    """
    Валидация Bitcoin адреса (Legacy, SegWit, Native SegWit).
    Поддерживает форматы: 1..., 3..., bc1...
    """
    if not address:
        return False
    
    # Legacy (P2PKH) - начинается с 1
    legacy_pattern = r'^[1][a-km-zA-HJ-NP-Z1-9]{25,34}$'
    # SegWit (P2SH) - начинается с 3
    segwit_pattern = r'^[3][a-km-zA-HJ-NP-Z1-9]{25,34}$'
    # Native SegWit (Bech32) - начинается с bc1
    bech32_pattern = r'^(bc1)[a-z0-9]{39,87}$'
    
    return bool(
        re.match(legacy_pattern, address) or 
        re.match(segwit_pattern, address) or 
        re.match(bech32_pattern, address)
    )


def ff_sign(data_str: str) -> str:
    """
    Создание HMAC-SHA256 подписи для запроса к FixedFloat API.
    Подпись создаётся из тела запроса и секретного ключа.
    """
    if not FF_API_SECRET:
        raise ValueError("FF_API_SECRET не задан в .env")
    return hmac.new(
        key=FF_API_SECRET.encode("utf-8"),
        msg=data_str.encode("utf-8"),
        digestmod=hashlib.sha256,
    ).hexdigest()


def ff_request(method: str, params=None) -> dict:
    """
    Универсальный синхронный POST-запрос к FixedFloat API.
    
    Args:
        method: endpoint API (например: "ccies", "price", "create")
        params: параметры запроса (dict)
    
    Returns:
        dict с данными ответа от API
    
    Raises:
        RuntimeError: если API вернул ошибку (code != 0)
    """
    if not FF_API_KEY or not FF_API_SECRET:
        raise ValueError("FF_API_KEY или FF_API_SECRET не заданы в .env")

    if params is None:
        params = {}

    url = f"{FF_API_URL}/{method}"
    data_str = json.dumps(params, separators=(",", ":"), ensure_ascii=False)

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json; charset=UTF-8",
        "X-API-KEY": FF_API_KEY,
        "X-API-SIGN": ff_sign(data_str),
    }

    logger.info(f"FixedFloat API запрос: {method} с параметрами {params}")
    resp = requests.post(url, data=data_str.encode("utf-8"), headers=headers, timeout=30)
    
    logger.info(f"FixedFloat ответ: status={resp.status_code}")

    data = resp.json()
    if data.get("code") != 0:
        logger.error(f"FixedFloat API ошибка: {data}")
        raise RuntimeError(f"FixedFloat error: {data}")
    
    return data["data"]


def ff_get_ccies():
    """
    Получить список всех доступных валют и сетей из FixedFloat.
    Используется для проверки актуальных кодов валют.
    """
    return ff_request("ccies", {})


async def ff_request_async(method: str, params=None) -> dict:
    """
    Асинхронная обёртка над ff_request для неблокирующих вызовов API.
    Выполняет синхронный запрос в отдельном потоке, чтобы не блокировать event loop бота.
    """
    return await asyncio.to_thread(ff_request, method, params)


async def update_network_codes():
    """
    Обновляет маппинг кодов сетей из реального API FixedFloat.
    Вызывается при старте бота для актуализации кодов валют.
    """
    try:
        items = await ff_request_async("ccies", {})
        for item in items:
            if item.get("coin") == "USDT":
                code = item.get("code")
                network = item.get("network", "").upper()
                
                # Обновляем известные маппинги
                if "ARBITRUM" in network:
                    NETWORK_CODES["USDT-ARB"] = code
                elif "BSC" in network or "BEP20" in network:
                    NETWORK_CODES["USDT-BSC"] = code
                elif "POLYGON" in network or "MATIC" in network:
                    NETWORK_CODES["USDT-MATIC"] = code
        
        logger.info(f"Обновлены коды сетей: {NETWORK_CODES}")
    except Exception as e:
        logger.error(f"Ошибка обновления кодов сетей: {e}")


def create_fixedfloat_order(network_key: str, amount_usdt: float, btc_address: str) -> dict:
    """
    Универсальная функция создания ордера на обмен USDT -> BTC через FixedFloat.
    
    Args:
        network_key: ключ сети из NETWORK_CODES (например "USDT-ARB")
        amount_usdt: сумма в USDT для обмена
        btc_address: адрес BTC для получения
    
    Returns:
        dict с данными созданного ордера (id, адрес депозита, сумма и т.д.)
    """
    from_ccy = NETWORK_CODES.get(network_key)
    if not from_ccy:
        raise ValueError(f"Неизвестная сеть: {network_key}")

    params = {
        "type": "fixed",  # фиксированный курс
        "fromCcy": from_ccy,  # из какой валюты
        "toCcy": "BTC",  # в какую валюту
        "direction": "from",  # фиксируем исходную сумму
        "amount": float(amount_usdt),
        "toAddress": btc_address,  # куда отправить BTC
    }
    
    logger.info(f"Создание ордера: {amount_usdt} {from_ccy} -> BTC на {btc_address}")
    return ff_request("create", params)


# ============================================================================
# ИНИЦИАЛИЗАЦИЯ БОТА И БД
# ============================================================================

# Токен Telegram бота из переменных окружения
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN не найден в .env!")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
DB_PATH = os.getenv("DATABASE_PATH", "./dca.db")


async def init_db():
    """
    Инициализация SQLite базы данных.
    Создаёт таблицу dca_plans для хранения планов автоматических покупок.
    
    Структура таблицы:
    - user_id: Telegram ID пользователя (НЕ уникальный - может быть несколько планов)
    - from_asset: сеть USDT (USDT-ARB, USDT-BSC, USDT-MATIC)
    - amount: сумма покупки в USD
    - interval_hours: интервал между покупками (в часах)
    - btc_address: адрес BTC для получения
    - next_run: UNIX timestamp следующего запуска
    - active: активен ли план (1/0)
    - active_order_id: ID активного ордера на FixedFloat (если есть)
    - active_order_address: адрес для депозита активного ордера
    - active_order_amount: сумма для отправки
    - active_order_expires: timestamp истечения ордера
    - deleted: флаг мягкого удаления (0 = активен, 1 = удалён)
    - Уникальность: может быть до 3 планов на одну сеть (user_id + from_asset)
    """
    async with aiosqlite.connect(DB_PATH) as db:
        # Создаём таблицу если её нет
        await db.execute('''
            CREATE TABLE IF NOT EXISTS dca_plans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                from_asset TEXT,
                amount REAL,
                interval_hours INTEGER,
                btc_address TEXT,
                next_run INTEGER,
                active BOOLEAN DEFAULT 1,
                created_at INTEGER DEFAULT (strftime('%s','now')),
                active_order_id TEXT,
                active_order_address TEXT,
                active_order_amount TEXT,
                active_order_expires INTEGER,
                deleted BOOLEAN DEFAULT 0
            )
        ''')
        
        # Проверяем существующие столбцы и добавляем новые если их нет
        async with db.execute("PRAGMA table_info(dca_plans)") as cursor:
            columns = await cursor.fetchall()
            existing_columns = [col[1] for col in columns]
        
        if "active_order_id" not in existing_columns:
            await db.execute("ALTER TABLE dca_plans ADD COLUMN active_order_id TEXT")
        if "active_order_address" not in existing_columns:
            await db.execute("ALTER TABLE dca_plans ADD COLUMN active_order_address TEXT")
        if "active_order_amount" not in existing_columns:
            await db.execute("ALTER TABLE dca_plans ADD COLUMN active_order_amount TEXT")
        if "active_order_expires" not in existing_columns:
            await db.execute("ALTER TABLE dca_plans ADD COLUMN active_order_expires INTEGER")
        if "deleted" not in existing_columns:
            await db.execute("ALTER TABLE dca_plans ADD COLUMN deleted BOOLEAN DEFAULT 0")
        
        await db.commit()
    logger.info("База данных инициализирована")


# ============================================================================
# DCA SCHEDULER - автоматическое выполнение планов
# ============================================================================

async def dca_scheduler():
    """
    Фоновая задача для автоматического выполнения DCA планов.
    Проверяет каждую минуту, есть ли планы готовые к выполнению (next_run <= now).
    Если план готов - создаёт ордер на FixedFloat и обновляет next_run.
    """
    logger.info("DCA Scheduler запущен")
    
    while True:
        try:
            await asyncio.sleep(60)  # проверка каждую минуту
            
            now = int(time.time())
            
            async with aiosqlite.connect(DB_PATH) as db:
                # Получаем все активные планы, которые пора выполнить (с ID!)
                # Только НЕ удаленные планы
                async with db.execute(
                    "SELECT id, user_id, from_asset, amount, interval_hours, btc_address, next_run "
                    "FROM dca_plans WHERE active = 1 AND deleted = 0 AND next_run <= ?",
                    (now,)
                ) as cursor:
                    plans = await cursor.fetchall()
                
                for plan in plans:
                    plan_id, user_id, from_asset, amount, interval_hours, btc_address, next_run = plan
                    
                    try:
                        # Проверяем нет ли уже активного ордера для этого плана
                        async with db.execute(
                            "SELECT active_order_id, active_order_expires FROM dca_plans WHERE id = ?",
                            (plan_id,)
                        ) as cur:
                            order_check = await cur.fetchone()
                        
                        if order_check:
                            existing_order_id, existing_order_expires = order_check
                            if existing_order_id and existing_order_expires and existing_order_expires > now:
                                # Уже есть активный ордер - пропускаем
                                logger.info(f"Пропуск DCA для plan_id={plan_id}: уже есть активный ордер {existing_order_id}")
                                continue
                        
                        logger.info(f"Выполнение DCA для plan_id={plan_id}, user_id={user_id}: {amount} {from_asset}")
                        
                        # Создаём ордер на обмен
                        order_data = await asyncio.to_thread(
                            create_fixedfloat_order,
                            from_asset,
                            amount,
                            btc_address
                        )
                        
                        order_id = order_data.get("id")
                        from_obj = order_data.get("from", {}) or {}
                        deposit_code = from_obj.get("code")
                        deposit_address = from_obj.get("address")
                        deposit_amount = from_obj.get("amount")
                        
                        # Получаем время истечения ордера
                        time_left = order_data.get("time", {}).get("left", 0)
                        order_expires = int(time.time()) + time_left
                        hours = time_left // 3600
                        minutes = (time_left % 3600) // 60
                        time_text = f"{hours}ч {minutes}мин" if hours > 0 else f"{minutes}мин"
                        
                        # Формируем ссылку на ордер
                        order_url = f"https://fixedfloat.com/order/{order_id}"
                        
                        # ВАЖНО: Сохраняем активный ордер в БД для предотвращения дубликатов
                        await db.execute(
                            "UPDATE dca_plans SET active_order_id = ?, active_order_address = ?, "
                            "active_order_amount = ?, active_order_expires = ? WHERE id = ?",
                            (order_id, deposit_address, f"{deposit_amount} {deposit_code}", order_expires, plan_id)
                        )
                        await db.commit()
                        
                        # Отправляем уведомление пользователю
                        await bot.send_message(
                            user_id,
                            f"✅ DCA план выполнен!\n\n"
                            f"🆔 Ордер: {order_id}\n"
                            f"🔗 Ссылка: {order_url}\n\n"
                            f"💵 Отправь: {deposit_amount} {deposit_code}\n"
                            f"📍 Адрес депозита:\n{deposit_address}\n\n"
                            f"⏰ Ордер действителен: {time_text}\n\n"
                            f"⚠️ Отправь токены на указанный адрес для завершения обмена."
                        )
                        
                        # Обновляем время следующего запуска ТОЛЬКО для этого конкретного плана
                        new_next_run = now + (interval_hours * 3600)
                        await db.execute(
                            "UPDATE dca_plans SET next_run = ? WHERE id = ?",
                            (new_next_run, plan_id)
                        )
                        await db.commit()
                        
                        logger.info(f"DCA выполнен успешно для plan_id={plan_id}, user_id={user_id}, order_id={order_id}")
                        
                    except Exception as e:
                        logger.error(f"Ошибка выполнения DCA для plan_id={plan_id}, user_id={user_id}: {e}")
                        # Отправляем уведомление об ошибке
                        try:
                            await bot.send_message(
                                user_id,
                                f"❌ Ошибка выполнения DCA плана:\n`{str(e)}`\n\n"
                                f"План будет повторён через {interval_hours}ч",
                                parse_mode="Markdown"
                            )
                        except:
                            pass
                        
                        # Откладываем на следующий интервал ТОЛЬКО для этого конкретного плана
                        new_next_run = now + (interval_hours * 3600)
                        await db.execute(
                            "UPDATE dca_plans SET next_run = ? WHERE id = ?",
                            (new_next_run, plan_id)
                        )
                        await db.commit()
                        
        except Exception as e:
            logger.error(f"Ошибка в DCA scheduler: {e}")


# ============================================================================
# TELEGRAM КОМАНДЫ - обработчики команд от пользователей
# ============================================================================


@dp.message(Command("start"))
async def cmd_start(message: Message):
    """
    Команда /start - приветствие и список доступных команд.
    Первая команда, которую видит новый пользователь.
    """
    user_id = message.from_user.id
    username = message.from_user.username or "пользователь"
    
    await message.answer(
        f"👋 Привет, @{username}!\n\n"
        f"🤖 **AutoDCA Bot** - автоматические покупки BTC через FixedFloat\n\n"
        f"📋 **Доступные команды:**\n\n"
        f"🔧 **Настройка:**\n"
        f"• `/setdca` - создать DCA план\n"
        f"• `/status` - посмотреть активные планы\n"
        f"• `/pause` - приостановить план\n"
        f"• `/resume` - возобновить план\n\n"
        f"💱 **Ручные операции:**\n"
        f"• `/execute` - выполнить обмен вручную\n"
        f"• `/networks` - посмотреть поддерживаемые сети\n"
        f"• `/limits` - проверить лимиты обмена\n\n"
        f"ℹ️ **Информация:**\n"
        f"• `/help` - подробная справка\n"
        f"• `/ping` - проверка работы бота\n\n"
        f"💡 Начни с команды `/setdca` для создания плана автоматических покупок!",
        parse_mode="Markdown"
    )
    logger.info(f"Новый пользователь: {user_id} (@{username})")


@dp.message(Command("help"))
async def cmd_help(message: Message):
    """
    Команда /help - подробная справка по использованию бота.
    """
    await message.answer(
        "📖 Подробная справка AutoDCA Bot\n\n"
        "Что такое DCA?\n"
        "Dollar Cost Averaging - стратегия регулярных покупок BTC на фиксированную сумму.\n\n"
        "Как настроить автоматические покупки:\n\n"
        "1. Создай план командой:\n"
        "/setdca USDT-ARB 50 24 bc1q...\n\n"
        "Параметры:\n"
        "• Сеть: USDT-ARB, USDT-BSC, USDT-MATIC\n"
        "• Сумма: 10-500 USD\n"
        "• Интервал: 12 (12ч), 24 (день), 168 (неделя), 720 (месяц)\n"
        "• BTC адрес: куда получать BTC\n\n"
        "2. Бот автоматически создаст ордера по расписанию\n\n"
        "3. Ты получишь уведомление с адресом для отправки USDT\n\n"
        "4. После отправки USDT получишь BTC на указанный адрес\n\n"
        "Безопасность:\n"
        "• Бот не хранит приватные ключи\n"
        "• Токены отправляешь сам вручную\n"
        "• Обмен через проверенный сервис FixedFloat"
    )


@dp.message(Command("ping"))
async def cmd_ping(message: Message):
    """
    Команда /ping - проверка работоспособности бота.
    Показывает user_id для технической поддержки.
    """
    user_id = message.from_user.id
    await message.answer(
        f"✅ Бот работает!\n\n"
        f"👤 Твой user_id: {user_id}\n"
        f"🕐 Время сервера: {time.strftime('%Y-%m-%d %H:%M:%S')}"
    )


@dp.message(Command("limits"))
async def cmd_limits(message: Message):
    """
    Команда /limits - показать лимиты обмена для USDT -> BTC для всех сетей.
    """
    try:
        await message.answer("⏳ Получаю лимиты...")
        
        limits_text = "💱 Лимиты обмена USDT → BTC\n\n"
        
        # Проверяем лимиты для всех поддерживаемых сетей
        for network_name, network_code in NETWORK_CODES.items():
            try:
                data = await ff_request_async("price", {
                    "type": "fixed",
                    "fromCcy": network_code,
                    "toCcy": "BTC",
                    "direction": "from",
                    "amount": 50,
                })
                
                from_info = data.get("from", {})
                to_info = data.get("to", {})
                min_amt = from_info.get("min", "—")
                max_amt = from_info.get("max", "—")
                to_amount = to_info.get("amount", "—")
                
                # Вычисляем курс: сколько USDT за 1 BTC
                if to_amount and to_amount != "—":
                    btc_amount = float(to_amount)  # BTC за 50 USDT
                    rate = 50.0 / btc_amount  # USDT за 1 BTC
                    rate_formatted = f"{rate:,.2f} USDT"
                else:
                    rate_formatted = "—"
                
                # Ограничиваем max суммой 500
                if max_amt != "—" and float(max_amt) > 500:
                    max_display = "500 (лимит бота)"
                else:
                    max_display = f"{max_amt}"
                
                limits_text += f"🔹 {network_name}:\n"
                limits_text += f"   Min: {min_amt} USDT\n"
                limits_text += f"   Max: {max_display} USDT\n"
                limits_text += f"   Курс: 1 BTC = {rate_formatted}\n\n"
                
            except Exception as e:
                logger.error(f"Ошибка получения лимитов для {network_name}: {e}")
                limits_text += f"🔹 {network_name}: ошибка\n\n"
        
        limits_text += "💡 Лимиты обновляются в реальном времени"
        
        await message.answer(limits_text)
        
    except Exception as e:
        logger.error(f"Ошибка получения лимитов: {e}")
        await message.answer(f"❌ Ошибка получения лимитов: {e}")


@dp.message(Command("networks"))
async def cmd_networks(message: Message):
    """
    Команда /networks - показать все доступные сети USDT с проверкой на FixedFloat.
    Проверяет в реальном времени какие сети доступны и работают.
    """
    try:
        await message.answer("⏳ Проверяю доступность сетей на FixedFloat...")
        
        # Получаем список всех валют из FixedFloat
        items = await ff_request_async("ccies", {})
        
        # Собираем доступные USDT сети
        available_networks = {}
        for item in items:
            if item.get("coin") == "USDT":
                code = item.get("code")
                network = item.get("network", "")
                available_networks[code] = network
        
        # Проверяем поддерживаемые ботом сети
        text = "🌐 Доступные сети USDT:\n\n"
        text += "Поддерживаемые ботом:\n"
        
        bot_supported = {
            "USDT-ARB": "USDTARBITRUM",
            "USDT-BSC": "USDTBSC",
            "USDT-MATIC": "USDTMATIC"
        }
        
        for bot_name, api_code in bot_supported.items():
            if api_code in available_networks:
                status = "✅"
                network_name = available_networks[api_code]
            else:
                status = "❌"
                network_name = "недоступна"
            text += f"{status} {bot_name} - {network_name}\n"
        
        # Показываем другие доступные USDT сети
        text += "\nДругие сети USDT на FixedFloat:\n"
        
        other_networks = []
        for code, network in available_networks.items():
            if code not in bot_supported.values():
                other_networks.append(f"• {code} - {network}")
        
        if other_networks:
            text += "\n".join(other_networks[:10])  # показываем до 10 других сетей
        else:
            text += "Нет других доступных сетей"
        
        text += "\n\n💡 Данные обновлены в реальном времени"
        
        await message.answer(text)
        
    except Exception as e:
        logger.error(f"Ошибка получения списка сетей: {e}")
        await message.answer(f"❌ Ошибка получения списка сетей: {e}")


@dp.message(lambda message: message.text and message.text.startswith("/execute"))
async def cmd_execute(message: Message):
    """
    Команда /execute или /execute_N - ручное выполнение обмена по DCA-плану.
    N - порядковый номер плана (1, 2, 3), как в /status
    """
    user_id = message.from_user.id
    
    # Пытаемся извлечь порядковый номер плана из команды
    text = message.text.strip()
    plan_number = None
    
    # Пробуем формат /execute_1
    if "_" in text:
        try:
            plan_number = int(text.split("_")[1])
        except:
            pass
    # Пробуем формат /execute 1
    elif " " in text:
        try:
            plan_number = int(text.split()[1])
        except:
            pass
    
    # Получаем список всех планов пользователя (в том же порядке что и в /status)
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT id, from_asset, amount, interval_hours FROM dca_plans WHERE user_id = ? AND deleted = 0 ORDER BY id",
            (user_id,),
        ) as cur:
            plans = await cur.fetchall()
    
    if not plans:
        await message.answer(
            "❗️У тебя нет DCA-планов.\n\n"
            "Создай план командой:\n"
            "/setdca USDT-ARB 50 24 bc1q..."
        )
        return
    
    # Если номер не указан - показываем список
    if plan_number is None:
        if len(plans) == 1:
            # Если план один - выполняем его автоматически
            plan_number = 1
        else:
            # Показываем список для выбора
            def format_interval(hours):
                if hours == 12:
                    return "12ч"
                elif hours == 24:
                    return "день"
                elif hours == 168:
                    return "неделю"
                elif hours == 720:
                    return "месяц"
                else:
                    return f"{hours}ч"
            
            text = "📋 Выбери план для выполнения:\n\n"
            for idx, p in enumerate(plans, start=1):
                text += f"• /execute_{idx} - {p[1]}, {p[2]}$, раз в {format_interval(p[3])}\n"
            await message.answer(text)
            return
    
    # Проверяем что номер плана валиден
    if plan_number < 1 or plan_number > len(plans):
        await message.answer(f"❌ План {plan_number} не найден\n\nУ тебя {len(plans)} план(ов)")
        return
    
    # Получаем реальный ID плана по порядковому номеру
    plan_id = plans[plan_number - 1][0]
    
    # Получаем конкретный план по ID (только не удаленные)
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT from_asset, amount, btc_address, active_order_id, active_order_address, "
            "active_order_amount, active_order_expires "
            "FROM dca_plans WHERE id = ? AND user_id = ? AND deleted = 0",
            (plan_id, user_id)
        ) as cur:
            row = await cur.fetchone()
    
    if not row:
        await message.answer("❌ План не найден или не принадлежит тебе")
        return
    
    from_asset, amount, btc_address, active_order_id, active_order_address, active_order_amount, active_order_expires = row

    # Проверяем есть ли уже активный ордер для ЭТОГО конкретного плана
    now = int(time.time())
    if active_order_id and active_order_expires and active_order_expires > now:
        # У этого плана уже есть активный неистёкший ордер
        time_left = active_order_expires - now
        hours = time_left // 3600
        minutes = (time_left % 3600) // 60
        time_text = f"{hours}ч {minutes}мин" if hours > 0 else f"{minutes}мин"
        
        order_url = f"https://fixedfloat.com/order/{active_order_id}"
        
        await message.answer(
            f"⚠️ У этого плана уже есть активный ордер!\n\n"
            f"🆔 ID: {active_order_id}\n"
            f"🔗 Ссылка: {order_url}\n\n"
            f"💵 Отправь: {active_order_amount}\n"
            f"📍 На адрес:\n{active_order_address}\n\n"
            f"🎯 Получишь BTC на:\n{btc_address}\n\n"
            f"⏰ Ордер действителен: {time_text}\n\n"
            f"💡 Дождись истечения текущего ордера или завершения обмена"
        )
        return
    elif active_order_id and active_order_expires and active_order_expires <= now:
        # Ордер истёк, очищаем его
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE dca_plans SET active_order_id = NULL, active_order_address = NULL, "
                "active_order_amount = NULL, active_order_expires = NULL WHERE id = ?",
                (plan_id,)
            )
            await db.commit()

    try:
        await message.answer(f"⏳ Создаю ордер {from_asset} на FixedFloat...")
        
        # Создаём ордер через универсальную функцию
        data = await asyncio.to_thread(
            create_fixedfloat_order,
            from_asset,
            amount,
            btc_address
        )

        if not data or not isinstance(data, dict):
            await message.answer(f"❌ Неожиданный ответ FixedFloat: {data}")
            return

        # Парсим ответ
        order_id = data.get("id")
        from_obj = data.get("from", {}) or {}
        deposit_code = from_obj.get("code")
        deposit_amount = from_obj.get("amount")
        deposit_address = from_obj.get("address")
        
        # Получаем время истечения ордера (в секундах)
        time_left = data.get("time", {}).get("left", 0)
        
        # Вычисляем часы и минуты
        hours = time_left // 3600
        minutes = (time_left % 3600) // 60
        
        # Формируем строку времени
        if hours > 0:
            time_text = f"{hours}ч {minutes}мин"
        else:
            time_text = f"{minutes}мин"

        # Формируем ссылку на ордер
        order_url = f"https://fixedfloat.com/order/{order_id}"
        
        # Сохраняем информацию об активном ордере в БД
        order_expires = int(time.time()) + time_left
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE dca_plans SET active_order_id = ?, active_order_address = ?, "
                "active_order_amount = ?, active_order_expires = ? WHERE id = ?",
                (order_id, deposit_address, f"{deposit_amount} {deposit_code}", order_expires, plan_id)
            )
            await db.commit()
        
        # Отправляем пользователю детали ордера
        await message.answer(
            f"✅ Ордер создан!\n\n"
            f"🆔 ID: {order_id}\n"
            f"🔗 Ссылка: {order_url}\n\n"
            f"💵 Отправь: {deposit_amount} {deposit_code}\n"
            f"📍 На адрес:\n{deposit_address}\n\n"
            f"🎯 Получишь BTC на:\n{btc_address}\n\n"
            f"⏰ Ордер действителен: {time_text}\n\n"
            f"⚠️ Отправь токены на указанный адрес для завершения обмена."
        )
        
        logger.info(f"Ручной ордер создан: user_id={user_id}, plan_id={plan_id}, order_id={order_id}")
        
    except Exception as e:
        logger.error(f"Ошибка создания ордера для user_id={user_id}: {e}")
        await message.answer(f"❌ Ошибка при создании ордера:\n{str(e)}")

@dp.message(Command("status"))
async def cmd_status(message: Message):
    """
    Команда /status - показать все DCA планы пользователя.
    Отображает все планы с деталями и временем следующего запуска.
    """
    user_id = message.from_user.id
    
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT id, from_asset, amount, interval_hours, btc_address, next_run, active, "
            "active_order_id, active_order_address, active_order_amount, active_order_expires "
            "FROM dca_plans WHERE user_id = ? AND deleted = 0 ORDER BY id", 
            (user_id,)
        ) as cursor:
            plans = await cursor.fetchall()
    
    if not plans:
        await message.answer(
            "📋 У тебя нет DCA планов\n\n"
            "Создай план командой:\n"
            "/setdca USDT-ARB 50 24 bc1q..."
        )
        return
    
    # Вычисляем текущее время
    now = int(time.time())
    
    # Функция для преобразования интервала в читаемый формат
    def format_interval(hours):
        if hours == 12:
            return "12 часов"
        elif hours == 24:
            return "день"
        elif hours == 168:
            return "неделю"
        elif hours == 720:
            return "месяц"
        else:
            return f"{hours}ч"
    
    status_text = f"📊 Твои DCA планы ({len(plans)}):\n\n"
    
    # Используем порядковый номер вместо ID из базы для понятной нумерации
    for idx, plan in enumerate(plans, start=1):
        plan_id, from_asset, amount, interval_hours, btc_address, next_run, active, \
        order_id, order_address, order_amount, order_expires = plan
        
        # Вычисляем время до следующего запуска
        time_left = next_run - now
        hours_left = max(0, time_left // 3600)
        minutes_left = max(0, (time_left % 3600) // 60)
        
        status_emoji = "✅" if active else "⏸"
        status_name = "Активен" if active else "Пауза"
        
        masked_addr = btc_address[:10] + "..." + btc_address[-6:] if len(btc_address) > 16 else btc_address
        
        status_text += (
            f"━━━━━━━━━━━━━━\n"
            f"📌 План {idx}\n"
            f"{status_emoji} {from_asset} - {status_name}\n"
            f"💵 Сумма: {amount} USD\n"
            f"⏱ Интервал: раз в {format_interval(interval_hours)}\n"
            f"🎯 BTC: {masked_addr}\n"
            f"⏰ Через: {hours_left}ч {minutes_left}мин\n"
        )
        
        # Проверяем есть ли активный ордер (и не истёк ли он)
        if order_id and order_expires:
            if order_expires > now:
                # Ордер активен
                order_time_left = order_expires - now
                order_hours = order_time_left // 3600
                order_minutes = (order_time_left % 3600) // 60
                order_time_text = f"{order_hours}ч {order_minutes}мин" if order_hours > 0 else f"{order_minutes}мин"
                
                order_url = f"https://fixedfloat.com/order/{order_id}"
                
                status_text += (
                    f"\n🔥 Активный ордер:\n"
                    f"ID: {order_id}\n"
                    f"Ссылка: {order_url}\n"
                    f"Отправь: {order_amount}\n"
                    f"На адрес: {order_address[:15]}...\n"
                    f"Истекает через: {order_time_text}\n"
                )
            else:
                # Ордер истёк - очищаем его в фоне
                async def cleanup_expired_order(plan_id):
                    async with aiosqlite.connect(DB_PATH) as db:
                        await db.execute(
                            "UPDATE dca_plans SET active_order_id = NULL, active_order_address = NULL, "
                            "active_order_amount = NULL, active_order_expires = NULL WHERE id = ?",
                            (plan_id,)
                        )
                        await db.commit()
                
                # Запускаем очистку в фоне (не блокируем ответ)
                asyncio.create_task(cleanup_expired_order(plan_id))
        
        status_text += (
            f"\nУправление этим планом:\n"
            f"/execute_{idx} - выполнить сейчас\n"
        )
        
        if active:
            status_text += f"/pause_{idx} - приостановить\n"
        else:
            status_text += f"/resume_{idx} - возобновить\n"
        
        status_text += f"/delete_{idx} - удалить\n"
    
    await message.answer(status_text)


@dp.message(lambda message: message.text and message.text.startswith("/pause"))
async def cmd_pause(message: Message):
    """
    Команда /pause или /pause_N - приостановить автоматическое выполнение DCA плана.
    N - порядковый номер плана (1, 2, 3), как в /status
    """
    user_id = message.from_user.id
    
    # Пытаемся извлечь порядковый номер плана из команды
    text = message.text.strip()
    plan_number = None
    
    if "_" in text:
        try:
            plan_number = int(text.split("_")[1])
        except:
            pass
    elif " " in text:
        try:
            plan_number = int(text.split()[1])
        except:
            pass
    
    async with aiosqlite.connect(DB_PATH) as db:
        if plan_number:
            # Получаем список планов для конвертации номера в ID
            async with db.execute(
                "SELECT id FROM dca_plans WHERE user_id = ? AND deleted = 0 ORDER BY id",
                (user_id,)
            ) as cur:
                plans = await cur.fetchall()
            
            if plan_number < 1 or plan_number > len(plans):
                await message.answer(f"❌ План {plan_number} не найден")
                return
            
            plan_id = plans[plan_number - 1][0]
            
            # Приостанавливаем по ID
            await db.execute(
                "UPDATE dca_plans SET active = 0 WHERE id = ? AND user_id = ? AND deleted = 0",
                (plan_id, user_id)
            )
            msg = f"⏸ План {plan_number} приостановлен"
        else:
            # Приостанавливаем все планы пользователя (только не удаленные)
            await db.execute(
                "UPDATE dca_plans SET active = 0 WHERE user_id = ? AND deleted = 0",
                (user_id,)
            )
            msg = "⏸ Все DCA планы приостановлены"
        
        await db.commit()
    
    await message.answer(
        f"{msg}\n\n"
        "Автоматические покупки остановлены.\n"
        "Для возобновления: /resume"
    )
    if plan_number:
        logger.info(f"DCA план приостановлен: user_id={user_id}, plan_number={plan_number}")
    else:
        logger.info(f"Все DCA планы приостановлены: user_id={user_id}")


@dp.message(lambda message: message.text and message.text.startswith("/resume"))
async def cmd_resume(message: Message):
    """
    Команда /resume или /resume_N - возобновить автоматическое выполнение DCA плана.
    N - порядковый номер плана (1, 2, 3), как в /status
    """
    user_id = message.from_user.id
    
    # Пытаемся извлечь порядковый номер плана из команды
    text = message.text.strip()
    plan_number = None
    
    if "_" in text:
        try:
            plan_number = int(text.split("_")[1])
        except:
            pass
    elif " " in text:
        try:
            plan_number = int(text.split()[1])
        except:
            pass
    
    async with aiosqlite.connect(DB_PATH) as db:
        if plan_number:
            # Получаем список планов для конвертации номера в ID
            async with db.execute(
                "SELECT id FROM dca_plans WHERE user_id = ? AND deleted = 0 ORDER BY id",
                (user_id,)
            ) as cur:
                plans = await cur.fetchall()
            
            if plan_number < 1 or plan_number > len(plans):
                await message.answer(f"❌ План {plan_number} не найден")
                return
            
            plan_id = plans[plan_number - 1][0]
            
            # Возобновляем по ID
            await db.execute(
                "UPDATE dca_plans SET active = 1 WHERE id = ? AND user_id = ? AND deleted = 0",
                (plan_id, user_id)
            )
            msg = f"▶️ План {plan_number} возобновлён"
        else:
            # Возобновляем все планы пользователя (только не удаленные)
            await db.execute(
                "UPDATE dca_plans SET active = 1 WHERE user_id = ? AND deleted = 0",
                (user_id,)
            )
            msg = "▶️ Все DCA планы возобновлены"
        
        await db.commit()
    
    await message.answer(
        f"{msg}\n\n"
        "Автоматические покупки снова активны.\n"
        "Проверь статус: /status"
    )
    if plan_number:
        logger.info(f"DCA план возобновлён: user_id={user_id}, plan_number={plan_number}")
    else:
        logger.info(f"Все DCA планы возобновлены: user_id={user_id}")


@dp.message(lambda message: message.text and message.text.startswith("/delete"))
async def cmd_delete(message: Message):
    """
    Команда /delete_N - удалить DCA план полностью.
    N - порядковый номер плана (1, 2, 3), как в /status
    """
    user_id = message.from_user.id
    
    # Извлекаем порядковый номер плана из команды
    text = message.text.strip()
    plan_number = None
    
    if "_" in text:
        try:
            plan_number = int(text.split("_")[1])
        except:
            pass
    elif " " in text:
        try:
            plan_number = int(text.split()[1])
        except:
            pass
    
    if plan_number is None:
        await message.answer(
            "❌ Укажи номер плана для удаления\n\n"
            "Формат: /delete_1\n"
            "Посмотри номера в /status"
        )
        return
    
    # Получаем список планов для конвертации номера в ID
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT id FROM dca_plans WHERE user_id = ? AND deleted = 0 ORDER BY from_asset, id",
            (user_id,)
        ) as cur:
            plans = await cur.fetchall()
    
    if plan_number < 1 or plan_number > len(plans):
        await message.answer(f"❌ План {plan_number} не найден")
        return
    
    plan_id = plans[plan_number - 1][0]
    
    async with aiosqlite.connect(DB_PATH) as db:
        # Проверяем что план существует и принадлежит пользователю (только не удаленные)
        async with db.execute(
            "SELECT from_asset, active_order_id, active_order_expires FROM dca_plans WHERE id = ? AND user_id = ? AND deleted = 0",
            (plan_id, user_id)
        ) as cur:
            row = await cur.fetchone()
        
        if not row:
            await message.answer("❌ План не найден или не принадлежит тебе")
            return
        
        from_asset, active_order_id, active_order_expires = row
        
        # Проверяем есть ли активный ордер и предупреждаем пользователя
        if active_order_id and active_order_expires:
            now = int(time.time())
            if active_order_expires > now:
                # Ордер еще действителен - помечаем план как удаленный (НЕ удаляем!)
                # Это сохраняет информацию об активном ордере для предотвращения дубликатов
                time_left = active_order_expires - now
                hours = time_left // 3600
                minutes = (time_left % 3600) // 60
                time_text = f"{hours}ч {minutes}мин" if hours > 0 else f"{minutes}мин"
                
                order_url = f"https://fixedfloat.com/order/{active_order_id}"
                
                # Помечаем план как удаленный (мягкое удаление)
                await db.execute(
                    "UPDATE dca_plans SET deleted = 1, active = 0 WHERE id = ? AND user_id = ?",
                    (plan_id, user_id)
                )
                await db.commit()
                
                await message.answer(
                    f"🗑 План {from_asset} удалён\n\n"
                    f"⚠️ У этого плана был активный ордер:\n"
                    f"🆔 ID: {active_order_id}\n"
                    f"🔗 Ссылка: {order_url}\n"
                    f"⏰ Истекает через: {time_text}\n\n"
                    f"💡 Ордер остаётся активным на FixedFloat.\n"
                    f"Завершите обмен или дождитесь истечения.\n\n"
                    f"❗️ Новый план с теми же параметрами (сеть + сумма + интервал + BTC адрес) можно создать только после истечения ордера.\n\n"
                    f"Проверь оставшиеся планы: /status"
                )
                logger.info(f"DCA план с активным ордером помечен как удаленный: user_id={user_id}, plan_id={plan_id}, asset={from_asset}, order_id={active_order_id}")
                return
        
        # Удаляем план без активного ордера (можно удалить физически)
        await db.execute(
            "UPDATE dca_plans SET deleted = 1, active = 0 WHERE id = ? AND user_id = ?",
            (plan_id, user_id)
        )
        await db.commit()
    
    await message.answer(
        f"🗑 План {from_asset} удалён\n\n"
        "Проверь оставшиеся планы: /status"
    )
    logger.info(f"DCA план удалён: user_id={user_id}, plan_id={plan_id}, asset={from_asset}")


@dp.message(Command("setdca"))
async def cmd_setdca(message: Message):
    """
    Команда /setdca - создать или обновить DCA план.
    Формат: /setdca СЕТЬ СУММА ИНТЕРВАЛ BTC_АДРЕС
    
    Параметры:
    - СЕТЬ: USDT-ARB, USDT-BSC, USDT-MATIC
    - СУММА: 10-500 USD
    - ИНТЕРВАЛ: 1, 6, 24, 168 (часов)
    - BTC_АДРЕС: валидный Bitcoin адрес
    """
    args = message.text.split()[1:]
    
    if len(args) != 4:
        await message.answer(
            "❌ Неверный формат\n\n"
            "Используй:\n"
            "/setdca СЕТЬ СУММА ИНТЕРВАЛ BTC_АДРЕС\n\n"
            "Примеры:\n"
            "/setdca USDT-ARB 50 24 bc1qxy2...\n"
            "/setdca USDT-BSC 100 168 bc1qxy2...\n\n"
            "Интервалы:\n"
            "12 - раз в 12 часов\n"
            "24 - раз в день\n"
            "168 - раз в неделю\n"
            "720 - раз в месяц\n\n"
            "Подробнее: /help"
        )
        return
    
    try:
        from_asset, amount_str, interval_str, btc_address = args
        
        # Нормализация названия сети
        from_asset = from_asset.upper().replace("_", "-")
        amount = float(amount_str)
        interval = int(interval_str)
        
        # Валидация параметров
        allowed_assets = set(NETWORK_CODES.keys())
        
        if from_asset not in allowed_assets:
            await message.answer(
                f"❌ Неподдерживаемая сеть: {from_asset}\n\n"
                f"Доступные сети:\n" + "\n".join(f"• {a}" for a in allowed_assets)
            )
            return
        
        if amount < 10 or amount > 500:
            await message.answer(
                "❌ Неверная сумма\n\n"
                "Диапазон: 10-500 USD"
            )
            return
        
        if interval not in [12, 24, 168, 720]:
            await message.answer(
                "❌ Неверный интервал\n\n"
                "Доступные:\n"
                "• 12 - раз в 12 часов\n"
                "• 24 - раз в день\n"
                "• 168 - раз в неделю (7 дней)\n"
                "• 720 - раз в месяц (30 дней)"
            )
            return
        
        # Валидация BTC адреса
        if not validate_btc_address(btc_address):
            await message.answer(
                "❌ Неверный BTC адрес\n\n"
                "Проверь адрес и попробуй снова.\n"
                "Поддерживаются форматы:\n"
                "• Legacy (1...)\n"
                "• SegWit (3...)\n"
                "• Native SegWit (bc1...)"
            )
            return
        
        # Функция для преобразования интервала
        def format_interval(hours):
            if hours == 12:
                return "12 часов"
            elif hours == 24:
                return "день"
            elif hours == 168:
                return "неделю"
            elif hours == 720:
                return "месяц"
            else:
                return f"{hours}ч"
        
        # Сохранение плана в БД
        user_id = message.from_user.id
        next_run = int(time.time()) + (interval * 3600)
        now = int(time.time())
        
        async with aiosqlite.connect(DB_PATH) as db:
            # Проверяем сколько НЕ удаленных планов уже есть для этой сети
            async with db.execute(
                "SELECT COUNT(*) FROM dca_plans WHERE user_id = ? AND from_asset = ? AND deleted = 0",
                (user_id, from_asset)
            ) as cur:
                count_row = await cur.fetchone()
                plans_count = count_row[0] if count_row else 0
            
            # Проверяем не существует ли уже такой же НЕ удаленный план (сеть + сумма + интервал)
            async with db.execute(
                "SELECT id, active_order_id, active_order_expires FROM dca_plans "
                "WHERE user_id = ? AND from_asset = ? AND amount = ? AND interval_hours = ? AND deleted = 0",
                (user_id, from_asset, amount, interval)
            ) as cur:
                duplicate = await cur.fetchone()
            
            if duplicate:
                plan_id, order_id, order_expires = duplicate
                
                # Проверяем есть ли активный ордер для этого плана
                if order_id and order_expires and order_expires > now:
                    time_left = order_expires - now
                    hours = time_left // 3600
                    minutes = (time_left % 3600) // 60
                    time_text = f"{hours}ч {minutes}мин" if hours > 0 else f"{minutes}мин"
                    order_url = f"https://fixedfloat.com/order/{order_id}"
                    
                    await message.answer(
                        f"❌ Такой план уже существует и у него есть активный ордер!\n\n"
                        f"📋 План: {from_asset}, {amount} USD, раз в {format_interval(interval)}\n\n"
                        f"🔥 Активный ордер:\n"
                        f"🆔 ID: {order_id}\n"
                        f"🔗 Ссылка: {order_url}\n"
                        f"⏰ Истекает через: {time_text}\n\n"
                        f"💡 Дождись истечения ордера или используй другие параметры"
                    )
                    return
                else:
                    # План есть, но ордера нет или истёк
                    await message.answer(
                        f"❌ Такой план уже существует!\n\n"
                        f"📋 План: {from_asset}, {amount} USD, раз в {format_interval(interval)}\n\n"
                        f"💡 Используй другую сумму или интервал"
                    )
                    return
            
            # Проверяем лимит (не больше 3 планов на сеть)
            if plans_count >= 3:
                await message.answer(
                    f"❌ Достигнут лимит планов для {from_asset}\n\n"
                    f"Максимум: 3 плана на одну сеть\n"
                    f"Текущих планов: {plans_count}\n\n"
                    f"💡 Удали один из планов: /status"
                )
                return
            
            # Проверяем есть ли активный ордер для ТОЧНО ТАКОГО ЖЕ плана (сеть + сумма + интервал + BTC адрес)
            # в удалённых планах
            async with db.execute(
                "SELECT active_order_id, active_order_address, active_order_amount, active_order_expires, btc_address "
                "FROM dca_plans WHERE user_id = ? AND from_asset = ? AND amount = ? AND interval_hours = ? "
                "AND active_order_id IS NOT NULL AND deleted = 1 "
                "ORDER BY active_order_expires DESC LIMIT 1",
                (user_id, from_asset, amount, interval)
            ) as cur:
                existing_order = await cur.fetchone()
            
            # Создаём новый план
            if existing_order and existing_order[3] and existing_order[3] > now:
                # Есть активный ордер от удалённого плана с теми же параметрами
                order_id, order_address, order_amount, order_expires, old_btc_address = existing_order
                
                # ВАЖНО: Проверяем совпадение BTC адреса!
                if old_btc_address != btc_address:
                    # BTC адрес отличается - не наследуем ордер, создаём новый план
                    await message.answer(
                        f"⚠️ Найден активный ордер от удалённого плана, но BTC адрес отличается!\n\n"
                        f"Старый адрес: {old_btc_address[:10]}...{old_btc_address[-6:]}\n"
                        f"Новый адрес: {btc_address[:10]}...{btc_address[-6:]}\n\n"
                        f"💡 Создаю новый план без наследования ордера.\n"
                        f"Старый ордер остаётся активным на FixedFloat."
                    )
                    # Создаём план без наследования ордера
                    await db.execute('''
                        INSERT INTO dca_plans 
                        (user_id, from_asset, amount, interval_hours, btc_address, next_run, active)
                        VALUES (?, ?, ?, ?, ?, ?, 1)
                    ''', (user_id, from_asset, amount, interval, btc_address, next_run))
                else:
                    # BTC адрес совпадает - наследуем ордер
                    await db.execute('''
                        INSERT INTO dca_plans 
                        (user_id, from_asset, amount, interval_hours, btc_address, next_run, active,
                         active_order_id, active_order_address, active_order_amount, active_order_expires)
                        VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
                    ''', (user_id, from_asset, amount, interval, btc_address, next_run,
                          order_id, order_address, order_amount, order_expires))
            else:
                # Нет активного ордера - создаём чистый план
                await db.execute('''
                    INSERT INTO dca_plans 
                    (user_id, from_asset, amount, interval_hours, btc_address, next_run, active)
                    VALUES (?, ?, ?, ?, ?, ?, 1)
                ''', (user_id, from_asset, amount, interval, btc_address, next_run))
            
            await db.commit()
            action = "создан"
        
        masked_addr = btc_address[:10] + "..." + btc_address[-6:] if len(btc_address) > 16 else btc_address
        
        # Форматируем интервал
        if interval == 12:
            interval_text = "12 часов"
        elif interval == 24:
            interval_text = "день"
        elif interval == 168:
            interval_text = "неделю"
        elif interval == 720:
            interval_text = "месяц"
        else:
            interval_text = f"{interval}ч"
        
        await message.answer(
            f"✅ DCA план {action}!\n\n"
            f"💱 Сеть: {from_asset}\n"
            f"💵 Сумма: {amount} USD\n"
            f"⏱ Интервал: раз в {interval_text}\n"
            f"🎯 На адрес: {masked_addr}\n\n"
            f"⏰ Первый запуск через {interval_text}\n\n"
            f"💡 Проверить статус: /status\n"
            f"💡 Выполнить сейчас: /execute"
        )
        
        logger.info(f"DCA план {action}: user_id={user_id}, {from_asset}, {amount} USD, {interval}ч")
        
    except ValueError as e:
        await message.answer(f"❌ Ошибка в параметрах: {str(e)}")
    except Exception as e:
        logger.error(f"Ошибка создания DCA плана: {e}")
        await message.answer(f"❌ Ошибка: {str(e)}")


# ============================================================================
# ЗАПУСК БОТА
# ============================================================================

async def main():
    """
    Главная функция запуска бота.
    Инициализирует БД, обновляет коды сетей, запускает scheduler и polling.
    """
    logger.info("=" * 60)
    logger.info("Запуск AutoDCA Bot...")
    
    # Инициализация базы данных
    await init_db()
    
    # Обновление актуальных кодов сетей из FixedFloat
    await update_network_codes()
    
    logger.info("🚀 AutoDCA Bot успешно запущен!")
    logger.info("=" * 60)
    
    # Запуск фонового планировщика DCA
    asyncio.create_task(dca_scheduler())
    
    # Запуск обработки сообщений от Telegram
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
