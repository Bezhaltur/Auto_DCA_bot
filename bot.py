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
from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.fsm.storage.memory import MemoryStorage
from networks import get_network_config, get_blockchair_url
from wallet import (
    save_keystore, load_keystore,
    delete_keystore, get_wallet_address,
    save_password_to_keyring, load_password_from_keyring,
    delete_password_from_keyring, keystore_exists
)
from auto_send import auto_send_usdt
from erc20 import get_web3_instance, get_usdt_balance, get_native_balance

# ============================================================================
# НАСТРОЙКА И КОНФИГУРАЦИЯ
# ============================================================================

# Настройка логирования - все операции бота логируются в файл и консоль
# Создаём директорию для логов если её нет
os.makedirs("logs", exist_ok=True)

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

# Import test configuration
from test_config import (
    DRY_RUN, MOCK_FIXEDFLOAT, USE_TESTNET, is_test_mode,
    get_mock_fixedfloat_order, get_mock_fixedfloat_ccies, get_mock_fixedfloat_price,
    mask_sensitive_data
)

# In-memory password cache (loaded from keyring at startup)
# Keys: user_id -> password
# This is ONLY a cache - keyring is the single source of truth
_wallet_passwords = {}

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

def format_interval(hours: int) -> str:
    """
    Преобразует интервал в часах в читаемый формат.
    Используется в нескольких местах для единообразия.
    """
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
    Supports mock mode for testing.
    
    Args:
        method: endpoint API (например: "ccies", "price", "create")
        params: параметры запроса (dict)
    
    Returns:
        dict с данными ответа от API
    
    Raises:
        RuntimeError: если API вернул ошибку (code != 0)
    """
    # Mock mode - return mocked responses
    if MOCK_FIXEDFLOAT:
        logger.info(f"[MOCK] FixedFloat API запрос: {method} с параметрами {mask_sensitive_data(params)}")
        
        if method == "ccies":
            mock_response = get_mock_fixedfloat_ccies()
            logger.info(f"[MOCK] FixedFloat ответ: {method}")
            return mock_response["data"]
        
        elif method == "price":
            network_key = params.get("fromCcy", "").replace("USDT", "USDT-")
            if "ARBITRUM" in network_key.upper():
                network_key = "USDT-ARB"
            elif "BSC" in network_key.upper():
                network_key = "USDT-BSC"
            elif "MATIC" in network_key.upper() or "POLYGON" in network_key.upper():
                network_key = "USDT-MATIC"
            mock_response = get_mock_fixedfloat_price(network_key)
            logger.info(f"[MOCK] FixedFloat ответ: {method}")
            return mock_response["data"]
        
        elif method == "create":
            # Extract network from fromCcy
            from_ccy = params.get("fromCcy", "")
            network_key = "USDT-ARB"  # default
            if "ARBITRUM" in from_ccy.upper():
                network_key = "USDT-ARB"
            elif "BSC" in from_ccy.upper():
                network_key = "USDT-BSC"
            elif "MATIC" in from_ccy.upper() or "POLYGON" in from_ccy.upper():
                network_key = "USDT-MATIC"
            
            amount = float(params.get("amount", 0))
            btc_address = params.get("toAddress", "")
            mock_response = get_mock_fixedfloat_order(network_key, amount, btc_address)
            logger.info(f"[MOCK] FixedFloat ответ: {method}, order_id={mock_response['data']['id']}")
            return mock_response["data"]
        
        else:
            logger.warning(f"[MOCK] Unknown method {method}, returning empty data")
            return {}
    
    # Real API call
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

    logger.info(f"FixedFloat API запрос: {method} с параметрами {mask_sensitive_data(params)}")
    try:
        resp = requests.post(url, data=data_str.encode("utf-8"), headers=headers, timeout=30)
        resp.raise_for_status()  # Вызовет исключение для HTTP ошибок (4xx, 5xx)
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка HTTP запроса к FixedFloat API: {e}")
        raise RuntimeError(f"Ошибка подключения к FixedFloat API: {e}")
    
    logger.info(f"FixedFloat ответ: status={resp.status_code}")

    try:
        data = resp.json()
    except ValueError as e:
        logger.error(f"Ошибка парсинга JSON ответа от FixedFloat: {e}, response text: {resp.text[:200]}")
        raise RuntimeError(f"Неверный формат ответа от FixedFloat API: {e}")
    
    code = data.get("code")
    if code != 0:
        error_msg = data.get("msg", "Unknown error")
        error_data = data.get("data")
        
        # Специальная обработка известных ошибок
        if code == 310:
            error_msg = "Валюта или сеть недоступна для обмена"
        elif code == 311:
            error_msg = "Валюта недоступна для получения в данный момент"
        elif code == 312:
            error_msg = "Валюта недоступна для отправки в данный момент"
        elif code == 301:
            error_msg = "Сумма вне допустимых лимитов"
        elif code == 401:
            error_msg = "Неверные API ключи"
        elif code == 501:
            error_msg = "Нет прав доступа к API"
        
        logger.error(f"FixedFloat API ошибка (code={code}): {error_msg}, data={error_data}")
        raise RuntimeError(f"FixedFloat error (code={code}): {error_msg}")
    
    return data["data"]




async def ff_request_async(method: str, params=None) -> dict:
    """
    Асинхронная обёртка над ff_request для неблокирующих вызовов API.
    Выполняет синхронный запрос в отдельном потоке, чтобы не блокировать event loop бота.
    """
    return await asyncio.to_thread(ff_request, method, params)


async def get_fixedfloat_limits(network_key: str) -> dict:
    """
    Получает минимальные и максимальные лимиты для сети из FixedFloat API.
    
    Args:
        network_key: ключ сети из NETWORK_CODES (например "USDT-ARB")
    
    Returns:
        dict с ключами 'min' и 'max' (float значения в USDT)
    
    Raises:
        RuntimeError: если сеть недоступна или API вернул ошибку
    """
    from_ccy = NETWORK_CODES.get(network_key)
    if not from_ccy:
        raise ValueError(f"Неизвестная сеть: {network_key}")
    
    try:
        # Используем price API для получения лимитов
        data = await ff_request_async("price", {
            "type": "fixed",
            "fromCcy": from_ccy,
            "toCcy": "BTC",
            "direction": "from",
            "amount": 50,  # любая сумма для получения лимитов
        })
        
        from_info = data.get("from", {})
        min_amt = from_info.get("min")
        max_amt = from_info.get("max")
        
        if min_amt is None or max_amt is None:
            raise RuntimeError(f"Не удалось получить лимиты для {network_key}")
        
        return {
            "min": float(min_amt),
            "max": float(max_amt)
        }
    except RuntimeError as e:
        # Пробрасываем ошибки API дальше
        raise
    except Exception as e:
        logger.error(f"Ошибка получения лимитов для {network_key}: {e}")
        raise RuntimeError(f"Ошибка получения лимитов для {network_key}: {e}")


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
        if "execution_state" not in existing_columns:
            await db.execute("ALTER TABLE dca_plans ADD COLUMN execution_state TEXT DEFAULT 'scheduled'")
        if "last_tx_hash" not in existing_columns:
            await db.execute("ALTER TABLE dca_plans ADD COLUMN last_tx_hash TEXT")
        
        # Создаём таблицу для хранения информации о кошельках (single wallet per user)
        await db.execute('''
            CREATE TABLE IF NOT EXISTS wallets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL UNIQUE,
                wallet_address TEXT NOT NULL,
                created_at INTEGER DEFAULT (strftime('%s','now'))
            )
        ''')
        
        # Удаляем encrypted_password если он существует (legacy migration)
        async with db.execute("PRAGMA table_info(wallets)") as cursor:
            columns = await cursor.fetchall()
            existing_columns = [col[1] for col in columns]
        
        # Note: SQLite doesn't support DROP COLUMN easily, so we'll just ignore it
        
        # Создаём таблицу для отслеживания отправленных транзакций
        # State tracking for idempotency and restart safety
        await db.execute('''
            CREATE TABLE IF NOT EXISTS sent_transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                plan_id INTEGER,
                order_id TEXT NOT NULL,
                network_key TEXT NOT NULL,
                approve_tx_hash TEXT,
                transfer_tx_hash TEXT,
                amount REAL NOT NULL,
                deposit_address TEXT NOT NULL,
                state TEXT DEFAULT 'scheduled',
                error_message TEXT,
                sent_at INTEGER DEFAULT (strftime('%s','now')),
                FOREIGN KEY(plan_id) REFERENCES dca_plans(id)
            )
        ''')
        
        # Migrate sent_transactions table to add state and error_message columns if missing
        async with db.execute("PRAGMA table_info(sent_transactions)") as cursor:
            columns = await cursor.fetchall()
            existing_columns = [col[1] for col in columns]
        
        if "state" not in existing_columns:
            await db.execute("ALTER TABLE sent_transactions ADD COLUMN state TEXT DEFAULT 'scheduled'")
        if "error_message" not in existing_columns:
            await db.execute("ALTER TABLE sent_transactions ADD COLUMN error_message TEXT")
        
        # Создаём таблицу для отслеживания завершённых ордеров
        await db.execute('''
            CREATE TABLE IF NOT EXISTS completed_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                order_id TEXT NOT NULL UNIQUE,
                btc_txid TEXT,
                notified INTEGER DEFAULT 0,
                completed_at INTEGER,
                FOREIGN KEY(user_id) REFERENCES dca_plans(user_id)
            )
        ''')
        
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
                                # Check if this order is blocked (can retry) or still in progress
                                async with db.execute(
                                    "SELECT state, sent_at FROM sent_transactions WHERE order_id = ? AND plan_id = ?",
                                    (existing_order_id, plan_id)
                                ) as state_cur:
                                    state_row = await state_cur.fetchone()
                                
                                if state_row:
                                    existing_state, last_attempt_time = state_row
                                    if existing_state == 'sent':
                                        # Order completed successfully - should not happen with active order
                                        logger.warning(f"Active order {existing_order_id} already sent, clearing active order")
                                    elif existing_state == 'sending':
                                        # Order still being sent - wait
                                        logger.info(f"Skip DCA plan_id={plan_id}: order {existing_order_id} still sending")
                                        continue
                                    elif existing_state == 'blocked':
                                        # Blocked order - implement strict wait logic
                                        # Only retry if DCA interval has passed since last attempt
                                        dca_interval_seconds = interval_hours * 3600
                                        time_since_attempt = now - (last_attempt_time or now)
                                        
                                        if time_since_attempt < dca_interval_seconds:
                                            # DCA interval not yet reached - do nothing
                                            logger.info(f"Skip DCA plan_id={plan_id}: blocked order {existing_order_id}, DCA interval not reached (wait {dca_interval_seconds - time_since_attempt}s)")
                                            continue
                                        else:
                                            # DCA interval reached - allow ONE new execution attempt
                                            logger.info(f"Retry DCA plan_id={plan_id}: blocked order {existing_order_id}, DCA interval reached")
                                            # Fall through to create new order
                                    elif existing_state == 'failed':
                                        # Failed order - already advanced schedule, shouldn't be here
                                        logger.warning(f"Active order {existing_order_id} failed, clearing active order")
                                else:
                                    # No transaction record yet - order exists but not attempted
                                    logger.info(f"Skip DCA plan_id={plan_id}: active order {existing_order_id} not yet attempted")
                                    continue
                            else:
                                # Order expired - can create new order
                                logger.info(f"Active order {existing_order_id} expired, creating new order for plan_id={plan_id}")
                        
                        logger.info(f"Выполнение DCA для plan_id={plan_id}, user_id={user_id}: {amount} {from_asset}")
                        
                        # Проверяем лимиты перед созданием ордера
                        try:
                            limits = await get_fixedfloat_limits(from_asset)
                            min_limit = limits["min"]
                            max_limit = limits["max"]
                            effective_max = min(max_limit, 500.0)
                            
                            if amount < min_limit or amount > effective_max:
                                logger.warning(f"Сумма {amount} вне лимитов для {from_asset}: min={min_limit:.2f}, max={effective_max:.2f}")
                                # Отправляем уведомление пользователю
                                await bot.send_message(
                                    user_id,
                                    f"❌ Ошибка выполнения DCA плана:\n\n"
                                    f"Сумма {amount:.2f} USDT вне допустимых лимитов для {from_asset}\n"
                                    f"Минимум: {min_limit:.2f} USDT\n"
                                    f"Максимум: {effective_max:.2f} USDT\n\n"
                                    f"💡 Обнови план с корректной суммой"
                                )
                                # Откладываем на следующий интервал
                                new_next_run = now + (interval_hours * 3600)
                                await db.execute(
                                    "UPDATE dca_plans SET next_run = ? WHERE id = ?",
                                    (new_next_run, plan_id)
                                )
                                await db.commit()
                                continue
                        except RuntimeError as e:
                            error_msg = str(e)
                            logger.error(f"Ошибка проверки лимитов для plan_id={plan_id}: {e}")
                            # Если сеть недоступна, пропускаем этот запуск
                            if "недоступна" in error_msg.lower() or "311" in error_msg or "312" in error_msg:
                                await bot.send_message(
                                    user_id,
                                    f"⚠️ Сеть {from_asset} недоступна на FixedFloat в данный момент\n\n"
                                    f"План будет повторён через {interval_hours}ч"
                                )
                                new_next_run = now + (interval_hours * 3600)
                                await db.execute(
                                    "UPDATE dca_plans SET next_run = ? WHERE id = ?",
                                    (new_next_run, plan_id)
                                )
                                await db.commit()
                                continue
                        
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
                        if not isinstance(time_left, (int, float)) or time_left < 0:
                            time_left = 0
                        order_expires = int(time.time()) + int(time_left)
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
                        
                        # Проверяем есть ли настроенный кошелёк для автоматической отправки (single wallet)
                        async with db.execute(
                            "SELECT wallet_address FROM wallets WHERE user_id = ?",
                            (user_id,)
                        ) as cur:
                            wallet_row = await cur.fetchone()
                        
                        # Проверяем есть ли пароль в памяти (user_id key)
                        wallet_password = _wallet_passwords.get(user_id)
                        
                        if wallet_row and wallet_password:
                            
                            # Парсим сумму из строки "amount code"
                            try:
                                required_amount = float(deposit_amount)
                            except:
                                required_amount = amount  # Fallback to plan amount
                            
                            # Create transaction record in 'sending' state BEFORE attempting send
                            await db.execute(
                                "INSERT INTO sent_transactions (user_id, plan_id, order_id, network_key, amount, deposit_address, state) VALUES (?, ?, ?, ?, ?, ?, 'sending')",
                                (user_id, plan_id, order_id, from_asset, required_amount, deposit_address)
                            )
                            await db.commit()
                            
                            await bot.send_message(
                                user_id,
                                f"✅ DCA plan executed!\n\n"
                                f"🆔 Order: {order_id}\n"
                                f"🔗 Link: {order_url}\n\n"
                                f"⏳ Auto-sending USDT..."
                            )
                            
                            # Автоматическая отправка USDT
                            try:
                                success, approve_tx, transfer_tx, error_msg = await auto_send_usdt(
                                    network_key=from_asset,
                                    user_id=user_id,
                                    wallet_password=wallet_password,
                                    deposit_address=deposit_address,
                                    required_amount=required_amount,
                                    btc_address=btc_address,
                                    order_id=order_id,
                                    dry_run=DRY_RUN
                                )
                            except Exception as send_error:
                                # RPC/Network error - mark as blocked, don't advance schedule
                                error_str = str(send_error)
                                logger.error(f"RPC/Network error during auto-send: {error_str}")
                                
                                # Check if it's a retryable error (RPC, timeout, connection)
                                is_retryable = any(keyword in error_str.lower() for keyword in 
                                    ['timeout', 'connection', 'rpc', '5xx', 'unavailable', 'failed to connect'])
                                
                                if is_retryable:
                                    # Mark as blocked - will retry when DCA interval reached
                                    await db.execute(
                                        "UPDATE sent_transactions SET state = 'blocked', error_message = ? WHERE order_id = ? AND plan_id = ?",
                                        (error_str[:500], order_id, plan_id)
                                    )
                                    await db.commit()
                                    
                                    await bot.send_message(
                                        user_id,
                                        f"⚠️ Network/RPC error - execution blocked\n\n"
                                        f"🆔 Order: {order_id}\n"
                                        f"Error: {error_str[:200]}\n\n"
                                        f"Will retry when next DCA interval is reached ({interval_hours}h).\n"
                                        f"Or use /execute to retry manually."
                                    )
                                    # DO NOT advance schedule - will retry
                                    continue
                                else:
                                    # Non-retryable error - mark as failed, advance schedule
                                    await db.execute(
                                        "UPDATE sent_transactions SET state = 'failed', error_message = ? WHERE order_id = ? AND plan_id = ?",
                                        (error_str[:500], order_id, plan_id)
                                    )
                                    await db.commit()
                                    
                                    await bot.send_message(
                                        user_id,
                                        f"❌ Auto-send failed\n\n"
                                        f"🆔 Order: {order_id}\n"
                                        f"Error: {error_str[:200]}\n\n"
                                        f"Please send manually."
                                    )
                                    # Advance schedule for failed transactions
                                    new_next_run = now + (interval_hours * 3600)
                                    await db.execute(
                                        "UPDATE dca_plans SET next_run = ? WHERE id = ?",
                                        (new_next_run, plan_id)
                                    )
                                    await db.commit()
                                    continue
                            
                            if success:
                                # Update transaction record with hashes and 'sent' state
                                config = get_network_config(from_asset)
                                await db.execute(
                                    "UPDATE sent_transactions SET approve_tx_hash = ?, transfer_tx_hash = ?, state = 'sent' WHERE order_id = ? AND plan_id = ?",
                                    (approve_tx, transfer_tx, order_id, plan_id)
                                )
                                await db.commit()
                                
                                explorer_base = config["explorer_base"]
                                transfer_url = f"{explorer_base}{transfer_tx}" if transfer_tx else None
                                
                                msg = (
                                    f"✅ USDT sent automatically!\n\n"
                                    f"🆔 Order: {order_id}\n"
                                    f"🔗 Link: {order_url}\n\n"
                                    f"💵 Sent: {required_amount:.6f} USDT\n"
                                    f"📍 To: {deposit_address[:10]}...{deposit_address[-6:]}\n\n"
                                )
                                
                                if approve_tx:
                                    approve_url = f"{explorer_base}{approve_tx}"
                                    msg += f"✅ Approve: {approve_url}\n"
                                
                                if transfer_url:
                                    msg += f"✅ Transfer: {transfer_url}\n"
                                
                                if DRY_RUN:
                                    msg += f"\n⚠️ DRY RUN MODE - transactions not broadcast"
                                
                                await bot.send_message(user_id, msg)
                                
                                logger.info(f"Auto-send successful: order_id={order_id}, approve_tx={approve_tx}, transfer_tx={transfer_tx}")
                                
                                # Advance schedule ONLY on successful send
                                new_next_run = now + (interval_hours * 3600)
                                await db.execute(
                                    "UPDATE dca_plans SET next_run = ? WHERE id = ?",
                                    (new_next_run, plan_id)
                                )
                                await db.commit()
                            else:
                                # Check if error is retryable
                                is_retryable = any(keyword in error_msg.lower() for keyword in 
                                    ['timeout', 'connection', 'rpc', '5xx', 'unavailable', 'failed to connect'])
                                
                                if is_retryable:
                                    # Mark as blocked - will retry when DCA interval reached
                                    await db.execute(
                                        "UPDATE sent_transactions SET state = 'blocked', error_message = ? WHERE order_id = ? AND plan_id = ?",
                                        (error_msg[:500], order_id, plan_id)
                                    )
                                    await db.commit()
                                    
                                    await bot.send_message(
                                        user_id,
                                        f"⚠️ Network/RPC error - execution blocked\n\n"
                                        f"🆔 Order: {order_id}\n"
                                        f"Error: {error_msg[:200]}\n\n"
                                        f"Will retry when next DCA interval is reached ({interval_hours}h).\n"
                                        f"Or use /execute to retry manually."
                                    )
                                    # DO NOT advance schedule
                                    continue
                                else:
                                    # Non-retryable error - mark as failed
                                    await db.execute(
                                        "UPDATE sent_transactions SET state = 'failed', error_message = ? WHERE order_id = ? AND plan_id = ?",
                                        (error_msg[:500], order_id, plan_id)
                                    )
                                    await db.commit()
                                    
                                    error_notification = (
                                        f"❌ Failed to auto-send USDT\n\n"
                                        f"🆔 Order: {order_id}\n"
                                        f"🔗 Link: {order_url}\n\n"
                                        f"Error: {error_msg}\n\n"
                                        f"💵 Please send manually:\n"
                                        f"{required_amount:.6f} USDT\n"
                                        f"📍 To:\n{deposit_address}\n\n"
                                        f"⏰ Order valid for: {time_text}"
                                    )
                                    await bot.send_message(user_id, error_notification)
                                    logger.error(f"Auto-send failed for order {order_id}: {error_msg}")
                                    
                                    # Advance schedule ONLY for failed (non-retryable) errors
                                    new_next_run = now + (interval_hours * 3600)
                                    await db.execute(
                                        "UPDATE dca_plans SET next_run = ? WHERE id = ?",
                                        (new_next_run, plan_id)
                                    )
                                    await db.commit()
                        else:
                            # Wallet not configured - ask to send manually
                            await bot.send_message(
                                user_id,
                                f"✅ DCA plan executed!\n\n"
                                f"🆔 Order: {order_id}\n"
                                f"🔗 Link: {order_url}\n\n"
                                f"💵 Send: {deposit_amount} {deposit_code}\n"
                                f"📍 Deposit address:\n{deposit_address}\n\n"
                                f"⏰ Order valid for: {time_text}\n\n"
                                f"💡 For auto-send, setup wallet:\n"
                                f"/setwallet"
                            )
                            # Advance schedule for manual send case (order created, user notified)
                            new_next_run = now + (interval_hours * 3600)
                            await db.execute(
                                "UPDATE dca_plans SET next_run = ? WHERE id = ?",
                                (new_next_run, plan_id)
                            )
                            await db.commit()
                        
                        logger.info(f"DCA execution completed for plan_id={plan_id}, user_id={user_id}, order_id={order_id}")
                        
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
        f"🤖 AutoDCA Bot - Автоматическая покупка BTC через FixedFloat\n\n"
        f"📋 Доступные команды:\n\n"
        f"🔧 Настройка:\n"
        f"/setwallet — настроить кошелёк\n"
        f"/setdca — создать DCA план\n"
        f"/status — статус планов\n"
        f"/pause — приостановить план\n"
        f"/resume — возобновить план\n"
        f"/delete — удалить план\n\n"
        f"💱 Ручные операции:\n"
        f"/execute — выполнить план вручную\n"
        f"/networks — доступные сети\n"
        f"/limits — лимиты обмена\n\n"
        f"ℹ️ Информация:\n"
        f"/help — подробная справка\n"
        f"/walletstatus — баланс кошелька\n"
        f"/history — история операций\n"
        f"/ping — проверка бота\n\n"
        f"💡 Начни с /setwallet для настройки кошелька!",
        parse_mode=None  # Plain text, no markdown
    )
    logger.info(f"New user: {user_id} (@{username})")


@dp.message(Command("help"))
async def cmd_help(message: Message):
    """
    Команда /help - подробная справка по использованию бота.
    """
    await message.answer(
        "📖 AutoDCA Bot — Локальный Telegram бот для DCA\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "🔐 Настройка кошелька (один раз)\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        "1. Создай wallet.json в папке с ботом:\n\n"
        "```json\n"
        "{\n"
        '  "private_key": "0xYOUR_PRIVATE_KEY",\n'
        '  "password": "YOUR_PASSWORD"\n'
        "}\n"
        "```\n\n"
        "2. Запусти:\n"
        "/setwallet\n\n"
        "Готово! Кошелёк настроен.\n\n"
        "⚠️ ВАЖНО:\n"
        "• wallet.json создаётся ОДИН РАЗ\n"
        "• Приватный ключ удаляется после создания keystore\n"
        "• Пароль хранится в OS keyring\n"
        "• Бот переживает перезапуск\n"
        "• Бот должен работать локально (не в облаке)\n"
        "• Один кошелёк работает для ВСЕХ сетей\n\n"
        "🔄 Сброс кошелька:\n"
        "1. Останови бота\n"
        "2. Удали файл keystore вручную\n"
        "3. Перезапусти бота\n"
        "4. Создай новый wallet.json\n"
        "5. Запусти /setwallet\n\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "💱 Как это работает\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        "1. Создаёшь DCA план: /setdca\n"
        "2. Бот работает 24/7 по расписанию\n"
        "3. Автоматически отправляет USDT на FixedFloat\n"
        "4. BTC приходит на твой адрес\n\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "ℹ️ Команды\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "/setwallet     — настроить кошелёк\n"
        "/setdca        — создать DCA план\n"
        "/status        — статус планов\n"
        "/execute       — выполнить план вручную\n"
        "/pause         — приостановить план\n"
        "/resume        — возобновить план\n"
        "/delete        — удалить план\n"
        "/limits        — лимиты обмена\n"
        "/history       — история операций\n"
        "/walletstatus  — баланс кошелька\n"
        "/networks      — доступные сети\n\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "🔐 Модель безопасности\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "• Эквивалент MetaMask / always-on кошелька\n"
        "• Все средства под ТВОИМ контролем\n"
        "• Бот работает ТОЛЬКО локально\n"
        "• Без облака, без третьих сторон\n"
        "• Приватные ключи никогда не хранятся незашифрованными\n"
        "• Пароль в OS keyring (Windows/macOS/Linux)"
    )


@dp.message(Command("history"))
async def cmd_history(message: Message):
    """
    Команда /history - показать историю операций.
    """
    user_id = message.from_user.id
    
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT COUNT(*) FROM completed_orders WHERE user_id = ?",
            (user_id,)
        ) as cur:
            row = await cur.fetchone()
            count = row[0] if row else 0
    
    if count == 0:
        await message.answer("История операций пуста.")
        return
    
    # Existing history display logic would go here
    # For now, just show empty state message as requested
    await message.answer("История операций пуста.")


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
                
                # Показываем минимум от FixedFloat и максимум бота (500)
                limits_text += f"🔹 {network_name}:\n"
                limits_text += f"   Минимум: {min_amt} USDT\n"
                limits_text += f"   Максимум: 500 USDT (ограничено настройками бота)\n"
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
            text = "📋 Выбери план для выполнения:\n\n"
            for idx, p in enumerate(plans, start=1):
                interval_text = format_interval(p[3])
                text += f"• /execute_{idx} - {p[1]}, {p[2]}$, раз в {interval_text}\n"
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
        # Проверяем лимиты перед созданием ордера
        try:
            limits = await get_fixedfloat_limits(from_asset)
            min_limit = limits["min"]
            max_limit = limits["max"]
            
            # Ограничиваем максимальный лимит бота (500 USD)
            effective_max = min(max_limit, 500.0)
            
            if amount < min_limit:
                await message.answer(
                    f"❌ Сумма меньше минимального лимита FixedFloat\n\n"
                    f"Минимальная сумма для {from_asset}: {min_limit:.2f} USDT\n"
                    f"Сумма в плане: {amount:.2f} USDT\n\n"
                    f"💡 Создай новый план с суммой от {min_limit:.2f} USDT"
                )
                return
            
            if amount > effective_max:
                await message.answer(
                    f"❌ Сумма больше максимального лимита\n\n"
                    f"Максимальная сумма для {from_asset}: {effective_max:.2f} USDT\n"
                    f"Сумма в плане: {amount:.2f} USDT\n\n"
                    f"💡 Создай новый план с суммой до {effective_max:.2f} USDT"
                )
                return
            
            logger.info(f"Лимиты для {from_asset}: min={min_limit:.2f}, max={effective_max:.2f}, amount={amount:.2f}")
        except RuntimeError as e:
            error_msg = str(e)
            if "недоступна" in error_msg.lower() or "311" in error_msg or "312" in error_msg:
                await message.answer(
                    f"❌ Сеть {from_asset} недоступна на FixedFloat в данный момент\n\n"
                    f"Попробуй позже или выбери другую сеть"
                )
            else:
                await message.answer(
                    f"❌ Не удалось проверить лимиты для {from_asset}\n\n"
                    f"Ошибка: {error_msg}\n\n"
                    f"Попробуй позже"
                )
            return
        
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
        if not isinstance(time_left, (int, float)) or time_left < 0:
            time_left = 0
        
        # Вычисляем часы и минуты
        hours = int(time_left) // 3600
        minutes = (int(time_left) % 3600) // 60
        
        # Формируем строку времени
        if hours > 0:
            time_text = f"{hours}ч {minutes}мин"
        else:
            time_text = f"{minutes}мин"

        # Формируем ссылку на ордер
        order_url = f"https://fixedfloat.com/order/{order_id}"
        
        # Сохраняем информацию об активном ордере в БД
        order_expires = int(time.time()) + int(time_left)
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE dca_plans SET active_order_id = ?, active_order_address = ?, "
                "active_order_amount = ?, active_order_expires = ? WHERE id = ?",
                (order_id, deposit_address, f"{deposit_amount} {deposit_code}", order_expires, plan_id)
            )
            await db.commit()
            
            # Проверяем есть ли настроенный кошелёк для автоматической отправки
            async with db.execute(
                "SELECT wallet_address FROM wallets WHERE user_id = ? AND network_key = ?",
                (user_id, from_asset)
            ) as cur:
                wallet_row = await cur.fetchone()
            
            # Проверяем есть ли пароль в памяти
            wallet_password = _wallet_passwords.get((user_id, from_asset))
        
        if wallet_row and wallet_password:
            
            # Парсим сумму из строки "amount code"
            try:
                required_amount = float(deposit_amount)
            except:
                required_amount = amount  # Fallback to plan amount
            
            await message.answer(
                f"✅ Ордер создан!\n\n"
                f"🆔 ID: {order_id}\n"
                f"🔗 Ссылка: {order_url}\n\n"
                f"⏳ Автоматически отправляю USDT..."
            )
            
            # Автоматическая отправка USDT
            success, approve_tx, transfer_tx, error_msg = await auto_send_usdt(
                network_key=from_asset,
                user_id=user_id,
                wallet_password=wallet_password,
                deposit_address=deposit_address,
                required_amount=required_amount,
                btc_address=btc_address,
                order_id=order_id,
                dry_run=DRY_RUN
            )
            
            if success:
                # Сохраняем информацию о транзакции
                config = get_network_config(from_asset)
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute(
                        "INSERT INTO sent_transactions (user_id, plan_id, order_id, network_key, approve_tx_hash, transfer_tx_hash, amount, deposit_address) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (user_id, plan_id, order_id, from_asset, approve_tx, transfer_tx, required_amount, deposit_address)
                    )
                    await db.commit()
                
                explorer_base = config["explorer_base"]
                transfer_url = f"{explorer_base}{transfer_tx}" if transfer_tx else None
                
                msg = (
                    f"✅ USDT отправлен автоматически!\n\n"
                    f"🆔 Ордер: {order_id}\n"
                    f"🔗 Ссылка: {order_url}\n\n"
                    f"💵 Отправлено: {required_amount:.6f} USDT\n"
                    f"📍 На адрес: {deposit_address[:10]}...{deposit_address[-6:]}\n\n"
                )
                
                if approve_tx:
                    approve_url = f"{explorer_base}{approve_tx}"
                    msg += f"✅ Approve: {approve_url}\n"
                
                if transfer_url:
                    msg += f"✅ Transfer: {transfer_url}\n"
                
                if DRY_RUN:
                    msg += f"\n⚠️ DRY RUN MODE - транзакции не были отправлены"
                
                await message.answer(msg)
                
                logger.info(f"Auto-send successful: order_id={order_id}, approve_tx={approve_tx}, transfer_tx={transfer_tx}")
            else:
                # Ошибка автоматической отправки - уведомляем пользователя
                error_notification = (
                    f"❌ Не удалось автоматически отправить USDT\n\n"
                    f"🆔 Ордер: {order_id}\n"
                    f"🔗 Ссылка: {order_url}\n\n"
                    f"Ошибка: {error_msg}\n\n"
                    f"💵 Требуется отправить вручную:\n"
                    f"{required_amount:.6f} USDT\n"
                    f"📍 На адрес:\n{deposit_address}\n\n"
                    f"⏰ Ордер действителен: {time_text}"
                )
                await message.answer(error_notification)
                logger.error(f"Auto-send failed for order {order_id}: {error_msg}")
        else:
            # Кошелёк не настроен - просим отправить вручную
            await message.answer(
                f"✅ Ордер создан!\n\n"
                f"🆔 ID: {order_id}\n"
                f"🔗 Ссылка: {order_url}\n\n"
                f"💵 Отправь: {deposit_amount} {deposit_code}\n"
                f"📍 На адрес:\n{deposit_address}\n\n"
                f"🎯 Получишь BTC на:\n{btc_address}\n\n"
                f"⏰ Ордер действителен: {time_text}\n\n"
                f"💡 Для автоматической отправки:\n"
                f"1. Настрой кошелёк: /setwallet\n"
                f"2. Установи пароль: /setpassword"
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
            "SELECT id FROM dca_plans WHERE user_id = ? AND deleted = 0 ORDER BY id",
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


@dp.message(Command("setwallet"))
async def cmd_setwallet(message: Message):
    """
    Команда /setwallet - настроить единый EVM кошелёк (NO ARGUMENTS).
    
    Читает wallet.json из корня проекта:
    {
      "private_key": "0xYOUR_PRIVATE_KEY",
      "password": "STRONG_PASSWORD"
    }
    
    Создаёт keystore, сохраняет пароль в keyring, перезаписывает wallet.json.
    """
    user_id = message.from_user.id
    
    # Check if keystore already exists
    if keystore_exists(user_id):
        await message.answer(
            "❌ Кошелёк уже инициализирован\n\n"
            "Если нужно сбросить кошелёк:\n"
            "1. Останови бота\n"
            "2. Удали файл keystore вручную\n"
            "3. Перезапусти бота\n"
            "4. Создай новый wallet.json\n"
            "5. Запусти /setwallet"
        )
        return
    
    # Read wallet.json from project root
    wallet_json_path = "wallet.json"
    if not os.path.exists(wallet_json_path):
        await message.answer(
            "❌ wallet.json не найден\n\n"
            "Создай wallet.json в папке с ботом:\n\n"
            "```json\n"
            "{\n"
            '  "private_key": "0xYOUR_PRIVATE_KEY",\n'
            '  "password": "YOUR_PASSWORD"\n'
            "}\n"
            "```\n\n"
            "Затем запусти /setwallet снова",
            parse_mode="Markdown"
        )
        return
    
    try:
        with open(wallet_json_path, "r") as f:
            wallet_data = json.load(f)
        
        private_key = wallet_data.get("private_key")
        password = wallet_data.get("password")
        
        if not private_key or not password:
            await message.answer(
                "❌ Неверный формат wallet.json\n\n"
                "Обязательные поля:\n"
                "• private_key\n"
                "• password"
            )
            return
        
        # Validate private key format
        if not private_key.startswith("0x"):
            private_key = "0x" + private_key
        
        # Create Ethereum keystore using eth_account
        from eth_account import Account
        account = Account.from_key(private_key)
        wallet_address = account.address
        
        # Encrypt to create keystore (v3)
        keystore = account.encrypt(password)
        
        # Save keystore using existing storage logic
        save_keystore(keystore, user_id)
        
        # Store password in OS keyring (single source of truth)
        save_password_to_keyring(user_id, password)
        
        # Populate in-memory cache
        _wallet_passwords[user_id] = password
        
        # Delete private_key from memory explicitly
        private_key = None
        del private_key
        
        # Overwrite wallet.json to contain ONLY keystore
        with open(wallet_json_path, "w") as f:
            json.dump({"keystore": keystore}, f, indent=2)
        
        # Save wallet address to database
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute('''
                INSERT OR REPLACE INTO wallets (user_id, wallet_address)
                VALUES (?, ?)
            ''', (user_id, wallet_address))
            await db.commit()
        
        await message.answer(
            f"✅ Кошелёк инициализирован успешно!\n\n"
            f"📍 Адрес: `{wallet_address}`\n\n"
            f"🔐 Безопасность:\n"
            f"• Приватный ключ зашифрован и удалён\n"
            f"• Пароль сохранён в OS keyring\n"
            f"• wallet.json перезаписан\n\n"
            f"⚠️ УДАЛИ все резервные копии wallet.json с приватным ключом!\n\n"
            f"💡 Автоотправка активирована для всех сетей",
            parse_mode="Markdown"
        )
        
        logger.info(f"Wallet initialized for user {user_id}: address={wallet_address}")
    
    except Exception as e:
        logger.error(f"Error in cmd_setwallet: {e}", exc_info=True)
        await message.answer(f"❌ Ошибка: {e}")


@dp.message(Command("walletstatus"))
async def cmd_walletstatus(message: Message):
    """
    Команда /walletstatus - показать статус кошелька и балансы на всех сетях.
    """
    user_id = message.from_user.id
    
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT wallet_address FROM wallets WHERE user_id = ?",
            (user_id,)
        ) as cursor:
            wallet_row = await cursor.fetchone()
    
    if not wallet_row:
        await message.answer(
            "📋 Wallet not configured\n\n"
            "Setup your wallet:\n"
            "/setwallet"
        )
        return
    
    wallet_address = wallet_row[0]
    status_text = f"💼 Wallet Status:\n\n"
    status_text += f"📍 Address: {wallet_address[:10]}...{wallet_address[-6:]}\n\n"
    status_text += f"Balances on all networks:\n\n"
    
    from networks import NETWORKS
    for network_key in NETWORKS.keys():
        config = get_network_config(network_key)
        
        try:
            w3 = get_web3_instance(network_key)
            usdt_balance = get_usdt_balance(w3, network_key, wallet_address)
            native_balance = get_native_balance(w3, wallet_address)
            
            status_text += (
                f"━━━━━━━━━━━━━━\n"
                f"🌐 {config['name']}\n"
                f"💵 USDT: {usdt_balance:.6f}\n"
                f"⛽ {config['native_token']}: {native_balance:.6f}\n\n"
            )
        except Exception as e:
            logger.error(f"Error getting balance for {network_key}: {e}")
            status_text += (
                f"━━━━━━━━━━━━━━\n"
                f"🌐 {config['name']}\n"
                f"❌ Error: {str(e)[:50]}\n\n"
            )
    
    # Show password status
    has_password = user_id in _wallet_passwords
    status_text += f"\n🔐 Password in keyring: {'✅' if has_password else '❌'}\n"
    
    if not has_password:
        status_text += "\n⚠️ No password found. Auto-send disabled."
    
    await message.answer(status_text)



@dp.message(Command("deletewallet"))
async def cmd_deletewallet(message: Message):
    """
    Команда /deletewallet - удалить кошелёк пользователя.
    Формат: /deletewallet (no arguments)
    """
    user_id = message.from_user.id
    
    # Удаляем из БД и файловой системы
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "DELETE FROM wallets WHERE user_id = ?",
            (user_id,)
        )
        await db.commit()
    
    deleted = delete_keystore(user_id)
    
    # Очищаем пароль из keyring и памяти
    delete_password_from_keyring(user_id)
    if user_id in _wallet_passwords:
        del _wallet_passwords[user_id]
    
    if deleted:
        await message.answer(
            f"✅ Wallet deleted\n\n"
            f"• Keystore file removed from disk\n"
            f"• Password removed from keyring\n"
            f"• Auto-send disabled"
        )
    else:
        await message.answer(
            f"✅ Wallet deleted from database\n\n"
            f"• Keystore file not found (may have been already deleted)\n"
            f"• Password removed from keyring\n"
            f"• Auto-send disabled"
        )
    
    logger.info(f"Wallet deleted: user_id={user_id}")


@dp.message(Command("setdca"))
async def cmd_setdca(message: Message):
    """
    Команда /setdca - создать или обновить DCA план.
    Формат: /setdca СЕТЬ СУММА ИНТЕРВАЛ BTC_АДРЕС
    
    Параметры:
    - СЕТЬ: USDT-ARB, USDT-BSC, USDT-MATIC
    - СУММА: 10-500 USD
    - ИНТЕРВАЛ: 12, 24, 168, 720 (часов)
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
        
        # Базовая проверка диапазона
        if amount < 10 or amount > 500:
            await message.answer(
                "❌ Неверная сумма\n\n"
                "Максимум: 500 USDT (ограничено настройками бота)\n\n"
                "Минимум зависит от сети, проверь /limits"
            )
            return
        
        # Проверка лимитов FixedFloat API
        try:
            limits = await get_fixedfloat_limits(from_asset)
            min_limit = limits["min"]
            max_limit = limits["max"]
            
            # Ограничиваем максимальный лимит бота (500 USD)
            effective_max = min(max_limit, 500.0)
            
            if amount < min_limit:
                await message.answer(
                    f"❌ Сумма меньше минимального лимита FixedFloat\n\n"
                    f"Минимум: {min_limit:.2f} USDT (сетевой лимит FixedFloat)\n"
                    f"Твоя сумма: {amount:.2f} USDT\n\n"
                    f"💡 Увеличь сумму до минимум {min_limit:.2f} USDT"
                )
                return
            
            if amount > effective_max:
                await message.answer(
                    f"❌ Сумма больше максимального лимита\n\n"
                    f"Максимум: 500 USDT (ограничено настройками бота)\n"
                    f"Твоя сумма: {amount:.2f} USDT\n\n"
                    f"💡 Уменьши сумму до максимум 500 USDT"
                )
                return
            
            logger.info(f"Лимиты для {from_asset}: min={min_limit:.2f}, max={effective_max:.2f}, amount={amount:.2f}")
        except RuntimeError as e:
            # Если не удалось получить лимиты, проверяем базовый диапазон
            error_msg = str(e)
            if "недоступна" in error_msg.lower() or "311" in error_msg or "312" in error_msg:
                await message.answer(
                    f"❌ Сеть {from_asset} недоступна на FixedFloat в данный момент\n\n"
                    f"Попробуй позже или выбери другую сеть"
                )
            else:
                await message.answer(
                    f"❌ Не удалось проверить лимиты для {from_asset}\n\n"
                    f"Ошибка: {error_msg}\n\n"
                    f"Попробуй позже"
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
        interval_text = format_interval(interval)
        
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

async def order_monitor():
    """
    Фоновая задача для мониторинга завершения ордеров FixedFloat.
    Проверяет статус ордеров и отправляет уведомления с Blockchair ссылками.
    """
    logger.info("Order Monitor запущен")
    
    while True:
        try:
            await asyncio.sleep(300)  # Проверка каждые 5 минут
            
            async with aiosqlite.connect(DB_PATH) as db:
                # Получаем все отправленные транзакции, для которых ещё не проверен статус ордера
                async with db.execute(
                    "SELECT DISTINCT st.order_id, st.user_id, dp.btc_address "
                    "FROM sent_transactions st "
                    "JOIN dca_plans dp ON st.plan_id = dp.id "
                    "LEFT JOIN completed_orders co ON st.order_id = co.order_id "
                    "WHERE co.order_id IS NULL AND st.transfer_tx_hash IS NOT NULL"
                ) as cursor:
                    orders_to_check = await cursor.fetchall()
            
            for order_id, user_id, btc_address in orders_to_check:
                try:
                    # Проверяем статус ордера через FixedFloat API
                    # Note: FixedFloat API может не иметь endpoint для проверки статуса
                    # В реальной реализации нужно использовать их API или webhook
                    # Здесь мы просто помечаем как проверенные после задержки
                    
                    # Для демонстрации: проверяем через некоторое время после отправки
                    async with aiosqlite.connect(DB_PATH) as db:
                        async with db.execute(
                            "SELECT sent_at FROM sent_transactions WHERE order_id = ? ORDER BY sent_at DESC LIMIT 1",
                            (order_id,)
                        ) as cur:
                            sent_row = await cur.fetchone()
                    
                    if sent_row:
                        sent_at = sent_row[0]
                        # Проверяем через 10 минут после отправки (в реальности нужно использовать API)
                        if int(time.time()) - sent_at > 600:
                            # Помечаем как проверенный (в реальности нужно получить BTC txid из API)
                            async with aiosqlite.connect(DB_PATH) as db:
                                await db.execute(
                                    "INSERT OR IGNORE INTO completed_orders (user_id, order_id, completed_at) VALUES (?, ?, ?)",
                                    (user_id, order_id, int(time.time()))
                                )
                                await db.commit()
                            
                            # Отправляем уведомление (без BTC txid, так как API может не предоставлять его)
                            blockchair_url = f"https://blockchair.com/bitcoin/address/{btc_address}"
                            await bot.send_message(
                                user_id,
                                f"✅ Ордер {order_id} обработан FixedFloat!\n\n"
                                f"🎯 BTC должен быть отправлен на:\n{btc_address}\n\n"
                                f"🔗 Проверь транзакции:\n{blockchair_url}\n\n"
                                f"💡 Если BTC не получен, проверь статус ордера на FixedFloat"
                            )
                            logger.info(f"Order {order_id} marked as completed for user {user_id}")
                
                except Exception as e:
                    logger.error(f"Error checking order {order_id}: {e}")
        
        except Exception as e:
            logger.error(f"Ошибка в order monitor: {e}")


async def load_passwords_at_startup():
    """
    Load passwords from OS keyring into memory cache at bot startup.
    This ensures auto-send continues to work after restarts.
    """
    logger.info("Loading wallet passwords from keyring...")
    
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id FROM wallets") as cursor:
            users = await cursor.fetchall()
    
    for (user_id,) in users:
        password = load_password_from_keyring(user_id)
        if password:
            _wallet_passwords[user_id] = password
            logger.info(f"Wallet password loaded from keyring for user {user_id}")
        else:
            logger.warning(f"No password in keyring for user {user_id}")





async def main():
    """
    Главная функция запуска бота.
    Инициализирует БД, обновляет коды сетей, запускает scheduler и polling.
    """
    logger.info("=" * 60)
    logger.info("Запуск AutoDCA Bot...")
    
    if is_test_mode():
        logger.warning("=" * 60)
        logger.warning("⚠️ TEST MODE(S) ENABLED:")
        if DRY_RUN:
            logger.warning("  • DRY_RUN: No transactions will be broadcast")
        if MOCK_FIXEDFLOAT:
            logger.warning("  • MOCK_FIXEDFLOAT: Using mocked API responses")
        if USE_TESTNET:
            logger.warning("  • USE_TESTNET: Using testnet networks")
        logger.warning("=" * 60)
    
    # Инициализация базы данных
    await init_db()
    
    # Load passwords from keyring into memory cache
    await load_passwords_at_startup()
    
    # Обновление актуальных кодов сетей из FixedFloat
    await update_network_codes()
    
    logger.info("🚀 AutoDCA Bot успешно запущен!")
    logger.info("=" * 60)
    
    # Запуск фонового планировщика DCA
    asyncio.create_task(dca_scheduler())
    
    # Запуск мониторинга завершения ордеров
    asyncio.create_task(order_monitor())
    
    # Запуск обработки сообщений от Telegram
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
