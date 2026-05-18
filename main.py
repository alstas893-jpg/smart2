import asyncio
import aiohttp
import pandas as pd
import os
import time
import logging
from telegram import Bot
from telegram.error import TelegramError
from dotenv import load_dotenv

# ================= LOGGING SETUP =================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('trading_bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ================= CONFIG =================
load_dotenv()

TOKEN = os.getenv("TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

if not TOKEN or not CHAT_ID:
    raise ValueError("TOKEN and CHAT_ID must be set in .env file")

# Список тикеров для мониторинга (YNDX заменен на VTBR)
TICKERS = ["SBER", "GAZP", "LKOH", "GMKN", "VTBR", "ROSN", "TATN", "NVTK", "PLZL", "SNGS"]

# Настройки свечей
INTERVAL = 1  # минут
CANDLES = 100  # количество свечей для анализа

# URL для MOEX API
BASE_URL = "https://iss.moex.com/iss/engines/stock/markets/shares"

# Инициализация Telegram бота
bot = Bot(token=TOKEN)

# Словарь для отслеживания последних сигналов (чтобы избежать дублирования)
last_signals = {}

# ================= MOEX API FUNCTIONS =================

async def fetch_json(session, url, retries=3):
    """Универсальная функция для получения JSON с MOEX API с улучшенной обработкой ошибок"""
    headers = {
        'Accept': 'application/json',
        'User-Agent': 'Mozilla/5.0 (compatible; TradingBot/1.0)'
    }
    
    for attempt in range(retries):
        try:
            async with session.get(
                url, 
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=15)
            ) as response:
                # Проверяем HTTP статус
                if response.status == 404:
                    logger.warning(f"Resource not found: {url}")
                    return None
                
                if response.status != 200:
                    logger.warning(f"HTTP {response.status} for {url}")
                    if attempt < retries - 1:
                        await asyncio.sleep(1 * (attempt + 1))
                    continue
                
                # Проверяем content-type
                content_type = response.headers.get('content-type', '')
                
                # Если получили HTML вместо JSON - это не ошибка парсинга, а неверный ответ сервера
                if 'html' in content_type.lower():
                    logger.warning(f"Got HTML instead of JSON from {url} (service might be unavailable)")
                    return None
                
                if 'json' not in content_type.lower():
                    text = await response.text()
                    logger.warning(f"Non-JSON response from {url}: {text[:200]}")
                    
                    # Проверяем на известные ошибки
                    if any(keyword in text.lower() for keyword in ['not found', 'не найдена', 'error', 'ошибка']):
                        return None
                    
                    if attempt < retries - 1:
                        await asyncio.sleep(1 * (attempt + 1))
                    continue
                
                # Парсим JSON
                try:
                    return await response.json()
                except Exception as e:
                    logger.error(f"JSON parse error for {url}: {e}")
                    if attempt < retries - 1:
                        await asyncio.sleep(1 * (attempt + 1))
                    continue
                    
        except asyncio.TimeoutError:
            logger.warning(f"Timeout for {url} (attempt {attempt + 1}/{retries})")
        except aiohttp.ClientError as e:
            logger.error(f"Client error for {url}: {e}")
            if attempt < retries - 1:
                await asyncio.sleep(2 * (attempt + 1))
        except Exception as e:
            logger.error(f"Unexpected error for {url}: {e}")
            if attempt < retries - 1:
                await asyncio.sleep(1 * (attempt + 1))
    
    return None


async def get_candles(session, ticker):
    """Получение и подготовка свечных данных"""
    url = f"{BASE_URL}/securities/{ticker}/candles.json?interval={INTERVAL}&limit={CANDLES}"
    
    data = await fetch_json(session, url)
    if not data:
        return None
    
    try:
        candles = data.get("candles", {}).get("data", [])
        cols = data.get("candles", {}).get("columns", [])
        
        if not candles or not cols:
            logger.warning(f"Empty candles data for {ticker}")
            return None
        
        # Создаем DataFrame
        df = pd.DataFrame(candles, columns=cols)
        
        # Конвертируем числовые колонки
        numeric_cols = ["open", "close", "high", "low", "volume", "value"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Удаляем строки с NaN в критических колонках
        df = df.dropna(subset=["open", "close", "high", "low"])
        
        # Проверяем минимальное количество свечей
        if len(df) < 50:
            logger.warning(f"Insufficient data for {ticker}: {len(df)} candles")
            return None
        
        logger.debug(f"Got {len(df)} candles for {ticker}")
        return df
        
    except Exception as e:
        logger.error(f"Error processing candles for {ticker}: {e}")
        return None


async def get_orderbook(session, ticker):
    """Получение данных стакана заявок с правильной обработкой ошибок и разными URL"""
    
    # Пробуем разные варианты URL для стакана
    urls = [
        f"{BASE_URL}/boards/TQBR/securities/{ticker}/orderbook.json",
        f"{BASE_URL}/securities/{ticker}/orderbook.json",
        f"https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities/{ticker}/orderbook.json",
        f"https://iss.moex.com/iss/engines/stock/markets/shares/securities/{ticker}/orderbook.json"
    ]
    
    for url in urls:
        try:
            data = await fetch_json(session, url)
            
            if not data:
                continue
            
            orderbook_data = data.get("orderbook", {}).get("data", [])
            
            if not orderbook_data or not orderbook_data[0]:
                logger.debug(f"Empty orderbook data from {url}")
                continue
            
            # Извлекаем bids и asks
            row = orderbook_data[0]
            
            # Структура данных в ISS: [timestamp, bids, asks, ...]
            bids = row[2] if len(row) > 2 and row[2] else []
            asks = row[3] if len(row) > 3 and row[3] else []
            
            # Считаем объемы
            bid_vol = sum(b[1] for b in bids if len(b) > 1) if bids else 0
            ask_vol = sum(a[1] for a in asks if len(a) > 1) if asks else 0
            
            if bid_vol > 0 or ask_vol > 0:
                logger.debug(f"Orderbook for {ticker}: BID={bid_vol:.0f}, ASK={ask_vol:.0f} (from {url})")
                return bid_vol, ask_vol
            
        except Exception as e:
            logger.debug(f"Failed to get orderbook from {url}: {e}")
            continue
    
    # Если все попытки неудачны - возвращаем None без ошибки
    logger.debug(f"No orderbook data available for {ticker} (market may be closed or data unavailable)")
    return None, None


# ================= TECHNICAL ANALYSIS =================

def calculate_liquidity_levels(df):
    """Расчет уровней ликвидности"""
    if len(df) < 21:
        return None, None
    
    high_lvl = df["high"].rolling(window=20).max().iloc[-2]
    low_lvl = df["low"].rolling(window=20).min().iloc[-2]
    
    return high_lvl, low_lvl


def detect_liquidity_grab(df):
    """Обнаружение захвата ликвидности"""
    if len(df) < 21:
        return None
    
    prev = df.iloc[-2]
    last = df.iloc[-1]
    
    high_lvl, low_lvl = calculate_liquidity_levels(df)
    
    if high_lvl is None or low_lvl is None:
        return None
    
    # Медвежий захват (пробой максимума и возврат)
    if prev["high"] > high_lvl and last["close"] < prev["high"]:
        logger.debug(f"Liquidity grab detected: SHORT")
        return "SHORT"
    
    # Бычий захват (пробой минимума и возврат)
    if prev["low"] < low_lvl and last["close"] > prev["low"]:
        logger.debug(f"Liquidity grab detected: LONG")
        return "LONG"
    
    return None


def check_displacement(df, multiplier=1.5):
    """Проверка расширенного движения (Displacement)"""
    if len(df) < 21:
        return False
    
    last = df.iloc[-1]
    body = abs(last["close"] - last["open"])
    
    # Средний размер свечи за 20 периодов
    avg_body = (df["close"] - df["open"]).abs().rolling(window=20).mean().iloc[-1]
    
    if pd.isna(avg_body) or avg_body == 0:
        return False
    
    return body > avg_body * multiplier


def check_volume_spike(df, multiplier=1.5):
    """Проверка всплеска объема"""
    if len(df) < 21:
        return False
    
    last_vol = df.iloc[-1]["volume"]
    
    # Средний объем за 20 периодов
    avg_vol = df["volume"].rolling(window=20).mean().iloc[-1]
    
    if pd.isna(avg_vol) or avg_vol == 0:
        return False
    
    return last_vol > avg_vol * multiplier


def find_fair_value_gap(df):
    """Поиск Fair Value Gap (FVG)"""
    if len(df) < 3:
        return None
    
    c1 = df.iloc[-3]  # 3 свечи назад
    c2 = df.iloc[-2]  # 2 свечи назад
    c3 = df.iloc[-1]  # текущая свеча
    
    # Бычий FVG (разрыв вверх)
    if c1["high"] < c3["low"]:
        fvg = (float(c1["high"]), float(c3["low"]))
        logger.debug(f"Bullish FVG found: {fvg}")
        return fvg
    
    # Медвежий FVG (разрыв вниз)
    if c1["low"] > c3["high"]:
        fvg = (float(c3["high"]), float(c1["low"]))
        logger.debug(f"Bearish FVG found: {fvg}")
        return fvg
    
    return None


def analyze_orderbook_bias(bid_vol, ask_vol):
    """Анализ смещения в стакане"""
    if bid_vol is None or ask_vol is None:
        return "NO_DATA"  # Возвращаем специальный статус вместо NEUTRAL
    
    if ask_vol == 0:
        return "STRONG_BUY" if bid_vol > 0 else "NO_DATA"
    
    if bid_vol == 0:
        return "STRONG_SELL" if ask_vol > 0 else "NO_DATA"
    
    ratio = bid_vol / ask_vol
    
    if ratio > 1.3:
        return "STRONG_BUY"
    elif ratio > 1.1:
        return "BUY"
    elif ratio < 0.7:
        return "STRONG_SELL"
    elif ratio < 0.9:
        return "SELL"
    
    return "NEUTRAL"


# ================= SIGNAL GENERATION =================

def generate_trading_signal(df, bid_vol, ask_vol):
    """Генерация торгового сигнала на основе всех факторов"""
    if df is None or len(df) < 50:
        logger.debug("Insufficient data for signal generation")
        return None
    
    # Шаг 1: Захват ликвидности
    side = detect_liquidity_grab(df)
    if not side:
        logger.debug("No liquidity grab detected")
        return None
    
    # Шаг 2: Расширенное движение
    if not check_displacement(df):
        logger.debug(f"No displacement for {side} signal")
        return None
    
    # Шаг 3: Всплеск объема
    if not check_volume_spike(df):
        logger.debug(f"No volume spike for {side} signal")
        return None
    
    # Шаг 4: Fair Value Gap
    fvg = find_fair_value_gap(df)
    if not fvg:
        logger.debug(f"No FVG found for {side} signal")
        return None
    
    # Шаг 5: Анализ стакана (если данные доступны)
    ob_bias = analyze_orderbook_bias(bid_vol, ask_vol)
    
    # Проверка соответствия направления сигнала и стакана
    # Если данных о стакане нет (NO_DATA) - пропускаем проверку
    if ob_bias != "NO_DATA":
        if side == "LONG" and ob_bias in ["SELL", "STRONG_SELL"]:
            logger.debug(f"LONG signal rejected by orderbook bias: {ob_bias}")
            return None
        
        if side == "SHORT" and ob_bias in ["BUY", "STRONG_BUY"]:
            logger.debug(f"SHORT signal rejected by orderbook bias: {ob_bias}")
            return None
    else:
        logger.debug(f"No orderbook data, skipping orderbook bias check")
    
    # Дополнительный фильтр: близость цены к FVG
    current_price = df.iloc[-1]["close"]
    fvg_low, fvg_high = min(fvg), max(fvg)
    
    # Проверяем, что цена находится в пределах 2% от FVG (увеличили с 1% для большей гибкости)
    price_in_range = False
    if fvg_low > 0:
        dist_to_low = abs(current_price - fvg_low) / fvg_low
        dist_to_high = abs(current_price - fvg_high) / fvg_high
        if dist_to_low < 0.02 or dist_to_high < 0.02:
            price_in_range = True
    
    if not price_in_range:
        logger.debug(f"Price too far from FVG: {current_price:.2f}, FVG: {fvg}")
        return None
    
    # Формируем сигнал
    signal = {
        "side": side,
        "price": float(current_price),
        "fvg": fvg,
        "bias": ob_bias if ob_bias != "NO_DATA" else "N/A",
        "bid_vol": bid_vol if bid_vol else 0,
        "ask_vol": ask_vol if ask_vol else 0,
        "volume_ratio": (bid_vol / ask_vol) if (ask_vol and ask_vol > 0) else 0,
        "timestamp": pd.Timestamp.now()
    }
    
    logger.info(f"✅ Signal generated: {side} at price={signal['price']:.2f}, FVG={fvg}")
    return signal


# ================= TELEGRAM NOTIFICATIONS =================

async def send_telegram_signal(ticker, signal):
    """Отправка сигнала в Telegram с форматированием"""
    
    # Проверяем, не отправляли ли мы уже такой сигнал
    signal_key = f"{ticker}_{signal['side']}_{signal['timestamp'].strftime('%Y%m%d_%H%M')}"
    
    if signal_key in last_signals:
        logger.debug(f"Duplicate signal prevented for {ticker}")
        return
    
    try:
        # Форматируем FVG
        fvg_low, fvg_high = min(signal['fvg']), max(signal['fvg'])
        fvg_mid = (fvg_low + fvg_high) / 2
        
        # Эмодзи для направления
        emoji = "🟢" if signal['side'] == "LONG" else "🔴"
        
        # Форматируем объемы
        if signal['bid_vol'] >= 1_000_000:
            bid_str = f"{signal['bid_vol']/1_000_000:.1f}M"
        elif signal['bid_vol'] >= 1_000:
            bid_str = f"{signal['bid_vol']/1_000:.0f}K"
        else:
            bid_str = f"{signal['bid_vol']:.0f}"
            
        if signal['ask_vol'] >= 1_000_000:
            ask_str = f"{signal['ask_vol']/1_000_000:.1f}M"
        elif signal['ask_vol'] >= 1_000:
            ask_str = f"{signal['ask_vol']/1_000:.0f}K"
        else:
            ask_str = f"{signal['ask_vol']:.0f}"
        
        # Формируем сообщение
        message = (
            f"{emoji} <b>{ticker}</b> - <b>{signal['side']}</b>\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"💵 Цена: <b>{signal['price']:.2f}</b>\n"
            f"📊 FVG: <b>{fvg_low:.2f}</b> - <b>{fvg_high:.2f}</b>\n"
            f"🎯 Mid FVG: <b>{fvg_mid:.2f}</b>\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"📈 Стакан: <b>{signal['bias']}</b>\n"
            f"🟢 BID: <b>{bid_str}</b> | 🔴 ASK: <b>{ask_str}</b>\n"
            f"📊 Ratio: <b>{signal['volume_ratio']:.2f}</b>\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"⏰ {signal['timestamp'].strftime('%H:%M:%S')}"
        )
        
        await bot.send_message(
            chat_id=CHAT_ID,
            text=message,
            parse_mode='HTML'
        )
        
        # Сохраняем сигнал в историю
        last_signals[signal_key] = time.time()
        
        # Очищаем старые сигналы (старше 1 часа)
        current_time = time.time()
        old_keys = [k for k, v in last_signals.items() if current_time - v > 3600]
        for k in old_keys:
            del last_signals[k]
        
        logger.info(f"📤 Signal sent to Telegram: {ticker} {signal['side']}")
        
    except TelegramError as e:
        logger.error(f"Telegram error: {e}")
    except Exception as e:
        logger.error(f"Error sending signal: {e}")


# ================= MAIN PROCESSING =================

async def process_ticker(session, ticker):
    """Обработка одного тикера"""
    try:
        logger.debug(f"Processing {ticker}...")
        
        # Получаем данные
        df = await get_candles(session, ticker)
        if df is None:
            logger.debug(f"{ticker}: No candle data")
            return
        
        bid_vol, ask_vol = await get_orderbook(session, ticker)
        
        # Генерируем сигнал
        signal = generate_trading_signal(df, bid_vol, ask_vol)
        
        if signal:
            await send_telegram_signal(ticker, signal)
        else:
            logger.debug(f"No signal for {ticker}")
            
    except Exception as e:
        logger.error(f"Error processing {ticker}: {e}", exc_info=True)


async def health_check():
    """Проверка работоспособности бота"""
    try:
        tickers_str = ", ".join(TICKERS[:5]) + ("..." if len(TICKERS) > 5 else "")
        await bot.send_message(
            chat_id=CHAT_ID,
            text="✅ Бот запущен и мониторит рынок\n"
                 f"📊 Тикеры ({len(TICKERS)}): {tickers_str}\n"
                 f"⏱ Интервал: {INTERVAL} мин\n"
                 f"🔧 Orderbook: авто-восстановление"
        )
        logger.info("Health check message sent")
    except Exception as e:
        logger.error(f"Health check failed: {e}")


async def main_loop():
    """Основной цикл мониторинга"""
    
    # Настройка HTTP клиента
    connector = aiohttp.TCPConnector(
        limit=10,
        limit_per_host=5,
        ttl_dns_cache=300,
        enable_cleanup_closed=True
    )
    
    timeout = aiohttp.ClientTimeout(
        total=30,
        connect=10,
        sock_read=10
    )
    
    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout
    ) as session:
        
        logger.info("=" * 50)
        logger.info("🚀 Trading bot starting...")
        logger.info(f"📊 Monitoring {len(TICKERS)} tickers: {', '.join(TICKERS)}")
        logger.info(f"⏱ Interval: {INTERVAL} min")
        logger.info("=" * 50)
        
        # Отправляем сообщение о запуске
        await health_check()
        
        # Основной цикл
        while True:
            try:
                # Синхронизация с закрытием свечи
                now = time.time()
                seconds_to_next_candle = (INTERVAL * 60) - (now % (INTERVAL * 60))
                
                # Добавляем 1 секунду для надежности
                wait_time = max(seconds_to_next_candle + 1, 1)
                
                logger.info(f"⏳ Waiting {wait_time:.0f}s until next candle close...")
                await asyncio.sleep(wait_time)
                
                logger.info(f"🔄 Processing all tickers...")
                
                # Обрабатываем все тикеры параллельно
                tasks = [process_ticker(session, ticker) for ticker in TICKERS]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Логируем ошибки
                for ticker, result in zip(TICKERS, results):
                    if isinstance(result, Exception):
                        logger.error(f"Task for {ticker} failed: {result}")
                
                logger.debug("All tickers processed")
                
            except asyncio.CancelledError:
                logger.info("Bot cancelled")
                break
                
            except KeyboardInterrupt:
                logger.info("Bot stopped by user")
                break
                
            except Exception as e:
                logger.error(f"Main loop error: {e}", exc_info=True)
                await asyncio.sleep(60)  # Пауза при серьезной ошибке


# ================= ENTRY POINT =================

if __name__ == "__main__":
    try:
        logger.info("Starting trading bot...")
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.critical(f"Fatal error: {e}", exc_info=True)
    finally:
        logger.info("Bot shutdown complete")