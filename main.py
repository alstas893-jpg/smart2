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
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ================= CONFIG =================
load_dotenv()

TOKEN = os.getenv("TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

if not TOKEN or not CHAT_ID:
    raise ValueError("TOKEN and CHAT_ID must be set in .env file")

TICKERS = ["SBER", "GAZP", "LKOH", "GMKN", "YNDX"]
INTERVAL = 1
CANDLES = 100
BASE_URL = "https://iss.moex.com/iss/engines/stock/markets/shares/securities"

bot = Bot(token=TOKEN)

# ================= MOEX DATA =================

async def get_candles(session, ticker, retries=3):
    """Получение свечных данных с повторными попытками"""
    url = f"{BASE_URL}/{ticker}/candles.json?interval={INTERVAL}&limit={CANDLES}"
    
    for attempt in range(retries):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200:
                    logger.warning(f"HTTP {r.status} for {ticker} candles")
                    continue
                    
                data = await r.json()
                
                candles = data.get("candles", {}).get("data", [])
                cols = data.get("candles", {}).get("columns", [])
                
                if not candles or not cols:
                    logger.warning(f"Empty data for {ticker}")
                    return None
                
                df = pd.DataFrame(candles, columns=cols)
                
                # Конвертация числовых колонок
                numeric_cols = ["open", "close", "high", "low", "volume"]
                for col in numeric_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # Проверка на достаточное количество данных
                if len(df) < 50:
                    logger.warning(f"Insufficient data for {ticker}: {len(df)} candles")
                    return None
                    
                return df
                
        except asyncio.TimeoutError:
            logger.warning(f"Timeout for {ticker} candles (attempt {attempt + 1}/{retries})")
        except Exception as e:
            logger.error(f"Error fetching candles for {ticker}: {e}")
            if attempt < retries - 1:
                await asyncio.sleep(1 * (attempt + 1))  # Экспоненциальная задержка
    
    return None


async def get_orderbook(session, ticker, retries=3):
    """Получение данных стакана с повторными попытками"""
    url = f"{BASE_URL}/{ticker}/orderbook.json"
    
    for attempt in range(retries):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200:
                    continue
                    
                data = await r.json()
                
                orderbook_data = data.get("orderbook", {}).get("data", [])
                if not orderbook_data:
                    return None, None
                
                bids = orderbook_data[0][2] if len(orderbook_data[0]) > 2 else []
                asks = orderbook_data[0][3] if len(orderbook_data[0]) > 3 else []
                
                bid_vol = sum(b[1] for b in bids) if bids else 0
                ask_vol = sum(a[1] for a in asks) if asks else 0
                
                return bid_vol, ask_vol
                
        except Exception as e:
            logger.error(f"Error fetching orderbook for {ticker}: {e}")
            if attempt < retries - 1:
                await asyncio.sleep(0.5 * (attempt + 1))
    
    return None, None


# ================= STRATEGY =================

def liquidity_grab(df):
    """Определение захвата ликвидности"""
    if len(df) < 21:  # Минимум для расчета rolling(20)
        return None
        
    prev = df.iloc[-2]
    last = df.iloc[-1]
    
    high_lvl = df["high"].rolling(20).max().iloc[-2]
    low_lvl = df["low"].rolling(20).min().iloc[-2]
    
    # Проверка на пробой и возврат
    if prev["high"] > high_lvl and last["close"] < prev["high"]:
        return "SHORT"
    
    if prev["low"] < low_lvl and last["close"] > prev["low"]:
        return "LONG"
    
    return None


def displacement(df):
    """Проверка на расширенное движение"""
    if len(df) < 21:
        return False
        
    last = df.iloc[-1]
    body = abs(last["close"] - last["open"])
    
    avg = (df["close"] - df["open"]).abs().rolling(20).mean().iloc[-1]
    
    if pd.isna(avg) or avg == 0:
        return False
        
    return body > avg * 1.5


def volume_spike(df):
    """Проверка на всплеск объема"""
    if len(df) < 21:
        return False
        
    last_vol = df.iloc[-1]["volume"]
    avg_vol = df["volume"].rolling(20).mean().iloc[-1]
    
    if pd.isna(avg_vol) or avg_vol == 0:
        return False
        
    return last_vol > avg_vol * 1.5


def find_fvg(df):
    """Поиск Fair Value Gap"""
    if len(df) < 3:
        return None
        
    c1, c2, c3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
    
    # Бычий FVG (разрыв вверх)
    if c1["high"] < c3["low"]:
        return (float(c1["high"]), float(c3["low"]))
    
    # Медвежий FVG (разрыв вниз)
    if c1["low"] > c3["high"]:
        return (float(c3["high"]), float(c1["low"]))
    
    return None


def orderbook_bias(bid_vol, ask_vol):
    """Определение смещения в стакане"""
    if bid_vol is None or ask_vol is None:
        return "NEUTRAL"
    
    if ask_vol == 0:
        return "BUY" if bid_vol > 0 else "NEUTRAL"
    
    ratio = bid_vol / ask_vol
    
    if ratio > 1.2:
        return "BUY"
    elif ratio < 0.8:
        return "SELL"
    
    return "NEUTRAL"


def generate_signal(df, bid_vol, ask_vol):
    """Генерация торгового сигнала"""
    if df is None or len(df) < 50:
        return None
    
    side = liquidity_grab(df)
    if not side:
        return None
    
    if not displacement(df):
        return None
    
    if not volume_spike(df):
        return None
    
    fvg = find_fvg(df)
    if not fvg:
        return None
    
    ob_bias = orderbook_bias(bid_vol, ask_vol)
    
    # Проверка соответствия направления
    if side == "LONG" and ob_bias == "SELL":
        return None
    
    if side == "SHORT" and ob_bias == "BUY":
        return None
    
    # Дополнительный фильтр: цена должна быть около FVG
    current_price = df.iloc[-1]["close"]
    fvg_low, fvg_high = min(fvg), max(fvg)
    
    # Цена должна быть недалеко от FVG (в пределах 0.5%)
    if abs(current_price - fvg_low) / fvg_low > 0.005 and \
       abs(current_price - fvg_high) / fvg_high > 0.005:
        return None
    
    return {
        "side": side,
        "price": float(current_price),
        "fvg": fvg,
        "bias": ob_bias,
        "bid_vol": bid_vol or 0,
        "ask_vol": ask_vol or 0
    }


# ================= MAIN =================

async def send_signal(ticker, signal):
    """Отправка сигнала в Telegram с защитой от ошибок"""
    try:
        fvg_low, fvg_high = min(signal['fvg']), max(signal['fvg'])
        
        text = (
            f"📊 {ticker}\n"
            f"Сигнал: {signal['side']}\n"
            f"Цена: {signal['price']:.2f}\n"
            f"FVG: {fvg_low:.2f} - {fvg_high:.2f}\n"
            f"Стакан: {signal['bias']}\n"
            f"BID: {signal['bid_vol']:.0f} | ASK: {signal['ask_vol']:.0f}"
        )
        
        await bot.send_message(chat_id=CHAT_ID, text=text)
        logger.info(f"Signal sent for {ticker}: {signal['side']}")
        
    except TelegramError as e:
        logger.error(f"Telegram error for {ticker}: {e}")
    except Exception as e:
        logger.error(f"Error sending signal for {ticker}: {e}")


async def process_ticker(session, ticker):
    """Обработка одного тикера"""
    try:
        df = await get_candles(session, ticker)
        if df is None:
            return
        
        bid_vol, ask_vol = await get_orderbook(session, ticker)
        
        signal = generate_signal(df, bid_vol, ask_vol)
        
        if signal:
            await send_signal(ticker, signal)
            
    except Exception as e:
        logger.error(f"Error processing {ticker}: {e}")


async def main():
    """Основной цикл"""
    # Создаем сессию с пулом соединений
    connector = aiohttp.TCPConnector(limit=10, limit_per_host=5)
    timeout = aiohttp.ClientTimeout(total=30)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        logger.info("Bot started")
        
        # Проверка подключения к Telegram
        try:
            await bot.send_message(chat_id=CHAT_ID, text="🤖 Бот запущен и мониторит рынок")
        except Exception as e:
            logger.error(f"Cannot send startup message: {e}")
        
        while True:
            try:
                now = time.time()
                
                # Синхронизация с закрытием свечи (INTERVAL минут)
                seconds_to_next = (INTERVAL * 60) - (now % (INTERVAL * 60))
                await asyncio.sleep(seconds_to_next + 1)  # +1 секунда для надёжности
                
                # Обработка всех тикеров параллельно
                tasks = [process_ticker(session, ticker) for ticker in TICKERS]
                await asyncio.gather(*tasks, return_exceptions=True)
                
            except asyncio.CancelledError:
                logger.info("Bot stopped")
                break
            except Exception as e:
                logger.error(f"Main loop error: {e}")
                await asyncio.sleep(60)  # Пауза при ошибке


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.critical(f"Fatal error: {e}")