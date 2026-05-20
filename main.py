```python
import asyncio
import aiohttp
import pandas as pd
import os
import time
import random
import logging

from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes
)

# =====================================================
# CONFIG
# =====================================================

load_dotenv()

TOKEN = os.getenv("TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

if not TOKEN:
    raise ValueError("TOKEN missing")

if not CHAT_ID:
    raise ValueError("CHAT_ID missing")

INTERVAL = 5

TICKERS = [
    "SBER",
    "GAZP",
    "LKOH",
    "GMKN",
    "VTBR",
    "ROSN",
    "TATN",
    "NVTK",
    "PLZL",
    "SNGS"
]

BASE_URL = (
    "https://iss.moex.com/iss/"
    "engines/stock/markets/shares/"
    "boards/TQBR/securities"
)

MSK = timezone(timedelta(hours=3))

SEM = asyncio.Semaphore(3)

# =====================================================
# LOGGING
# =====================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(
            "bot.log",
            encoding="utf-8"
        ),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# =====================================================
# GLOBALS
# =====================================================

start_time = time.time()

cycles = 0
signals_count = 0

last_signals = {}

# =====================================================
# TIME
# =====================================================

def now_msk():

    return datetime.now(MSK)

def market_is_open():

    now = now_msk()

    if now.weekday() >= 5:
        return False

    current = now.hour * 60 + now.minute

    market_open = 10 * 60
    market_close = 23 * 60 + 50

    return market_open <= current <= market_close

# =====================================================
# REQUESTS
# =====================================================

async def fetch_json(
    session,
    url,
    params=None
):

    for attempt in range(3):

        try:

            await asyncio.sleep(
                random.uniform(0.2, 1.0)
            )

            async with session.get(
                url,
                params=params
            ) as response:

                if response.status != 200:

                    logger.warning(
                        f"HTTP {response.status}"
                    )

                    continue

                return await response.json()

        except Exception as e:

            logger.warning(
                f"fetch retry: {e}"
            )

        await asyncio.sleep(1)

    return None

# =====================================================
# CANDLES
# =====================================================

async def get_candles(
    session,
    ticker
):

    try:

        params = {
            "interval": INTERVAL,
            "iss.meta": "off",
            "iss.only": "candles"
        }

        url = (
            f"{BASE_URL}/"
            f"{ticker}/candles.json"
        )

        data = await fetch_json(
            session,
            url,
            params=params
        )

        if not data:

            logger.warning(
                f"{ticker}: no data"
            )

            return None

        candles = data.get(
            "candles",
            {}
        )

        rows = candles.get(
            "data",
            []
        )

        cols = candles.get(
            "columns",
            []
        )

        if not rows:

            logger.warning(
                f"{ticker}: rows empty"
            )

            return None

        df = pd.DataFrame(
            rows,
            columns=cols
        )

        if "begin" in df.columns:

            df["date"] = pd.to_datetime(
                df["begin"]
            )

        elif "end" in df.columns:

            df["date"] = pd.to_datetime(
                df["end"]
            )

        else:

            logger.warning(
                f"{ticker}: no date column"
            )

            return None

        numeric_cols = [
            "open",
            "high",
            "low",
            "close",
            "volume"
        ]

        for col in numeric_cols:

            if col in df.columns:

                df[col] = pd.to_numeric(
                    df[col],
                    errors="coerce"
                )

        df = df.dropna(
            subset=[
                "open",
                "high",
                "low",
                "close"
            ]
        )

        if len(df) < 20:

            logger.warning(
                f"{ticker}: candles < 20"
            )

            return None

        df = (
            df
            .sort_values("date")
            .reset_index(drop=True)
        )

        logger.info(
            f"✅ {ticker} | "
            f"{len(df)} candles | "
            f"last={df.iloc[-1]['close']}"
        )

        return df

    except Exception as e:

        logger.error(
            f"{ticker} candles error: {e}"
        )

    return None

# =====================================================
# ORDERBOOK
# =====================================================

async def get_orderbook(
    session,
    ticker
):

    try:

        url = (
            f"{BASE_URL}/"
            f"{ticker}/orderbook.json"
        )

        params = {
            "depth": 20
        }

        data = await fetch_json(
            session,
            url,
            params=params
        )

        if not data:
            return None, None

        ob = data.get(
            "orderbook",
            {}
        )

        rows = ob.get(
            "data",
            []
        )

        if not rows:
            return None, None

        bids = 0
        asks = 0

        for row in rows:

            try:

                side = row[0]
                volume = row[2]

                if side == "B":
                    bids += volume

                elif side == "S":
                    asks += volume

            except:
                pass

        return bids, asks

    except Exception as e:

        logger.error(
            f"{ticker} orderbook error: {e}"
        )

    return None, None

# =====================================================
# ANALYSIS
# =====================================================

def liquidity_grab(df):

    try:

        high_lvl = (
            df["high"]
            .rolling(20)
            .max()
            .iloc[-2]
        )

        low_lvl = (
            df["low"]
            .rolling(20)
            .min()
            .iloc[-2]
        )

        prev = df.iloc[-2]
        last = df.iloc[-1]

        if (
            prev["high"] > high_lvl and
            last["close"] < prev["high"]
        ):
            return "SHORT"

        if (
            prev["low"] < low_lvl and
            last["close"] > prev["low"]
        ):
            return "LONG"

    except:
        pass

    return None

def displacement(df):

    try:

        body = abs(
            df.iloc[-1]["close"] -
            df.iloc[-1]["open"]
        )

        avg_body = (
            abs(
                df["close"] -
                df["open"]
            )
            .rolling(20)
            .mean()
            .iloc[-1]
        )

        return body > avg_body * 1.2

    except:
        return False

def volume_spike(df):

    try:

        last_vol = df.iloc[-1]["volume"]

        avg_vol = (
            df["volume"]
            .rolling(20)
            .mean()
            .iloc[-1]
        )

        return last_vol > avg_vol * 1.2

    except:
        return False

def fair_value_gap(df):

    try:

        c1 = df.iloc[-3]
        c3 = df.iloc[-1]

        if c1["high"] < c3["low"]:

            return (
                c1["high"],
                c3["low"]
            )

        if c1["low"] > c3["high"]:

            return (
                c3["high"],
                c1["low"]
            )

    except:
        pass

    return None

def orderbook_bias(
    bids,
    asks
):

    if bids is None:
        return "NEUTRAL"

    if asks == 0:
        return "BUY"

    ratio = bids / asks

    if ratio > 1.2:
        return "BUY"

    if ratio < 0.8:
        return "SELL"

    return "NEUTRAL"

# =====================================================
# SIGNAL
# =====================================================

def generate_signal(
    df,
    bids,
    asks
):

    side = liquidity_grab(df)

    if not side:
        return None

    if not displacement(df):
        return None

    if not volume_spike(df):
        return None

    fvg = fair_value_gap(df)

    if not fvg:
        return None

    bias = orderbook_bias(
        bids,
        asks
    )

    if side == "LONG" and bias == "SELL":
        return None

    if side == "SHORT" and bias == "BUY":
        return None

    return {
        "side": side,
        "price": float(
            df.iloc[-1]["close"]
        ),
        "fvg": fvg,
        "bias": bias
    }

# =====================================================
# TELEGRAM
# =====================================================

async def send_signal(
    bot,
    ticker,
    signal
):

    global signals_count

    key = (
        f"{ticker}_"
        f"{signal['side']}"
    )

    if key in last_signals:

        if (
            time.time() -
            last_signals[key]
        ) < 3600:

            return

    emoji = (
        "🟢"
        if signal["side"] == "LONG"
        else "🔴"
    )

    text = (
        f"{emoji} <b>{ticker}</b>\n\n"
        f"📈 Signal: "
        f"<b>{signal['side']}</b>\n"
        f"💵 Price: "
        f"<b>{signal['price']:.2f}</b>\n"
        f"📊 Bias: "
        f"<b>{signal['bias']}</b>\n"
        f"🎯 FVG: "
        f"{signal['fvg'][0]:.2f} - "
        f"{signal['fvg'][1]:.2f}"
    )

    try:

        await bot.send_message(
            chat_id=CHAT_ID,
            text=text,
            parse_mode="HTML"
        )

        last_signals[key] = time.time()

        signals_count += 1

        logger.info(
            f"📤 SIGNAL "
            f"{ticker} "
            f"{signal['side']}"
        )

    except Exception as e:

        logger.error(
            f"telegram error: {e}"
        )

# =====================================================
# PROCESS
# =====================================================

async def process_ticker(
    session,
    ticker,
    bot
):

    async with SEM:

        try:

            df = await get_candles(
                session,
                ticker
            )

            if df is None:
                return

            bids, asks = (
                await get_orderbook(
                    session,
                    ticker
                )
            )

            signal = generate_signal(
                df,
                bids,
                asks
            )

            if signal:

                await send_signal(
                    bot,
                    ticker,
                    signal
                )

        except Exception as e:

            logger.error(
                f"{ticker}: {e}"
            )

# =====================================================
# COMMANDS
# =====================================================

async def status_cmd(
    update,
    context: ContextTypes.DEFAULT_TYPE
):

    uptime = int(
        time.time() - start_time
    )

    text = (
        f"📊 BOT STATUS\n\n"
        f"⏱ Uptime: {uptime}s\n"
        f"🔄 Cycles: {cycles}\n"
        f"📤 Signals: {signals_count}\n"
        f"📈 Tickers: {len(TICKERS)}"
    )

    await update.message.reply_text(
        text
    )

# =====================================================
# SCANNER
# =====================================================

async def scanner(application):

    global cycles

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    timeout = aiohttp.ClientTimeout(
        total=30
    )

    async with aiohttp.ClientSession(
        timeout=timeout,
        headers=headers
    ) as session:

        bot = application.bot

        logger.info("BOT STARTED")

        while True:

            try:

                if not market_is_open():

                    logger.info(
                        "Market closed"
                    )

                    await asyncio.sleep(60)

                    continue

                cycles += 1

                logger.info(
                    f"Cycle #{cycles}"
                )

                tasks = [

                    process_ticker(
                        session,
                        ticker,
                        bot
                    )

                    for ticker in TICKERS
                ]

                await asyncio.gather(
                    *tasks,
                    return_exceptions=True
                )

                logger.info(
                    f"Cycle done | "
                    f"signals={signals_count}"
                )

                await asyncio.sleep(
                    INTERVAL * 60
                )

            except Exception as e:

                logger.error(
                    f"scanner error: {e}"
                )

                await asyncio.sleep(30)

# =====================================================
# MAIN
# =====================================================

async def main():

    app = (
        Application.builder()
        .token(TOKEN)
        .build()
    )

    app.add_handler(
        CommandHandler(
            "status",
            status_cmd
        )
    )

    await app.initialize()

    await app.start()

    await app.updater.start_polling()

    logger.info(
        "Telegram polling started"
    )

    asyncio.create_task(
        scanner(app)
    )

    while True:

        await asyncio.sleep(3600)

# =====================================================
# START
# =====================================================

if __name__ == "__main__":

    try:

        asyncio.run(main())

    except KeyboardInterrupt:

        logger.info("Bot stopped")