"""
╔══════════════════════════════════════════════════════════════════════╗
║      СТРАТЕГИЯ УМНЫХ ДЕНЕГ — ЛОНГ ИНТРАДЕЙ (MOEX)                  ║
║      Полное сканирование всех акций | .env конфигурация             ║
║      python-telegram-bot v20+ | Python 3.10+                        ║
╚══════════════════════════════════════════════════════════════════════╝

Структура проекта:
  smart_money_moex_strategy.py   ← этот файл
  .env                           ← конфигурация (из .env.example)
  logs/                          ← создаётся автоматически

Установка зависимостей:
  pip install "python-telegram-bot[job-queue]" aiohttp pandas python-dotenv

Запуск:
  python smart_money_moex_strategy.py
"""

# ──────────────────────────────────────────────────────────────────
# ИМПОРТЫ
# ──────────────────────────────────────────────────────────────────

import asyncio
import logging
import logging.handlers
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, time
from pathlib import Path
from typing import Optional

import aiohttp
import pandas as pd
from dotenv import load_dotenv

# ──────────────────────────────────────────────────────────────────
# ЗАГРУЗКА .env
# ──────────────────────────────────────────────────────────────────

load_dotenv()  # ищет .env в текущей директории


def _env(key: str, default, cast=str):
    """Читает переменную из .env с приведением типа."""
    val = os.getenv(key, str(default))
    if cast is bool:
        return val.lower() in ("1", "true", "yes", "on")
    return cast(val)


# ──────────────────────────────────────────────────────────────────
# КОНФИГУРАЦИЯ
# ──────────────────────────────────────────────────────────────────

class Config:
    # Telegram
    BOT_TOKEN: str = _env("BOT_TOKEN", "")
    CHAT_ID:   str = _env("CHAT_ID", "")

    # Сканер
    SCAN_INTERVAL_SEC:   int   = _env("SCAN_INTERVAL_SEC",   300,  int)
    SCAN_TIMEFRAME:      int   = _env("SCAN_TIMEFRAME",      10,   int)   # ISS interval
    SCAN_CANDLES_LIMIT:  int   = _env("SCAN_CANDLES_LIMIT",  60,   int)
    SCAN_MAX_CONCURRENT: int   = _env("SCAN_MAX_CONCURRENT", 10,   int)
    SCAN_BATCH_DELAY:    float = _env("SCAN_BATCH_DELAY_SEC", 1.5, float)

    # SMC параметры
    SWING_LOOKBACK:  int   = _env("SWING_LOOKBACK",   10,   int)
    OB_LOOKBACK:     int   = _env("OB_LOOKBACK",       5,   int)
    FVG_MIN_GAP_PCT: float = _env("FVG_MIN_GAP_PCT",  0.15, float)
    VOLUME_MULT:     float = _env("VOLUME_MULT",       1.5,  float)
    RR_RATIO:        float = _env("RR_RATIO",          2.0,  float)
    SL_ATR_MULT:     float = _env("SL_ATR_MULT",       1.0,  float)
    MIN_CONFIDENCE:  int   = _env("MIN_CONFIDENCE",    55,   int)

    # Временной фильтр
    SESSION_ONLY: bool = _env("SESSION_ONLY", True,    bool)
    PRIME_START:  str  = _env("PRIME_START",  "10:00", str)
    PRIME_END:    str  = _env("PRIME_END",    "12:00", str)

    # Фильтры тикеров
    MIN_PRICE:  float = _env("MIN_PRICE",  10.0,   float)
    MIN_VOLUME: int   = _env("MIN_VOLUME", 100000, int)
    BOARD:      str   = _env("BOARD",      "TQBR", str)

    # Логирование
    LOG_LEVEL: str = _env("LOG_LEVEL", "INFO")
    LOG_FILE:  str = _env("LOG_FILE",  "")

    @classmethod
    def validate(cls):
        errors = []
        if not cls.BOT_TOKEN or "1234567890" in cls.BOT_TOKEN:
            errors.append("BOT_TOKEN не задан в .env")
        if not cls.CHAT_ID:
            errors.append("CHAT_ID не задан в .env")
        if errors:
            for e in errors:
                print(f"❌ Конфигурация: {e}")
            sys.exit(1)

    @classmethod
    def prime_window(cls) -> tuple[time, time]:
        def _t(s):
            h, m = s.split(":")
            return time(int(h), int(m))
        return _t(cls.PRIME_START), _t(cls.PRIME_END)

    @classmethod
    def to_strategy_params(cls) -> dict:
        return {
            "swing_lookback":  cls.SWING_LOOKBACK,
            "ob_lookback":     cls.OB_LOOKBACK,
            "fvg_min_gap_pct": cls.FVG_MIN_GAP_PCT,
            "volume_mult":     cls.VOLUME_MULT,
            "rr_ratio":        cls.RR_RATIO,
            "sl_atr_mult":     cls.SL_ATR_MULT,
            "session_only":    cls.SESSION_ONLY,
        }


# ──────────────────────────────────────────────────────────────────
# ЛОГИРОВАНИЕ
# ──────────────────────────────────────────────────────────────────

def setup_logging():
    level    = getattr(logging, Config.LOG_LEVEL.upper(), logging.INFO)
    fmt      = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    datefmt  = "%Y-%m-%d %H:%M:%S"
    handlers = [logging.StreamHandler()]

    if Config.LOG_FILE:
        log_path = Path(Config.LOG_FILE)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(
            logging.handlers.RotatingFileHandler(
                log_path,
                maxBytes=5 * 1024 * 1024,
                backupCount=3,
                encoding="utf-8",
            )
        )

    logging.basicConfig(level=level, format=fmt, datefmt=datefmt, handlers=handlers)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("telegram").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)


logger = logging.getLogger("smc_bot")


# ──────────────────────────────────────────────────────────────────
# СТРУКТУРЫ ДАННЫХ
# ──────────────────────────────────────────────────────────────────

@dataclass
class SmartMoneySignal:
    ticker:      str
    direction:   str
    entry_price: float
    sl_price:    float
    tp1_price:   float
    tp2_price:   float
    ob_zone:     tuple
    fvg_zone:    tuple
    confidence:  int
    reason:      str
    timestamp:   datetime = field(default_factory=datetime.now)

    def risk_reward(self) -> float:
        risk   = self.entry_price - self.sl_price
        reward = self.tp2_price   - self.entry_price
        return round(reward / risk, 2) if risk > 0 else 0.0

    def to_telegram(self) -> str:
        rr = self.risk_reward()
        if self.confidence >= 80:
            badge = "🔥 *СИЛЬНЫЙ СИГНАЛ*"
        elif self.confidence >= 65:
            badge = "✅ *СИГНАЛ*"
        else:
            badge = "⚠️ *СЛАБЫЙ СИГНАЛ*"

        return (
            f"{badge} | ЛОНГ | *{self.ticker}*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📥 *Вход:*        `{self.entry_price:.2f} ₽`\n"
            f"🛑 *Стоп:*        `{self.sl_price:.2f} ₽`\n"
            f"🎯 *TP1 (1:1):*   `{self.tp1_price:.2f} ₽`\n"
            f"🎯 *TP2 (1:2):*   `{self.tp2_price:.2f} ₽`\n"
            f"📊 *R/R:*         `1:{rr}`\n"
            f"🧠 *Уверенность:* `{self.confidence}%`\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🔍 *Логика:*\n_{self.reason}_\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📦 *Order Block:* `{self.ob_zone[0]:.2f}–{self.ob_zone[1]:.2f}`\n"
            f"📐 *FVG зона:*    `{self.fvg_zone[0]:.2f}–{self.fvg_zone[1]:.2f}`\n"
            f"🕐 `{self.timestamp.strftime('%H:%M %d.%m.%Y')}`\n"
            f"\n⚠️ _Риск не более 1–2% от депозита_"
        )


# ──────────────────────────────────────────────────────────────────
# MOEX ISS API
# ──────────────────────────────────────────────────────────────────

BASE_URL = "https://iss.moex.com/iss"


async def fetch_all_tickers(session: aiohttp.ClientSession) -> list[str]:
    """
    Загружает ВСЕ активные акции режима TQBR с MOEX ISS API.
    Фильтрует по минимальной цене и валидному формату тикера.
    Возвращает фолбэк-список топ-30 при ошибке.
    """
    url = (
        f"{BASE_URL}/engines/stock/markets/shares/boards/{Config.BOARD}"
        f"/securities.json?iss.meta=off&iss.only=securities"
        f"&securities.columns=SECID,PREVPRICE,STATUS"
    )
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            data = await resp.json(content_type=None)

        cols = data["securities"]["columns"]
        rows = data["securities"]["data"]
        df   = pd.DataFrame(rows, columns=cols)

        df = df[df["STATUS"] == "A"]                              # только торгуемые
        df = df[pd.to_numeric(df["PREVPRICE"], errors="coerce") >= Config.MIN_PRICE]
        df = df[df["SECID"].str.match(r"^[A-Z]{4,5}$")]          # нормальные тикеры

        tickers = sorted(df["SECID"].tolist())
        logger.info(f"Загружено {len(tickers)} тикеров с {Config.BOARD}")
        return tickers

    except Exception as e:
        logger.error(f"fetch_all_tickers: {e}. Используется фолбэк-список.")
        return [
            "SBER", "GAZP", "LKOH", "YNDX", "ROSN", "NVTK", "MGNT",
            "POLY", "ALRS", "MTSS", "AFLT", "GMKN", "HYDR", "MOEX",
            "PIKK", "RUAL", "TATN", "SNGS", "NLMK", "MAGN", "CHMF",
            "PHOR", "FEES", "IRAO", "VKCO", "OZON", "TCSG", "FIXP",
            "SGZH", "MVID",
        ]


async def fetch_moex_candles(
    session: aiohttp.ClientSession,
    ticker: str,
    interval: int,
    limit: int,
) -> Optional[pd.DataFrame]:
    """
    Загружает OHLCV-свечи для одного тикера через MOEX ISS API.
    interval: 1=1m, 10=10m, 60=1h, 24=1д
    """
    url = (
        f"{BASE_URL}/engines/stock/markets/shares/boards/{Config.BOARD}"
        f"/securities/{ticker}/candles.json"
        f"?interval={interval}&limit={limit}&iss.meta=off"
    )
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                return None
            data = await resp.json(content_type=None)

        cols = [c["name"] for c in data["candles"]["columns"]]
        rows = data["candles"]["data"]
        if not rows:
            return None

        df = pd.DataFrame(rows, columns=cols)
        df.rename(columns={"begin": "datetime"}, inplace=True)
        df["datetime"] = pd.to_datetime(df["datetime"])
        df.set_index("datetime", inplace=True)
        df = df[["open", "high", "low", "close", "volume"]].astype(float)

        # Фильтр неликвидных свечей
        if df["volume"].iloc[-1] < Config.MIN_VOLUME:
            return None

        return df

    except asyncio.TimeoutError:
        logger.debug(f"Таймаут: {ticker}")
        return None
    except Exception as e:
        logger.debug(f"fetch_candles({ticker}): {e}")
        return None


# ──────────────────────────────────────────────────────────────────
# ЯДРО SMC СТРАТЕГИИ
# ──────────────────────────────────────────────────────────────────

class SmartMoneyStrategy:
    """
    Полная реализация Smart Money Concepts для лонг интрадей.

    Порядок проверок (все обязательны для BOS и OB, остальные — очки):
        1. market_structure  — BOS / CHoCH
        2. find_order_block  — последний OB
        3. find_fvg          — Fair Value Gap
        4. liquidity_sweep   — снятие ликвидности
        5. volume_confirm    — VWAP + VSA
        6. generate_signal   — итоговый сигнал
    """

    def __init__(self, params: dict):
        self.p = params
        ps, pe = Config.prime_window()
        self._prime_start = ps
        self._prime_end   = pe

    # ── 1. СТРУКТУРА РЫНКА ────────────────────────────────────────

    def market_structure(self, df: pd.DataFrame) -> dict:
        n         = self.p["swing_lookback"]
        highs     = df["high"].rolling(n).max()
        lows      = df["low"].rolling(n).min()
        last_high = float(highs.iloc[-2])
        last_low  = float(lows.iloc[-2])
        cur_close = float(df["close"].iloc[-1])

        bullish_bos = cur_close > last_high

        half      = max(n // 2, 2)
        downtrend = float(df["close"].iloc[-n]) > float(df["close"].iloc[-half])
        choch     = downtrend and bullish_bos

        return {
            "bullish_bos": bullish_bos,
            "choch":       choch,
            "last_high":   last_high,
            "last_low":    last_low,
        }

    # ── 2. ORDER BLOCK ────────────────────────────────────────────

    def find_order_block(self, df: pd.DataFrame) -> Optional[dict]:
        lookback = self.p["ob_lookback"]
        avg_vol  = df["volume"].rolling(20).mean()

        for i in range(-lookback - 1, -1):
            try:
                candle = df.iloc[i]
                next_c = df.iloc[i + 1]
            except IndexError:
                continue

            bearish = float(candle["close"]) < float(candle["open"])
            bullish = float(next_c["close"]) > float(next_c["open"])
            vol_ok  = float(next_c["volume"]) > float(avg_vol.iloc[i + 1]) * self.p["volume_mult"]

            if bearish and bullish and vol_ok:
                return {
                    "ob_low":  float(candle["low"]),
                    "ob_high": float(candle["high"]),
                    "ob_idx":  i,
                }
        return None

    # ── 3. FAIR VALUE GAP ─────────────────────────────────────────

    def find_fvg(self, df: pd.DataFrame) -> Optional[dict]:
        min_gap = self.p["fvg_min_gap_pct"] / 100.0

        for i in range(-9, -1):
            try:
                prev_high = float(df["high"].iloc[i - 1])
                curr_low  = float(df["low"].iloc[i + 1])
            except IndexError:
                continue

            if curr_low > prev_high:
                gap_pct = (curr_low - prev_high) / prev_high
                if gap_pct >= min_gap:
                    return {
                        "fvg_low":  prev_high,
                        "fvg_high": curr_low,
                        "gap_pct":  round(gap_pct * 100, 3),
                    }
        return None

    # ── 4. ЛИКВИДНОСТЬ ────────────────────────────────────────────

    def liquidity_sweep(self, df: pd.DataFrame) -> dict:
        lows      = df["low"].tail(20)
        tolerance = float(df["close"].iloc[-1]) * 0.001
        eq_lows   = lows[abs(lows - lows.min()) <= tolerance]
        sweep     = False

        if len(eq_lows) >= 2:
            recent_low  = float(df["low"].iloc[-3:-1].min())
            current_cls = float(df["close"].iloc[-1])
            min_eq      = float(eq_lows.min())
            sweep = recent_low < min_eq and current_cls > min_eq

        return {
            "liquidity_swept":  sweep,
            "equal_lows_price": float(lows.min()),
        }

    # ── 5. ОБЪЁМ + VWAP ───────────────────────────────────────────

    def volume_confirm(self, df: pd.DataFrame) -> dict:
        d2      = df.copy()
        typical = (d2["high"] + d2["low"] + d2["close"]) / 3
        cum_vol = d2["volume"].cumsum()
        vwap    = (d2["volume"] * typical).cumsum() / cum_vol

        vwap_last  = float(vwap.iloc[-1])
        close_last = float(d2["close"].iloc[-1])
        above_vwap = close_last > vwap_last

        avg_vol   = float(d2["volume"].rolling(20).mean().iloc[-1])
        last_vol  = float(d2["volume"].iloc[-1])
        vol_ratio = round(last_vol / avg_vol, 2) if avg_vol > 0 else 0.0
        vol_spike = vol_ratio >= self.p["volume_mult"]

        return {
            "above_vwap": above_vwap,
            "vwap_price": round(vwap_last, 2),
            "vol_spike":  vol_spike,
            "vol_ratio":  vol_ratio,
        }

    # ── ATR ───────────────────────────────────────────────────────

    @staticmethod
    def atr(df: pd.DataFrame, period: int = 14) -> float:
        prev = df["close"].shift(1)
        tr   = pd.concat([
            df["high"] - df["low"],
            (df["high"] - prev).abs(),
            (df["low"]  - prev).abs(),
        ], axis=1).max(axis=1)
        val = tr.rolling(period).mean().iloc[-1]
        return float(val) if pd.notna(val) else 0.0

    # ── ВРЕМЕННОЙ ФИЛЬТР ──────────────────────────────────────────

    def in_prime_window(self, now: datetime) -> bool:
        if not self.p["session_only"]:
            return True
        t = now.time()
        return self._prime_start <= t <= self._prime_end

    # ── ГЛАВНЫЙ МЕТОД ─────────────────────────────────────────────

    def generate_signal(
        self,
        ticker: str,
        df: pd.DataFrame,
        now: Optional[datetime] = None,
    ) -> Optional[SmartMoneySignal]:
        now = now or datetime.now()

        if not self.in_prime_window(now):
            return None

        min_len = max(self.p["swing_lookback"] + 5, 25)
        if len(df) < min_len:
            return None

        # BOS — обязательное условие
        ms = self.market_structure(df)
        if not ms["bullish_bos"]:
            return None

        # OB — обязательное условие
        ob = self.find_order_block(df)
        if ob is None:
            return None

        fvg   = self.find_fvg(df)
        liq   = self.liquidity_sweep(df)
        vol   = self.volume_confirm(df)
        atr_v = self.atr(df)

        if atr_v <= 0:
            return None

        reasons = []
        score   = 0

        reasons.append("✔ Бычий BOS: пробит структурный хай")
        score += 25

        if ms["choch"]:
            reasons.append("✔ CHoCH: подтверждена смена тренда")
            score += 15

        reasons.append(f"✔ Order Block: {ob['ob_low']:.2f}–{ob['ob_high']:.2f}")
        score += 20

        if fvg:
            reasons.append(f"✔ FVG {fvg['gap_pct']}%: {fvg['fvg_low']:.2f}–{fvg['fvg_high']:.2f}")
            score += 15

        if liq["liquidity_swept"]:
            reasons.append("✔ Снятие ликвидности: равные лои поглощены")
            score += 15

        if vol["above_vwap"]:
            reasons.append(f"✔ Цена выше VWAP ({vol['vwap_price']:.2f})")
            score += 5

        if vol["vol_spike"]:
            reasons.append(f"✔ Объёмный всплеск ×{vol['vol_ratio']}")
            score += 5

        if score < Config.MIN_CONFIDENCE:
            return None

        entry = float(df["close"].iloc[-1])
        sl    = entry - atr_v * self.p["sl_atr_mult"]
        risk  = entry - sl
        if risk <= 0:
            return None

        tp1 = entry + risk * 1.0
        tp2 = entry + risk * self.p["rr_ratio"]

        if (tp2 - entry) / risk < self.p["rr_ratio"]:
            return None

        return SmartMoneySignal(
            ticker      = ticker,
            direction   = "LONG",
            entry_price = round(entry, 2),
            sl_price    = round(sl,    2),
            tp1_price   = round(tp1,   2),
            tp2_price   = round(tp2,   2),
            ob_zone     = (ob["ob_low"], ob["ob_high"]),
            fvg_zone    = (fvg["fvg_low"], fvg["fvg_high"]) if fvg else (sl, entry),
            confidence  = min(score, 100),
            reason      = "\n".join(reasons),
            timestamp   = now,
        )


# ──────────────────────────────────────────────────────────────────
# СКАНЕР — ПАРАЛЛЕЛЬНАЯ ОБРАБОТКА ВСЕХ ТИКЕРОВ
# ──────────────────────────────────────────────────────────────────

class MoexScanner:
    """
    Асинхронный сканер всех акций MOEX.

    Алгоритм:
      1. Загружает список тикеров с ISS API (кэш 1 час)
      2. Батчами по SCAN_MAX_CONCURRENT скачивает свечи
      3. Для каждого тикера запускает SmartMoneyStrategy
      4. Возвращает список сигналов, отсортированных по confidence
    """

    def __init__(self):
        self.strategy = SmartMoneyStrategy(Config.to_strategy_params())
        self._tickers: list[str]              = []
        self._tickers_loaded_at: Optional[datetime] = None

    async def get_tickers(self, session: aiohttp.ClientSession) -> list[str]:
        """Кэш тикеров на 1 час — не долбим API при каждом сканировании."""
        now = datetime.now()
        stale = (
            not self._tickers
            or self._tickers_loaded_at is None
            or (now - self._tickers_loaded_at).seconds > 3600
        )
        if stale:
            self._tickers = await fetch_all_tickers(session)
            self._tickers_loaded_at = now
        return self._tickers

    async def _scan_one(
        self,
        session: aiohttp.ClientSession,
        ticker: str,
        now: datetime,
    ) -> Optional[SmartMoneySignal]:
        df = await fetch_moex_candles(
            session, ticker,
            Config.SCAN_TIMEFRAME,
            Config.SCAN_CANDLES_LIMIT,
        )
        if df is None or df.empty:
            return None
        return self.strategy.generate_signal(ticker, df, now=now)

    async def run_scan(self, session: aiohttp.ClientSession) -> list[SmartMoneySignal]:
        """Сканирует все тикеры, возвращает сигналы (лучшие первыми)."""
        tickers = await self.get_tickers(session)
        now     = datetime.now()
        signals: list[SmartMoneySignal] = []

        logger.info(
            f"Сканирование {len(tickers)} тикеров "
            f"| прайм-тайм {Config.PRIME_START}–{Config.PRIME_END} МСК"
        )

        batch_sz = Config.SCAN_MAX_CONCURRENT
        for i in range(0, len(tickers), batch_sz):
            batch   = tickers[i : i + batch_sz]
            tasks   = [self._scan_one(session, t, now) for t in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for ticker, result in zip(batch, results):
                if isinstance(result, Exception):
                    logger.debug(f"Ошибка {ticker}: {result}")
                elif result is not None:
                    signals.append(result)
                    logger.info(
                        f"✅ {result.ticker}: вход={result.entry_price:.2f} "
                        f"confidence={result.confidence}%"
                    )

            # Пауза между батчами — не перегружаем MOEX ISS
            if i + batch_sz < len(tickers):
                await asyncio.sleep(Config.SCAN_BATCH_DELAY)

        signals.sort(key=lambda s: s.confidence, reverse=True)
        logger.info(f"Итог: {len(signals)} сигналов из {len(tickers)} тикеров")
        return signals


# ──────────────────────────────────────────────────────────────────
# TELEGRAM — КОМАНДЫ
# ──────────────────────────────────────────────────────────────────

async def cmd_start(update, context) -> None:
    text = (
        "🧠 *Smart Money MOEX Bot*\n\n"
        "Сканирует *все акции Мосбиржи* и ищет сигналы на *лонг*\n"
        "по методологии *Smart Money Concepts (SMC)*.\n\n"
        "*Команды:*\n"
        "/status — статус бота и кол-во тикеров\n"
        "/signal SBER — сигнал по конкретному тикеру\n"
        "/scan — немедленное сканирование всего рынка\n"
        "/help — описание стратегии\n\n"
        f"⏰ Автосканер: каждые *{Config.SCAN_INTERVAL_SEC // 60} мин*\n"
        f"🕐 Прайм-тайм: *{Config.PRIME_START}–{Config.PRIME_END} МСК*"
    )
    await update.message.reply_text(text, parse_mode="Markdown")


async def cmd_help(update, context) -> None:
    text = (
        "🔍 *Smart Money Concepts — логика стратегии*\n\n"
        "*1️⃣ BOS* — Break of Structure\n"
        "_Пробой последнего структурного хая — рынок бычий_\n\n"
        "*2️⃣ CHoCH* — Change of Character\n"
        "_Смена нисходящего тренда на восходящий_\n\n"
        "*3️⃣ Order Block*\n"
        "_Последняя медвежья свеча перед импульсным ростом_\n\n"
        "*4️⃣ FVG* — Fair Value Gap\n"
        "_Ценовой дисбаланс — зона возврата и притяжения_\n\n"
        "*5️⃣ Liquidity Sweep*\n"
        "_Снос равных минимумов перед реальным движением вверх_\n\n"
        "*6️⃣ VWAP + VSA*\n"
        "_Цена выше VWAP, объём ≥1.5× среднего_\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "*Управление позицией:*\n"
        f"• Стоп-лосс: `{Config.SL_ATR_MULT}×ATR(14)` ниже входа\n"
        f"• TP1 (1:1): закрыть 50% позиции\n"
        f"• TP2 (1:{Config.RR_RATIO}): закрыть 50% позиции\n\n"
        "⚠️ _Риск не более 1–2% депозита на сделку_"
    )
    await update.message.reply_text(text, parse_mode="Markdown")


async def cmd_status(update, context) -> None:
    scanner: MoexScanner = context.bot_data.get("scanner")
    tickers  = scanner._tickers if scanner else []
    now      = datetime.now()
    ps, pe   = Config.prime_window()
    in_prime = ps <= now.time() <= pe

    text = (
        f"📊 *Статус сканера*\n\n"
        f"⏰ Время МСК: `{now.strftime('%H:%M:%S')}`\n"
        f"🟢 Прайм-тайм: `{'да ✅' if in_prime else 'нет ⏸'} ({Config.PRIME_START}–{Config.PRIME_END})`\n"
        f"📋 Тикеров в списке: `{len(tickers)}`\n"
        f"🔄 Интервал сканера: `каждые {Config.SCAN_INTERVAL_SEC // 60} мин`\n"
        f"📈 Режим торгов: `{Config.BOARD}`\n"
        f"💵 Мин. цена: `{Config.MIN_PRICE} ₽`\n"
        f"📦 Мин. объём свечи: `{Config.MIN_VOLUME:,}`\n"
        f"🧠 Мин. уверенность: `{Config.MIN_CONFIDENCE}%`\n"
        f"⚡ Параллельных запросов: `{Config.SCAN_MAX_CONCURRENT}`"
    )
    await update.message.reply_text(text, parse_mode="Markdown")


async def cmd_signal(update, context) -> None:
    if not context.args:
        await update.message.reply_text(
            "Укажите тикер:\n`/signal SBER`", parse_mode="Markdown"
        )
        return

    ticker   = context.args[0].strip().upper()
    strategy = SmartMoneyStrategy(Config.to_strategy_params())
    msg      = await update.message.reply_text(
        f"⏳ Анализирую *{ticker}*...", parse_mode="Markdown"
    )

    async with aiohttp.ClientSession() as session:
        df = await fetch_moex_candles(
            session, ticker, Config.SCAN_TIMEFRAME, Config.SCAN_CANDLES_LIMIT
        )

    if df is None or df.empty:
        await msg.edit_text(
            f"❌ Нет данных для *{ticker}*\n"
            f"Проверьте тикер или попробуйте позже.",
            parse_mode="Markdown",
        )
        return

    signal = strategy.generate_signal(ticker, df)
    if signal:
        await msg.edit_text(signal.to_telegram(), parse_mode="Markdown")
    else:
        now = datetime.now()
        ps, pe = Config.prime_window()
        hint = (
            f"\n\n⚠️ _Вне прайм-тайма {Config.PRIME_START}–{Config.PRIME_END} МСК — "
            f"сигналы не выдаются_"
        ) if not (ps <= now.time() <= pe) else ""
        await msg.edit_text(
            f"⏳ *{ticker}*: условия SMC не выполнены.\n"
            f"_Нет полного набора: BOS + OB + объём ≥{Config.MIN_CONFIDENCE}%_" + hint,
            parse_mode="Markdown",
        )


async def cmd_scan(update, context) -> None:
    scanner: MoexScanner = context.bot_data.get("scanner")
    if not scanner:
        await update.message.reply_text("❌ Сканер не инициализирован")
        return

    ticker_count = len(scanner._tickers) or "?"
    msg = await update.message.reply_text(
        f"🔍 Сканирую *{ticker_count}* акций MOEX...\n"
        f"_Это займёт {Config.SCAN_INTERVAL_SEC // 60 // 2 + 1}–30 сек_",
        parse_mode="Markdown",
    )

    async with aiohttp.ClientSession() as session:
        signals = await scanner.run_scan(session)

    if not signals:
        await msg.edit_text(
            "⏳ Сигналов не найдено.\n"
            "_Условия SMC не выполнены ни по одному тикеру._",
            parse_mode="Markdown",
        )
        return

    await msg.edit_text(
        f"✅ Найдено сигналов: *{len(signals)}*. Отправляю...",
        parse_mode="Markdown",
    )
    for sig in signals:
        await context.bot.send_message(
            chat_id    = update.effective_chat.id,
            text       = sig.to_telegram(),
            parse_mode = "Markdown",
        )
        await asyncio.sleep(0.4)


# ──────────────────────────────────────────────────────────────────
# ДЖОБ АВТОСКАНЕРА
# ──────────────────────────────────────────────────────────────────

async def scanner_job(context) -> None:
    chat_id: str         = context.bot_data.get("chat_id", "")
    scanner: MoexScanner = context.bot_data.get("scanner")

    if not scanner or not chat_id:
        logger.error("scanner_job: нет scanner или chat_id в bot_data")
        return

    try:
        async with aiohttp.ClientSession() as session:
            signals = await scanner.run_scan(session)

        if not signals:
            logger.info("Автосканер: сигналов нет")
            return

        for sig in signals:
            await context.bot.send_message(
                chat_id    = chat_id,
                text       = sig.to_telegram(),
                parse_mode = "Markdown",
            )
            await asyncio.sleep(0.5)

        logger.info(f"Автосканер: отправлено {len(signals)} сигналов")

    except Exception as e:
        logger.error(f"scanner_job: {e}", exc_info=True)


# ──────────────────────────────────────────────────────────────────
# ТОЧКА ВХОДА
# ──────────────────────────────────────────────────────────────────

def main():
    from telegram.ext import ApplicationBuilder, CommandHandler

    setup_logging()
    Config.validate()

    logger.info("=" * 60)
    logger.info("🚀 Smart Money MOEX Bot запускается")
    logger.info(f"   Прайм-тайм:    {Config.PRIME_START}–{Config.PRIME_END} МСК")
    logger.info(f"   Интервал:      каждые {Config.SCAN_INTERVAL_SEC // 60} мин")
    logger.info(f"   Таймфрейм ISS: {Config.SCAN_TIMEFRAME}")
    logger.info(f"   Параллельно:   {Config.SCAN_MAX_CONCURRENT} тикеров")
    logger.info(f"   Min цена:      {Config.MIN_PRICE} ₽")
    logger.info(f"   Min объём:     {Config.MIN_VOLUME:,}")
    logger.info(f"   Chat ID:       {Config.CHAT_ID}")
    logger.info("=" * 60)

    scanner = MoexScanner()

    app = ApplicationBuilder().token(Config.BOT_TOKEN).build()
    app.bot_data["chat_id"] = Config.CHAT_ID
    app.bot_data["scanner"] = scanner

    app.add_handler(CommandHandler("start",  cmd_start))
    app.add_handler(CommandHandler("help",   cmd_help))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("signal", cmd_signal))
    app.add_handler(CommandHandler("scan",   cmd_scan))

    # Автосканер запускается через 5 сек после старта
    app.job_queue.run_repeating(
        scanner_job,
        interval = Config.SCAN_INTERVAL_SEC,
        first    = 5,
    )

    logger.info("Бот запущен. Ctrl+C для остановки.")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
