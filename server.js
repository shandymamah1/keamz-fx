const express = require("express");
const WebSocket = require("ws");

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;

const DEFAULT_SYMBOL = "stpRNG";

const DERIV_WS =
  "wss://api.derivws.com/trading/v1/options/ws/public";

const HISTORY_BATCHES = 12;
const TICKS_PER_BATCH = 1000;

// ============================================================
// BASIC HELPERS
// ============================================================

function avg(arr) {
  if (!arr.length) return 0;
  return arr.reduce((a, b) => a + b, 0) / arr.length;
}

function clamp(v, min, max) {
  return Math.max(min, Math.min(max, v));
}

function round(v, decimals = 2) {
  if (v === null || v === undefined || !Number.isFinite(v)) {
    return null;
  }

  const p = Math.pow(10, decimals);
  return Math.round(v * p) / p;
}

function normalizeSymbol(symbol) {
  if (!symbol) return DEFAULT_SYMBOL;

  return String(symbol)
    .trim()
    .replace(/[^a-zA-Z0-9_]/g, "")
    .slice(0, 30) || DEFAULT_SYMBOL;
}

// ============================================================
// DERIV HISTORY
// ============================================================

function getDerivHistoryBatch(symbol, endTime) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(DERIV_WS);

    let finished = false;

    const timeout = setTimeout(() => {
      if (!finished) {
        finished = true;
        try {
          ws.close();
        } catch (e) {}
        reject(new Error("Deriv WebSocket timeout"));
      }
    }, 20000);

    function finishError(err) {
      if (finished) return;

      finished = true;
      clearTimeout(timeout);

      try {
        ws.close();
      } catch (e) {}

      reject(err);
    }

    ws.on("open", () => {
      const request = {
        ticks_history: symbol,
        end: endTime || "latest",
        count: TICKS_PER_BATCH,
        style: "ticks",
        adjust_start_time: 1
      };

      ws.send(JSON.stringify(request));
    });

    ws.on("message", message => {
      try {
        const data = JSON.parse(message.toString());

        if (data.error) {
          finishError(
            new Error(data.error.message || "Deriv API error")
          );
          return;
        }

        if (data.history && data.history.prices) {
          const prices = data.history.prices;
          const times = data.history.times || [];

          const ticks = [];

          for (let i = 0; i < prices.length; i++) {
            ticks.push({
              time: Number(times[i]),
              price: Number(prices[i])
            });
          }

          finished = true;
          clearTimeout(timeout);

          try {
            ws.close();
          } catch (e) {}

          resolve(ticks);
        }
      } catch (err) {
        finishError(err);
      }
    });

    ws.on("error", err => {
      finishError(err);
    });

    ws.on("close", () => {
      if (!finished) {
        finishError(new Error("Deriv WebSocket closed"));
      }
    });
  });
}

async function getDerivTicks(symbol, batches = HISTORY_BATCHES) {
  let allTicks = [];
  let endTime = "latest";

  for (let i = 0; i < batches; i++) {
    const batch = await getDerivHistoryBatch(symbol, endTime);

    if (!batch.length) break;

    allTicks = allTicks.concat(batch);

    const oldest = batch[0];

    if (!oldest || !oldest.time) break;

    endTime = oldest.time - 1;

    if (batch.length < TICKS_PER_BATCH) {
      break;
    }
  }

  const unique = new Map();

  for (const tick of allTicks) {
    if (
      tick &&
      Number.isFinite(tick.time) &&
      Number.isFinite(tick.price)
    ) {
      unique.set(`${tick.time}_${tick.price}`, tick);
    }
  }

  return Array.from(unique.values())
    .sort((a, b) => a.time - b.time);
}

// ============================================================
// CANDLE BUILDER
// ============================================================

function buildCandles(ticks, seconds) {
  const candles = [];
  const buckets = new Map();

  for (const tick of ticks) {
    if (!Number.isFinite(tick.time)) continue;

    const bucket =
      Math.floor(tick.time / seconds) * seconds;

    if (!buckets.has(bucket)) {
      buckets.set(bucket, {
        time: bucket,
        open: tick.price,
        high: tick.price,
        low: tick.price,
        close: tick.price
      });
    } else {
      const c = buckets.get(bucket);

      c.high = Math.max(c.high, tick.price);
      c.low = Math.min(c.low, tick.price);
      c.close = tick.price;
    }
  }

  for (const candle of buckets.values()) {
    candles.push(candle);
  }

  return candles.sort((a, b) => a.time - b.time);
}

// ============================================================
// EMA
// ============================================================

function ema(values, period) {
  if (!values.length) return [];

  const result = [];

  const multiplier = 2 / (period + 1);

  let previous = values[0];

  result.push(previous);

  for (let i = 1; i < values.length; i++) {
    previous =
      (values[i] - previous) * multiplier + previous;

    result.push(previous);
  }

  return result;
}

// ============================================================
// WILDER RSI
// ============================================================

function rsi(values, period = 14) {
  if (values.length <= period) {
    return null;
  }

  let gains = 0;
  let losses = 0;

  for (let i = 1; i <= period; i++) {
    const change = values[i] - values[i - 1];

    if (change >= 0) {
      gains += change;
    } else {
      losses += Math.abs(change);
    }
  }

  let avgGain = gains / period;
  let avgLoss = losses / period;

  let currentRSI;

  if (avgLoss === 0) {
    currentRSI = 100;
  } else {
    const rs = avgGain / avgLoss;
    currentRSI = 100 - 100 / (1 + rs);
  }

  for (let i = period + 1; i < values.length; i++) {
    const change = values[i] - values[i - 1];

    const gain = change > 0 ? change : 0;
    const loss = change < 0 ? Math.abs(change) : 0;

    avgGain =
      (avgGain * (period - 1) + gain) / period;

    avgLoss =
      (avgLoss * (period - 1) + loss) / period;

    if (avgLoss === 0) {
      currentRSI = 100;
    } else {
      const rs = avgGain / avgLoss;
      currentRSI = 100 - 100 / (1 + rs);
    }
  }

  return currentRSI;
}

// ============================================================
// ATR
// ============================================================

function atr(candles, period = 14) {
  if (candles.length <= period) return null;

  const trs = [];

  for (let i = 1; i < candles.length; i++) {
    const current = candles[i];
    const previous = candles[i - 1];

    const tr = Math.max(
      current.high - current.low,
      Math.abs(current.high - previous.close),
      Math.abs(current.low - previous.close)
    );

    trs.push(tr);
  }

  if (trs.length < period) return null;

  let value = avg(trs.slice(0, period));

  for (let i = period; i < trs.length; i++) {
    value =
      (value * (period - 1) + trs[i]) / period;
  }

  return value;
}

// ============================================================
// MARKET TREND
// ============================================================

function getTrend(candles) {
  if (candles.length < 30) {
    return {
      direction: "RANGE",
      strength: 0
    };
  }

  const closes = candles.map(c => c.close);

  const fast = ema(closes, 9);
  const slow = ema(closes, 21);

  const fastNow = fast[fast.length - 1];
  const slowNow = slow[slow.length - 1];
  const price = closes[closes.length - 1];

  const currentATR = atr(candles, 14) || 0;

  let direction = "RANGE";

  if (
    fastNow > slowNow &&
    price >= fastNow
  ) {
    direction = "BULLISH";
  } else if (
    fastNow < slowNow &&
    price <= fastNow
  ) {
    direction = "BEARISH";
  }

  let strength = 0;

  if (currentATR > 0) {
    strength =
      Math.abs(fastNow - slowNow) /
      currentATR *
      100;
  }

  return {
    direction,
    strength: round(clamp(strength, 0, 100), 0)
  };
}

// ============================================================
// STRUCTURE
// ============================================================

function getStructure(candles) {
  if (candles.length < 15) {
    return {
      direction: "RANGE",
      bos: false,
      bosDirection: "NONE"
    };
  }

  const recent = candles.slice(-35);

  const swingHighs = [];
  const swingLows = [];

  for (let i = 2; i < recent.length - 2; i++) {
    const c = recent[i];

    if (
      c.high > recent[i - 1].high &&
      c.high > recent[i - 2].high &&
      c.high >= recent[i + 1].high &&
      c.high >= recent[i + 2].high
    ) {
      swingHighs.push(c.high);
    }

    if (
      c.low < recent[i - 1].low &&
      c.low < recent[i - 2].low &&
      c.low <= recent[i + 1].low &&
      c.low <= recent[i + 2].low
    ) {
      swingLows.push(c.low);
    }
  }

  const latest = recent[recent.length - 1];
  const previous = recent[recent.length - 2];

  const currentATR = atr(recent, 14) || 0;
  const buffer = currentATR * 0.06;

  let bos = false;
  let bosDirection = "NONE";

  const latestHigh =
    swingHighs.length
      ? swingHighs[swingHighs.length - 1]
      : null;

  const latestLow =
    swingLows.length
      ? swingLows[swingLows.length - 1]
      : null;

  if (
    latestHigh !== null &&
    latest.close > latestHigh + buffer &&
    previous.close <= latestHigh
  ) {
    bos = true;
    bosDirection = "BULLISH";
  }

  if (
    latestLow !== null &&
    latest.close < latestLow - buffer &&
    previous.close >= latestLow
  ) {
    bos = true;
    bosDirection = "BEARISH";
  }

  const lookback = Math.min(8, recent.length - 1);

  const start =
    recent[recent.length - 1 - lookback].close;

  const end = latest.close;

  let direction = "RANGE";

  if (end > start) {
    direction = "BULLISH";
  } else if (end < start) {
    direction = "BEARISH";
  }

  return {
    direction,
    bos,
    bosDirection
  };
}

// ============================================================
// LIQUIDITY SWEEP
// ============================================================

function detectLiquiditySweep(candles) {
  if (candles.length < 10) {
    return {
      direction: "NONE",
      strength: 0,
      age: null
    };
  }

  const recent = candles.slice(-8);

  let result = {
    direction: "NONE",
    strength: 0,
    age: null
  };

  for (let i = 1; i < recent.length; i++) {
    const c = recent[i];

    const previousCandles =
      recent.slice(Math.max(0, i - 4), i);

    if (!previousCandles.length) continue;

    const previousHigh = Math.max(
      ...previousCandles.map(x => x.high)
    );

    const previousLow = Math.min(
      ...previousCandles.map(x => x.low)
    );

    const range =
      Math.max(c.high - c.low, 0.00000001);

    // Buy-side liquidity sweep
    if (
      c.high > previousHigh &&
      c.close < previousHigh
    ) {
      const wick =
        c.high - Math.max(c.open, c.close);

      const strength =
        clamp((wick / range) * 100, 0, 100);

      result = {
        direction: "BUY-SIDE SWEEP",
        strength: round(strength, 0),
        age: recent.length - 1 - i
      };
    }

    // Sell-side liquidity sweep
    if (
      c.low < previousLow &&
      c.close > previousLow
    ) {
      const wick =
        Math.min(c.open, c.close) - c.low;

      const strength =
        clamp((wick / range) * 100, 0, 100);

      result = {
        direction: "SELL-SIDE SWEEP",
        strength: round(strength, 0),
        age: recent.length - 1 - i
      };
    }
  }

  return result;
}

// ============================================================
// FAIR VALUE GAP
// ============================================================

function detectFVG(candles) {
  if (candles.length < 5) {
    return {
      direction: "NONE",
      strength: 0,
      age: null,
      active: false
    };
  }

  const recent = candles.slice(-12);

  let result = {
    direction: "NONE",
    strength: 0,
    age: null,
    active: false
  };

  for (let i = 2; i < recent.length; i++) {
    const left = recent[i - 2];
    const middle = recent[i - 1];
    const right = recent[i];

    // Bullish FVG
    if (right.low > left.high) {
      const gap = right.low - left.high;

      const range =
        Math.max(middle.high - middle.low, 0.00000001);

      const strength =
        clamp((gap / range) * 100, 0, 100);

      const current =
        recent[recent.length - 1].close;

      const active = current >= left.high;

      result = {
        direction: "BULLISH FVG",
        strength: round(strength, 0),
        age: recent.length - 1 - i,
        active
      };
    }

    // Bearish FVG
    if (right.high < left.low) {
      const gap = left.low - right.high;

      const range =
        Math.max(middle.high - middle.low, 0.00000001);

      const strength =
        clamp((gap / range) * 100, 0, 100);

      const current =
        recent[recent.length - 1].close;

      const active = current <= left.low;

      result = {
        direction: "BEARISH FVG",
        strength: round(strength, 0),
        age: recent.length - 1 - i,
        active
      };
    }
  }

  return result;
}

// ============================================================
// MOMENTUM
// ============================================================

function getMomentum(candles) {
  if (candles.length < 6) {
    return {
      direction: "NEUTRAL",
      strength: 0
    };
  }

  const recent = candles.slice(-5);

  let bullish = 0;
  let bearish = 0;

  for (const c of recent) {
    const body =
      Math.abs(c.close - c.open);

    const range =
      Math.max(c.high - c.low, 0.00000001);

    const bodyRatio = body / range;

    if (
      c.close > c.open &&
      bodyRatio >= 0.5
    ) {
      bullish++;
    }

    if (
      c.close < c.open &&
      bodyRatio >= 0.5
    ) {
      bearish++;
    }
  }

  let direction = "NEUTRAL";

  if (bullish >= 3 && bullish > bearish) {
    direction = "BULLISH";
  } else if (
    bearish >= 3 &&
    bearish > bullish
  ) {
    direction = "BEARISH";
  }

  const strength =
    clamp(Math.abs(bullish - bearish) * 20, 0, 100);

  return {
    direction,
    strength
  };
}

// ============================================================
// RSI CONTEXT
// ============================================================

function getRSIContext(candles) {
  const closes = candles.map(c => c.close);

  const currentRSI = rsi(closes, 14);

  if (currentRSI === null) {
    return {
      value: null,
      context: "NEUTRAL",
      recovery: false,
      recoveryDirection: "NONE"
    };
  }

  const previousRSI =
    rsi(closes.slice(0, -1), 14);

  let context = "NEUTRAL";

  if (currentRSI < 30) {
    context = "OVERSOLD";
  } else if (currentRSI > 70) {
    context = "OVERBOUGHT";
  }

  let recovery = false;
  let recoveryDirection = "NONE";

  if (
    previousRSI !== null &&
    previousRSI < 35 &&
    currentRSI >= previousRSI + 1.5
  ) {
    recovery = true;
    recoveryDirection = "BULLISH";
  }

  if (
    previousRSI !== null &&
    previousRSI > 65 &&
    currentRSI <= previousRSI - 1.5
  ) {
    recovery = true;
    recoveryDirection = "BEARISH";
  }

  return {
    value: round(currentRSI, 2),
    context,
    recovery,
    recoveryDirection
  };
}

// ============================================================
// EMA STATE
// ============================================================

function getEMAState(candles) {
  if (candles.length < 25) {
    return "Neutral";
  }

  const closes = candles.map(c => c.close);

  const fast = ema(closes, 9);
  const slow = ema(closes, 21);

  const f = fast[fast.length - 1];
  const s = slow[slow.length - 1];

  if (f > s) {
    return "Bullish";
  }

  if (f < s) {
    return "Bearish";
  }

  return "Neutral";
}

// ============================================================
// BIAS SCORE
// ============================================================

function getBiasScore(
  trend5m,
  structure1m,
  momentum10s,
  ema1m
) {
  let buyBias = 0;
  let sellBias = 0;

  if (trend5m.direction === "BULLISH") {
    buyBias += 35;
  }

  if (trend5m.direction === "BEARISH") {
    sellBias += 35;
  }

  if (structure1m.direction === "BULLISH") {
    buyBias += 30;
  }

  if (structure1m.direction === "BEARISH") {
    sellBias += 30;
  }

  if (momentum10s.direction === "BULLISH") {
    buyBias += 20;
  }

  if (momentum10s.direction === "BEARISH") {
    sellBias += 20;
  }

  if (ema1m === "Bullish") {
    buyBias += 15;
  }

  if (ema1m === "Bearish") {
    sellBias += 15;
  }

  let biasDirection = "RANGE";

  if (
    buyBias >= 60 &&
    buyBias > sellBias + 10
  ) {
    biasDirection = "BULLISH";
  } else if (
    sellBias >= 60 &&
    sellBias > buyBias + 10
  ) {
    biasDirection = "BEARISH";
  }

  const biasStrength =
    Math.max(buyBias, sellBias);

  return {
    biasDirection,
    biasStrength,
    buyBias,
    sellBias
  };
}

// ============================================================
// DEVELOPING SETUP - V3.2
// ============================================================

function detectDevelopingSetup(
  trend5m,
  structure1m,
  momentum10s,
  fvg,
  liquidity
) {
  /*
    V3.2 TERMINOLOGY RULE:

    5M BULLISH + bullish 1M conditions
      -> BULLISH CONTINUATION DEVELOPING

    5M BEARISH + bearish 1M conditions
      -> BEARISH CONTINUATION DEVELOPING

    5M RANGE + bullish 1M conditions
      -> BULLISH SETUP DEVELOPING

    5M RANGE + bearish 1M conditions
      -> BEARISH SETUP DEVELOPING

    Opposite 5M direction is NOT called continuation.
  */

  // ----------------------------------------------------------
  // BULLISH CONDITIONS
  // ----------------------------------------------------------

  let bullishScore = 0;

  if (structure1m.direction === "BULLISH") {
    bullishScore += 2;
  }

  if (momentum10s.direction === "BULLISH") {
    bullishScore += 1;
  }

  if (
    fvg.direction === "BULLISH FVG" &&
    fvg.active
  ) {
    bullishScore += 1;
  }

  // ----------------------------------------------------------
  // BEARISH CONDITIONS
  // ----------------------------------------------------------

  let bearishScore = 0;

  if (structure1m.direction === "BEARISH") {
    bearishScore += 2;
  }

  if (momentum10s.direction === "BEARISH") {
    bearishScore += 1;
  }

  if (
    fvg.direction === "BEARISH FVG" &&
    fvg.active
  ) {
    bearishScore += 1;
  }

  // ----------------------------------------------------------
  // CONTINUATION / SETUP DEVELOPING
  // ----------------------------------------------------------

  if (
    trend5m.direction === "BULLISH" &&
    bullishScore >= 4
  ) {
    return "BULLISH CONTINUATION DEVELOPING";
  }

  if (
    trend5m.direction === "BEARISH" &&
    bearishScore >= 4
  ) {
    return "BEARISH CONTINUATION DEVELOPING";
  }

  // ----------------------------------------------------------
  // RANGE MARKET
  // ----------------------------------------------------------

  if (
    trend5m.direction === "RANGE" &&
    bullishScore >= 4
  ) {
    return "BULLISH SETUP DEVELOPING";
  }

  if (
    trend5m.direction === "RANGE" &&
    bearishScore >= 4
  ) {
    return "BEARISH SETUP DEVELOPING";
  }

  // ----------------------------------------------------------
  // REVERSALS
  // ----------------------------------------------------------

  if (
    liquidity.direction === "SELL-SIDE SWEEP" &&
    liquidity.age !== null &&
    liquidity.age <= 4 &&
    (
      momentum10s.direction === "BULLISH" ||
      structure1m.direction === "BULLISH"
    )
  ) {
    return "BULLISH REVERSAL DEVELOPING";
  }

  if (
    liquidity.direction === "BUY-SIDE SWEEP" &&
    liquidity.age !== null &&
    liquidity.age <= 4 &&
    (
      momentum10s.direction === "BEARISH" ||
      structure1m.direction === "BEARISH"
    )
  ) {
    return "BEARISH REVERSAL DEVELOPING";
  }

  return null;
}

// ============================================================
// TRIGGER QUALITY
// ============================================================

function getTriggerQuality(
  structure10s,
  momentum10s,
  candles10s
) {
  let score = 0;

  if (structure10s.bos) {
    score += 50;
  }

  if (
    structure10s.bos &&
    structure10s.bosDirection ===
      momentum10s.direction
  ) {
    score += 25;
  }

  if (momentum10s.direction !== "NEUTRAL") {
    score += 15;
  }

  if (candles10s.length) {
    const latest =
      candles10s[candles10s.length - 1];

    const range =
      Math.max(
        latest.high - latest.low,
        0.00000001
      );

    const body =
      Math.abs(latest.close - latest.open);

    if (body / range >= 0.6) {
      score += 10;
    }
  }

  score = clamp(score, 0, 100);

  let quality = "WEAK";

  if (score >= 75) {
    quality = "STRONG";
  } else if (score >= 50) {
    quality = "MODERATE";
  }

  return {
    score,
    quality
  };
}

// ============================================================
// CONFIRMED SETUP
// ============================================================

function getConfirmedSetup(
  trend5m,
  structure1m,
  structure10s,
  momentum10s,
  liquidity,
  triggerScore
) {
  // ----------------------------------------------------------
  // BULLISH CONTINUATION
  // ----------------------------------------------------------

  if (
    trend5m.direction === "BULLISH" &&
    structure1m.direction === "BULLISH" &&
    (
      (
        structure1m.bos &&
        structure1m.bosDirection === "BULLISH"
      ) ||
      (
        structure10s.bos &&
        structure10s.bosDirection === "BULLISH"
      )
    ) &&
    momentum10s.direction === "BULLISH" &&
    triggerScore >= 60
  ) {
    return {
      direction: "BUY",
      setupType: "BULLISH CONTINUATION"
    };
  }

  // ----------------------------------------------------------
  // BEARISH CONTINUATION
  // ----------------------------------------------------------

  if (
    trend5m.direction === "BEARISH" &&
    structure1m.direction === "BEARISH" &&
    (
      (
        structure1m.bos &&
        structure1m.bosDirection === "BEARISH"
      ) ||
      (
        structure10s.bos &&
        structure10s.bosDirection === "BEARISH"
      )
    ) &&
    momentum10s.direction === "BEARISH" &&
    triggerScore >= 60
  ) {
    return {
      direction: "SELL",
      setupType: "BEARISH CONTINUATION"
    };
  }

  // ----------------------------------------------------------
  // BULLISH REVERSAL
  // ----------------------------------------------------------

  if (
    liquidity.direction === "SELL-SIDE SWEEP" &&
    liquidity.age !== null &&
    liquidity.age <= 4 &&
    (
      (
        structure1m.bos &&
        structure1m.bosDirection === "BULLISH"
      ) ||
      (
        structure10s.bos &&
        structure10s.bosDirection === "BULLISH"
      )
    ) &&
    momentum10s.direction === "BULLISH" &&
    triggerScore >= 60
  ) {
    return {
      direction: "BUY",
      setupType: "BULLISH REVERSAL"
    };
  }

  // ----------------------------------------------------------
  // BEARISH REVERSAL
  // ----------------------------------------------------------

  if (
    liquidity.direction === "BUY-SIDE SWEEP" &&
    liquidity.age !== null &&
    liquidity.age <= 4 &&
    (
      (
        structure1m.bos &&
        structure1m.bosDirection === "BEARISH"
      ) ||
      (
        structure10s.bos &&
        structure10s.bosDirection === "BEARISH"
      )
    ) &&
    momentum10s.direction === "BEARISH" &&
    triggerScore >= 60
  ) {
    return {
      direction: "SELL",
      setupType: "BEARISH REVERSAL"
    };
  }

  return null;
}

// ============================================================
// SETUP SCORE
// ============================================================

function scoreSetup(
  direction,
  trend5m,
  structure1m,
  structure10s,
  momentum10s,
  fvg,
  emaState,
  liquidity,
  rsiInfo
) {
  let score = 0;
  let confirmations = 0;

  const bullish = direction === "BUY";
  const expected = bullish ? "BULLISH" : "BEARISH";

  // 5M trend
  if (trend5m.direction === expected) {
    score += 25;
    confirmations++;
  }

  // 1M structure
  if (structure1m.direction === expected) {
    score += 20;
    confirmations++;
  }

  // BOS
  if (
    structure1m.bos &&
    structure1m.bosDirection === expected
  ) {
    score += 20;
    confirmations++;
  } else if (
    structure10s.bos &&
    structure10s.bosDirection === expected
  ) {
    score += 15;
    confirmations++;
  }

  // Momentum
  if (momentum10s.direction === expected) {
    score += 15;
    confirmations++;
  }

  // FVG
  if (
    (
      bullish &&
      fvg.direction === "BULLISH FVG"
    ) ||
    (
      !bullish &&
      fvg.direction === "BEARISH FVG"
    )
  ) {
    score += 8;
  }

  // EMA
  if (
    (
      bullish &&
      emaState === "Bullish"
    ) ||
    (
      !bullish &&
      emaState === "Bearish"
    )
  ) {
    score += 5;
  }

  // Reversal liquidity
  if (
    (
      bullish &&
      liquidity.direction === "SELL-SIDE SWEEP" &&
      liquidity.age !== null &&
      liquidity.age <= 4
    ) ||
    (
      !bullish &&
      liquidity.direction === "BUY-SIDE SWEEP" &&
      liquidity.age !== null &&
      liquidity.age <= 4
    )
  ) {
    score += 12;
    confirmations++;
  }

  // RSI recovery
  if (
    rsiInfo.recovery &&
    rsiInfo.recoveryDirection === expected
  ) {
    score += 5;
  }

  score = clamp(score, 0, 100);

  return {
    score,
    confirmations
  };
}

// ============================================================
// LEVEL CALCULATION
// ============================================================

function calculateLevels(
  direction,
  candles1m,
  currentATR
) {
  if (
    !candles1m.length ||
    !currentATR ||
    !Number.isFinite(currentATR)
  ) {
    return {
      entry: null,
      stopLoss: null,
      takeProfit: null,
      rr: 0
    };
  }

  const entry =
    candles1m[candles1m.length - 1].close;

  const recent =
    candles1m.slice(-12);

  const recentLow =
    Math.min(...recent.map(c => c.low));

  const recentHigh =
    Math.max(...recent.map(c => c.high));

  let stopLoss;
  let takeProfit;

  if (direction === "BUY") {
    stopLoss =
      recentLow - currentATR * 0.35;

    const risk = entry - stopLoss;

    takeProfit =
      entry + risk * 2;
  } else {
    stopLoss =
      recentHigh + currentATR * 0.35;

    const risk = stopLoss - entry;

    takeProfit =
      entry - risk * 2;
  }

  const risk =
    Math.abs(entry - stopLoss);

  const reward =
    Math.abs(takeProfit - entry);

  const rr =
    risk > 0 ? reward / risk : 0;

  return {
    entry: round(entry, 5),
    stopLoss: round(stopLoss, 5),
    takeProfit: round(takeProfit, 5),
    rr: round(rr, 2)
  };
}

// ============================================================
// WATCH LIST
// ============================================================

function getWatchFor(
  trend5m,
  structure1m,
  triggerQuality,
  momentum10s,
  fvg,
  liquidity
) {
  const watch = [];

  if (trend5m.direction === "RANGE") {
    watch.push("5M directional breakout");
  }

  if (
    structure1m.direction === "RANGE" ||
    !structure1m.bos
  ) {
    watch.push("1M directional BOS");
  }

  if (triggerQuality !== "STRONG") {
    watch.push("Stronger entry trigger");
  }

  if (momentum10s.direction === "NEUTRAL") {
    watch.push("Momentum confirmation");
  }

  if (!fvg.active) {
    watch.push("Active FVG confirmation");
  }

  if (
    liquidity.age === null ||
    liquidity.age > 4
  ) {
    watch.push("Fresh liquidity confirmation");
  }

  return watch.slice(0, 5);
}

// ============================================================
// MAIN ANALYSIS
// ============================================================

async function analyse(symbol) {
  const ticks =
    await getDerivTicks(
      symbol,
      HISTORY_BATCHES
    );

  if (ticks.length < 100) {
    throw new Error(
      "Not enough market data received from Deriv"
    );
  }

  const candles10s =
    buildCandles(ticks, 10);

  const candles1m =
    buildCandles(ticks, 60);

  const candles5m =
    buildCandles(ticks, 300);

  const trend5m =
    getTrend(candles5m);

  const structure1m =
    getStructure(candles1m);

  const structure10s =
    getStructure(candles10s);

  const momentum10s =
    getMomentum(candles10s);

  const liquidity =
    detectLiquiditySweep(candles1m);

  const fvg =
    detectFVG(candles1m);

  const emaState =
    getEMAState(candles1m);

  const rsiInfo =
    getRSIContext(candles1m);

  const currentATR =
    atr(candles1m, 14) || 0;

  const bias =
    getBiasScore(
      trend5m,
      structure1m,
      momentum10s,
      emaState
    );

  const trigger =
    getTriggerQuality(
      structure10s,
      momentum10s,
      candles10s
    );

  const developingSetup =
    detectDevelopingSetup(
      trend5m,
      structure1m,
      momentum10s,
      fvg,
      liquidity
    );

  const confirmedSetup =
    getConfirmedSetup(
      trend5m,
      structure1m,
      structure10s,
      momentum10s,
      liquidity,
      trigger.score
    );

  let action = "WAIT";
  let marketState = "WAIT";
  let setupType = "NO CONFIRMED SETUP";
  let confidence = "LOW";

  let score = 0;
  let confirmations = 0;

  let levels = {
    entry: null,
    stopLoss: null,
    takeProfit: null,
    rr: 0
  };

  let reason =
    "Conditions are not sufficiently aligned.";

  // ==========================================================
  // CONFIRMED TRADE
  // ==========================================================

  if (confirmedSetup) {
    const scored =
      scoreSetup(
        confirmedSetup.direction,
        trend5m,
        structure1m,
        structure10s,
        momentum10s,
        fvg,
        emaState,
        liquidity,
        rsiInfo
      );

    score = scored.score;
    confirmations = scored.confirmations;

    if (
      score >= 65 &&
      confirmations >= 4 &&
      trigger.score >= 60
    ) {
      action =
        confirmedSetup.direction;

      setupType =
        confirmedSetup.setupType;

      marketState =
        confirmedSetup.setupType;

      confidence =
        score >= 80
          ? "HIGH"
          : score >= 70
            ? "MEDIUM"
            : "LOW";

      levels =
        calculateLevels(
          action,
          candles1m,
          currentATR
        );

      reason =
        action === "BUY"
          ? "Bullish structure, momentum and entry confirmation are aligned."
          : "Bearish structure, momentum and entry confirmation are aligned.";
    }
  }

  // ==========================================================
  // DEVELOPING SETUP
  // ==========================================================

  if (
    action === "WAIT" &&
    developingSetup
  ) {
    marketState =
      "SETUP DEVELOPING";

    setupType =
      developingSetup;

    confidence = "LOW";

    reason =
      developingSetup.includes("REVERSAL")
        ? "A reversal structure is developing, but the entry trigger is not confirmed."
        : developingSetup.includes("SETUP DEVELOPING")
          ? "Directional conditions are developing, but the market has not confirmed the required 5M trend."
          : "Directional conditions are developing, but the entry trigger is not confirmed.";
  }

  // ==========================================================
  // BIAS ONLY
  // ==========================================================

  if (
    action === "WAIT" &&
    !developingSetup &&
    bias.biasDirection !== "RANGE"
  ) {
    marketState =
      bias.biasDirection === "BULLISH"
        ? "BULLISH BIAS"
        : "BEARISH BIAS";

    setupType =
      bias.biasDirection === "BULLISH"
        ? "BULLISH BIAS"
        : "BEARISH BIAS";

    reason =
      bias.biasDirection === "BULLISH"
        ? "The market has a bullish bias, but no complete setup is developing yet."
        : "The market has a bearish bias, but no complete setup is developing yet.";
  }

  const watchFor =
    action === "WAIT"
      ? getWatchFor(
          trend5m,
          structure1m,
          trigger.quality,
          momentum10s,
          fvg,
          liquidity
        )
      : [];

  return {
    success: true,
    symbol,
    dataSource: "Deriv public market data",
    mode: "paper analysis only",
    version: "V3.2",

    history: {
      ticks: ticks.length,
      batches: HISTORY_BATCHES
    },

    candles: {
      tenSecond: candles10s.length,
      oneMinute: candles1m.length,
      fiveMinute: candles5m.length
    },

    analysis: {
      bias: bias.biasDirection,
      biasStrength: bias.biasStrength,
      buyBias: bias.buyBias,
      sellBias: bias.sellBias,

      trend5m: trend5m.direction,
      trend5mStrength: trend5m.strength,

      structure1m: structure1m.direction,

      bos: structure1m.bos,
      bosDirection: structure1m.bosDirection,

      triggerBosDirection:
        structure10s.bos
          ? structure10s.bosDirection
          : "NONE",

      triggerQuality: trigger.quality,
      triggerScore: trigger.score,

      liquidity:
        liquidity.direction,

      liquidityStrength:
        liquidity.strength,

      liquidityAge:
        liquidity.age,

      fvg:
        fvg.direction,

      fvgStrength:
        fvg.strength,

      fvgAge:
        fvg.age,

      fvgActive:
        fvg.active,

      momentum:
        momentum10s.direction,

      momentumStrength:
        momentum10s.strength,

      ema:
        emaState,

      rsi:
        rsiInfo.value,

      rsiContext:
        rsiInfo.context,

      rsiRecovery:
        rsiInfo.recovery,

      rsiRecoveryDirection:
        rsiInfo.recoveryDirection,

      atr:
        round(currentATR, 5)
    },

    conclusion: {
      action,
      marketState,
      setupType,
      confidence,

      score,
      confirmations,

      entry: levels.entry,
      stopLoss: levels.stopLoss,
      takeProfit: levels.takeProfit,
      rr: levels.rr,

      reason,
      watchFor
    }
  };
}

// ============================================================
// ROUTES
// ============================================================

app.get("/", (req, res) => {
  res.json({
    success: true,
    service: "Keamz Fx",
    status: "online",
    version: "V3.2",
    mode: "paper analysis only"
  });
});

app.get("/signal", async (req, res) => {
  try {
    const symbol =
      normalizeSymbol(
        req.query.symbol || DEFAULT_SYMBOL
      );

    const result =
      await analyse(symbol);

    res.json(result);

  } catch (error) {
    console.error("Signal error:", error);

    res.status(500).json({
      success: false,
      error: error.message,
      mode: "paper analysis only",
      version: "V3.2"
    });
  }
});

// ============================================================
// SERVER
// ============================================================

app.listen(PORT, "0.0.0.0", () => {
  console.log(
    `Keamz Fx V3.2 running on port ${PORT}`
  );
});
