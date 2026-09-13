const express = require("express");
const WebSocket = require("ws");

const app = express();

const PORT = process.env.PORT || 3000;
const DEFAULT_SYMBOL = "stpRNG";

const DERIV_WS =
  "wss://api.derivws.com/trading/v1/options/ws/public";

const HISTORY_BATCHES = 12;
const TICKS_PER_BATCH = 1000;

/*
==========================================================
KEAMZ FX V3
==========================================================

5M  = Market bias
1M  = Structure
10S = Trigger

V3 focuses on SETUPS rather than simply adding indicator
points together.

Setup types:

1. BULLISH CONTINUATION
2. BEARISH CONTINUATION
3. BULLISH REVERSAL
4. BEARISH REVERSAL
5. WAIT / NO CONFIRMED SETUP

Paper analysis only.
==========================================================
*/


// ========================================================
// GENERAL HELPERS
// ========================================================

function sleep(ms) {
  return new Promise(function(resolve) {
    setTimeout(resolve, ms);
  });
}

function safeNumber(value, fallback) {
  var n = Number(value);

  if (!Number.isFinite(n)) {
    return fallback;
  }

  return n;
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function average(values) {
  if (!values || values.length === 0) {
    return 0;
  }

  var total = 0;

  for (var i = 0; i < values.length; i++) {
    total += values[i];
  }

  return total / values.length;
}

function roundNumber(value, decimals) {
  if (!Number.isFinite(value)) {
    return null;
  }

  var factor = Math.pow(10, decimals || 2);

  return Math.round(value * factor) / factor;
}


// ========================================================
// DERIV HISTORY
// ========================================================

function getDerivHistoryBatch(symbol, endTime) {
  return new Promise(function(resolve, reject) {

    var ws = new WebSocket(DERIV_WS);

    var finished = false;

    function finishSuccess(data) {
      if (finished) return;

      finished = true;

      try {
        ws.close();
      } catch (e) {}

      resolve(data);
    }

    function finishError(error) {
      if (finished) return;

      finished = true;

      try {
        ws.close();
      } catch (e) {}

      reject(error);
    }

    var timeout = setTimeout(function() {
      finishError(new Error("Deriv WebSocket timeout."));
    }, 15000);

    ws.on("open", function() {

      var request = {
        ticks_history: symbol,
        end: endTime || "latest",
        count: TICKS_PER_BATCH,
        style: "ticks"
      };

      ws.send(JSON.stringify(request));
    });

    ws.on("message", function(message) {

      try {
        var data = JSON.parse(message.toString());

        if (data.error) {
          clearTimeout(timeout);

          finishError(
            new Error(
              data.error.message ||
              "Deriv returned an error."
            )
          );

          return;
        }

        if (
          data.history &&
          data.history.times &&
          data.history.prices
        ) {
          clearTimeout(timeout);

          finishSuccess({
            times: data.history.times,
            prices: data.history.prices
          });
        }

      } catch (error) {
        clearTimeout(timeout);
        finishError(error);
      }
    });

    ws.on("error", function(error) {
      clearTimeout(timeout);
      finishError(error);
    });

    ws.on("close", function() {
      if (!finished) {
        clearTimeout(timeout);
        finishError(
          new Error("Deriv WebSocket closed unexpectedly.")
        );
      }
    });
  });
}


// ========================================================
// GET LARGE TICK HISTORY
// ========================================================

async function getDerivTicks(symbol, batches) {

  var allTicks = [];

  var endTime = "latest";

  for (var i = 0; i < batches; i++) {

    var batch = await getDerivHistoryBatch(
      symbol,
      endTime
    );

    if (!batch || !batch.times || !batch.prices) {
      break;
    }

    for (var j = 0; j < batch.times.length; j++) {

      allTicks.push({
        time: Number(batch.times[j]),
        price: Number(batch.prices[j])
      });
    }

    var oldest = Math.min.apply(
      null,
      batch.times.map(function(t) {
        return Number(t);
      })
    );

    if (!Number.isFinite(oldest)) {
      break;
    }

    endTime = oldest - 1;

    await sleep(100);
  }

  // Remove invalid ticks.
  allTicks = allTicks.filter(function(tick) {
    return (
      Number.isFinite(tick.time) &&
      Number.isFinite(tick.price)
    );
  });

  // Sort oldest -> newest.
  allTicks.sort(function(a, b) {
    return a.time - b.time;
  });

  // Deduplicate.
  var unique = [];
  var seen = {};

  for (var k = 0; k < allTicks.length; k++) {

    var key =
      String(allTicks[k].time) +
      "_" +
      String(allTicks[k].price);

    if (!seen[key]) {
      seen[key] = true;
      unique.push(allTicks[k]);
    }
  }

  return unique;
}


// ========================================================
// BUILD CANDLES
// ========================================================

function buildCandles(ticks, seconds) {

  if (!ticks || ticks.length === 0) {
    return [];
  }

  var candles = [];
  var current = null;

  for (var i = 0; i < ticks.length; i++) {

    var tick = ticks[i];

    var bucket =
      Math.floor(tick.time / seconds) *
      seconds;

    if (!current || current.time !== bucket) {

      if (current) {
        current.close = current.lastPrice;
        delete current.lastPrice;

        current.range =
          current.high - current.low;

        current.body =
          Math.abs(current.close - current.open);

        current.direction =
          current.close > current.open
            ? "BULLISH"
            : current.close < current.open
              ? "BEARISH"
              : "NEUTRAL";

        candles.push(current);
      }

      current = {
        time: bucket,
        open: tick.price,
        high: tick.price,
        low: tick.price,
        close: tick.price,
        lastPrice: tick.price
      };

    } else {

      current.high =
        Math.max(current.high, tick.price);

      current.low =
        Math.min(current.low, tick.price);

      current.lastPrice = tick.price;
    }
  }

  if (current) {

    current.close = current.lastPrice;
    delete current.lastPrice;

    current.range =
      current.high - current.low;

    current.body =
      Math.abs(current.close - current.open);

    current.direction =
      current.close > current.open
        ? "BULLISH"
        : current.close < current.open
          ? "BEARISH"
          : "NEUTRAL";

    candles.push(current);
  }

  return candles;
}


// ========================================================
// EMA
// ========================================================

function ema(candles, period) {

  if (!candles || candles.length < period) {
    return null;
  }

  var multiplier =
    2 / (period + 1);

  var value = 0;

  for (var i = 0; i < period; i++) {
    value += candles[i].close;
  }

  value /= period;

  for (var j = period; j < candles.length; j++) {

    value =
      (candles[j].close - value) *
      multiplier +
      value;
  }

  return value;
}


// ========================================================
// WILDER RSI
// ========================================================

function rsi(candles, period) {

  if (!candles || candles.length <= period) {
    return null;
  }

  var gains = 0;
  var losses = 0;

  for (var i = 1; i <= period; i++) {

    var difference =
      candles[i].close -
      candles[i - 1].close;

    if (difference >= 0) {
      gains += difference;
    } else {
      losses += Math.abs(difference);
    }
  }

  var averageGain =
    gains / period;

  var averageLoss =
    losses / period;

  for (
    var j = period + 1;
    j < candles.length;
    j++
  ) {

    var change =
      candles[j].close -
      candles[j - 1].close;

    var gain =
      change > 0 ? change : 0;

    var loss =
      change < 0 ? Math.abs(change) : 0;

    averageGain =
      ((averageGain * (period - 1)) + gain) /
      period;

    averageLoss =
      ((averageLoss * (period - 1)) + loss) /
      period;
  }

  if (averageLoss === 0) {
    return 100;
  }

  var rs =
    averageGain / averageLoss;

  return 100 - (100 / (1 + rs));
}


// ========================================================
// ATR
// ========================================================

function atr(candles, period) {

  if (!candles || candles.length <= period) {
    return null;
  }

  var trs = [];

  for (var i = 1; i < candles.length; i++) {

    var current = candles[i];
    var previous = candles[i - 1];

    var tr = Math.max(
      current.high - current.low,
      Math.abs(
        current.high - previous.close
      ),
      Math.abs(
        current.low - previous.close
      )
    );

    trs.push(tr);
  }

  if (trs.length < period) {
    return null;
  }

  var value = 0;

  for (var j = 0; j < period; j++) {
    value += trs[j];
  }

  value /= period;

  for (var k = period; k < trs.length; k++) {

    value =
      ((value * (period - 1)) + trs[k]) /
      period;
  }

  return value;
}


// ========================================================
// MARKET TREND
// ========================================================

function getTrend(candles) {

  if (!candles || candles.length < 30) {
    return {
      direction: "RANGE",
      strength: 0
    };
  }

  var fast =
    ema(candles, 9);

  var slow =
    ema(candles, 21);

  var current =
    candles[candles.length - 1].close;

  if (
    fast === null ||
    slow === null
  ) {
    return {
      direction: "RANGE",
      strength: 0
    };
  }

  var distance =
    Math.abs(fast - slow);

  var atrValue =
    atr(candles, 14);

  if (!atrValue || atrValue === 0) {
    return {
      direction: "RANGE",
      strength: 0
    };
  }

  var normalized =
    distance / atrValue;

  var strength =
    clamp(
      Math.round(normalized * 100),
      0,
      100
    );

  if (
    fast > slow &&
    current >= fast
  ) {
    return {
      direction: "BULLISH",
      strength: strength
    };
  }

  if (
    fast < slow &&
    current <= fast
  ) {
    return {
      direction: "BEARISH",
      strength: strength
    };
  }

  return {
    direction: "RANGE",
    strength: strength
  };
}


// ========================================================
// STRUCTURE + BOS
// ========================================================

function getStructure(candles) {

  if (!candles || candles.length < 15) {

    return {
      direction: "RANGE",
      bos: false,
      bosDirection: "NONE",
      swingHigh: null,
      swingLow: null
    };
  }

  var swingHigh = null;
  var swingLow = null;

  var start =
    Math.max(2, candles.length - 30);

  var end =
    candles.length - 3;

  for (var i = start; i <= end; i++) {

    if (
      candles[i].high >
      candles[i - 1].high &&
      candles[i].high >
      candles[i + 1].high
    ) {
      swingHigh = candles[i].high;
    }

    if (
      candles[i].low <
      candles[i - 1].low &&
      candles[i].low <
      candles[i + 1].low
    ) {
      swingLow = candles[i].low;
    }
  }

  var last =
    candles[candles.length - 1];

  var previous =
    candles[candles.length - 2];

  var bos = false;
  var bosDirection = "NONE";

  var atrValue =
    atr(candles, 14) || 0;

  var buffer =
    atrValue * 0.08;

  if (
    swingHigh !== null &&
    last.close > swingHigh + buffer &&
    previous.close <= swingHigh + buffer
  ) {
    bos = true;
    bosDirection = "BULLISH";
  }

  if (
    swingLow !== null &&
    last.close < swingLow - buffer &&
    previous.close >= swingLow - buffer
  ) {
    bos = true;
    bosDirection = "BEARISH";
  }

  var recentCloses = [];

  for (
    var j = Math.max(0, candles.length - 8);
    j < candles.length;
    j++
  ) {
    recentCloses.push(candles[j].close);
  }

  var first =
    recentCloses[0];

  var lastClose =
    recentCloses[recentCloses.length - 1];

  var direction = "RANGE";

  if (
    lastClose > first &&
    last.close > last.open
  ) {
    direction = "BULLISH";
  }

  if (
    lastClose < first &&
    last.close < last.open
  ) {
    direction = "BEARISH";
  }

  return {
    direction: direction,
    bos: bos,
    bosDirection: bosDirection,
    swingHigh: swingHigh,
    swingLow: swingLow
  };
}


// ========================================================
// LIQUIDITY SWEEP
// ========================================================

function detectLiquiditySweep(candles) {

  if (!candles || candles.length < 10) {

    return {
      direction: "NONE",
      strength: 0,
      age: null
    };
  }

  var best = {
    direction: "NONE",
    strength: 0,
    age: null
  };

  var start =
    Math.max(2, candles.length - 8);

  for (
    var i = start;
    i < candles.length;
    i++
  ) {

    var candle =
      candles[i];

    var previousHigh =
      Math.max(
        candles[i - 1].high,
        candles[i - 2].high
      );

    var previousLow =
      Math.min(
        candles[i - 1].low,
        candles[i - 2].low
      );

    // Price sweeps highs and closes back below.
    if (
      candle.high > previousHigh &&
      candle.close < previousHigh
    ) {

      var wick =
        candle.high -
        Math.max(
          candle.open,
          candle.close
        );

      var range =
        candle.high -
        candle.low;

      var strength =
        range > 0
          ? clamp(
              Math.round(
                (wick / range) * 100
              ),
              0,
              100
            )
          : 0;

      var age =
        candles.length - 1 - i;

      if (strength > best.strength) {

        best = {
          direction: "BUY-SIDE SWEEP",
          strength: strength,
          age: age
        };
      }
    }

    // Price sweeps lows and closes back above.
    if (
      candle.low < previousLow &&
      candle.close > previousLow
    ) {

      var lowerWick =
        Math.min(
          candle.open,
          candle.close
        ) -
        candle.low;

      var candleRange =
        candle.high -
        candle.low;

      var lowerStrength =
        candleRange > 0
          ? clamp(
              Math.round(
                (lowerWick / candleRange) * 100
              ),
              0,
              100
            )
          : 0;

      var lowerAge =
        candles.length - 1 - i;

      if (
        lowerStrength >
        best.strength
      ) {

        best = {
          direction: "SELL-SIDE SWEEP",
          strength: lowerStrength,
          age: lowerAge
        };
      }
    }
  }

  return best;
}


// ========================================================
// FAIR VALUE GAP
// ========================================================

function detectFVG(candles) {

  if (!candles || candles.length < 5) {

    return {
      direction: "NONE",
      strength: 0,
      age: null,
      gap: 0,
      active: false
    };
  }

  var best = {
    direction: "NONE",
    strength: 0,
    age: null,
    gap: 0,
    active: false
  };

  var start =
    Math.max(2, candles.length - 12);

  for (
    var i = start;
    i < candles.length - 1;
    i++
  ) {

    var left =
      candles[i - 1];

    var middle =
      candles[i];

    var right =
      candles[i + 1];

    // Bullish FVG:
    // right.low > left.high
    if (
      right.low > left.high
    ) {

      var gap =
        right.low - left.high;

      var range =
        middle.high - middle.low;

      var strength =
        range > 0
          ? clamp(
              Math.round(
                (gap / range) * 100
              ),
              0,
              100
            )
          : 0;

      var active =
        candles[candles.length - 1].close >
        left.high;

      if (
        active &&
        strength >= best.strength
      ) {

        best = {
          direction: "BULLISH FVG",
          strength: strength,
          age: candles.length - 1 - i,
          gap: gap,
          active: true
        };
      }
    }

    // Bearish FVG:
    // right.high < left.low
    if (
      right.high < left.low
    ) {

      var bearishGap =
        left.low - right.high;

      var bearishRange =
        middle.high - middle.low;

      var bearishStrength =
        bearishRange > 0
          ? clamp(
              Math.round(
                (bearishGap / bearishRange) * 100
              ),
              0,
              100
            )
          : 0;

      var bearishActive =
        candles[candles.length - 1].close <
        left.low;

      if (
        bearishActive &&
        bearishStrength >= best.strength
      ) {

        best = {
          direction: "BEARISH FVG",
          strength: bearishStrength,
          age: candles.length - 1 - i,
          gap: bearishGap,
          active: true
        };
      }
    }
  }

  return best;
}


// ========================================================
// MOMENTUM
// ========================================================

function getMomentum(candles) {

  if (!candles || candles.length < 8) {
    return {
      direction: "NEUTRAL",
      strength: 0
    };
  }

  var recent =
    candles.slice(
      Math.max(0, candles.length - 5)
    );

  var bullish = 0;
  var bearish = 0;

  for (var i = 0; i < recent.length; i++) {

    var candle =
      recent[i];

    var range =
      candle.high - candle.low;

    if (range <= 0) {
      continue;
    }

    var bodyRatio =
      candle.body / range;

    if (
      candle.close > candle.open &&
      bodyRatio >= 0.45
    ) {
      bullish++;
    }

    if (
      candle.close < candle.open &&
      bodyRatio >= 0.45
    ) {
      bearish++;
    }
  }

  var strength =
    Math.abs(bullish - bearish) * 20;

  if (bullish >= 3 && bullish > bearish) {

    return {
      direction: "BULLISH",
      strength: clamp(strength, 0, 100)
    };
  }

  if (bearish >= 3 && bearish > bullish) {

    return {
      direction: "BEARISH",
      strength: clamp(strength, 0, 100)
    };
  }

  return {
    direction: "NEUTRAL",
    strength: strength
  };
}


// ========================================================
// MOMENTUM RECOVERY
// ========================================================

function getRSIContext(candles) {

  if (!candles || candles.length < 25) {

    return {
      value: null,
      context: "N/A",
      recovery: false,
      recoveryDirection: "NONE"
    };
  }

  var currentRSI =
    rsi(candles, 14);

  var previousCandles =
    candles.slice(
      0,
      candles.length - 1
    );

  var previousRSI =
    rsi(previousCandles, 14);

  if (
    currentRSI === null ||
    previousRSI === null
  ) {

    return {
      value: currentRSI,
      context: "N/A",
      recovery: false,
      recoveryDirection: "NONE"
    };
  }

  var recoveryAmount =
    currentRSI - previousRSI;

  var context = "NEUTRAL";

  if (currentRSI < 30) {
    context = "OVERSOLD";
  } else if (currentRSI > 70) {
    context = "OVERBOUGHT";
  }

  var recovery = false;
  var recoveryDirection = "NONE";

  // Bullish RSI recovery.
  if (
    previousRSI < 35 &&
    currentRSI > previousRSI &&
    recoveryAmount >= 1.5
  ) {
    recovery = true;
    recoveryDirection = "BULLISH";
  }

  // Bearish RSI recovery.
  if (
    previousRSI > 65 &&
    currentRSI < previousRSI &&
    recoveryAmount <= -1.5
  ) {
    recovery = true;
    recoveryDirection = "BEARISH";
  }

  return {
    value: currentRSI,
    context: context,
    recovery: recovery,
    recoveryDirection: recoveryDirection
  };
}


// ========================================================
// EMA DIRECTION
// ========================================================

function getEMAState(candles) {

  var fast =
    ema(candles, 9);

  var slow =
    ema(candles, 21);

  if (
    fast === null ||
    slow === null
  ) {
    return "N/A";
  }

  if (fast > slow) {
    return "Bullish";
  }

  if (fast < slow) {
    return "Bearish";
  }

  return "Neutral";
}


// ========================================================
// TRIGGER QUALITY
// ========================================================

function getTriggerQuality(
  candles10s,
  structure10s,
  momentum10s
) {

  var score = 0;

  if (
    structure10s.bos
  ) {
    score += 40;
  }

  if (
    structure10s.bosDirection ===
    momentum10s.direction
  ) {
    score += 30;
  }

  if (
    momentum10s.direction !==
    "NEUTRAL"
  ) {
    score += 20;
  }

  var last =
    candles10s[candles10s.length - 1];

  if (last) {

    var range =
      last.high - last.low;

    if (
      range > 0 &&
      last.body / range >= 0.55
    ) {
      score += 10;
    }
  }

  score =
    clamp(score, 0, 100);

  var quality = "WEAK";

  if (score >= 75) {
    quality = "STRONG";
  } else if (score >= 50) {
    quality = "MODERATE";
  }

  return {
    score: score,
    quality: quality
  };
}


// ========================================================
// SETUP DETECTION
// ========================================================

function determineSetup(
  trend5m,
  structure1m,
  liquidity1m,
  fvg1m,
  momentum10s,
  structure10s,
  rsiContext
) {

  /*
  --------------------------------------------------------
  BULLISH CONTINUATION
  --------------------------------------------------------
  */

  var bullishContinuation =
    trend5m.direction === "BULLISH" &&
    structure1m.direction === "BULLISH" &&
    (
      structure1m.bosDirection === "BULLISH" ||
      structure10s.bosDirection === "BULLISH"
    ) &&
    momentum10s.direction === "BULLISH";

  if (bullishContinuation) {

    return {
      type: "BULLISH CONTINUATION",
      direction: "BUY"
    };
  }


  /*
  --------------------------------------------------------
  BEARISH CONTINUATION
  --------------------------------------------------------
  */

  var bearishContinuation =
    trend5m.direction === "BEARISH" &&
    structure1m.direction === "BEARISH" &&
    (
      structure1m.bosDirection === "BEARISH" ||
      structure10s.bosDirection === "BEARISH"
    ) &&
    momentum10s.direction === "BEARISH";

  if (bearishContinuation) {

    return {
      type: "BEARISH CONTINUATION",
      direction: "SELL"
    };
  }


  /*
  --------------------------------------------------------
  BULLISH REVERSAL
  --------------------------------------------------------

  Sell-side liquidity is swept.
  Then bullish BOS + bullish momentum confirms.
  RSI recovery strengthens the setup.
  --------------------------------------------------------
  */

  var bullishReversal =
    liquidity1m.direction ===
      "SELL-SIDE SWEEP" &&

    liquidity1m.age !== null &&
    liquidity1m.age <= 4 &&

    (
      structure1m.bosDirection === "BULLISH" ||
      structure10s.bosDirection === "BULLISH"
    ) &&

    momentum10s.direction === "BULLISH";

  if (bullishReversal) {

    return {
      type: "BULLISH REVERSAL",
      direction: "BUY"
    };
  }


  /*
  --------------------------------------------------------
  BEARISH REVERSAL
  --------------------------------------------------------
  */

  var bearishReversal =
    liquidity1m.direction ===
      "BUY-SIDE SWEEP" &&

    liquidity1m.age !== null &&
    liquidity1m.age <= 4 &&

    (
      structure1m.bosDirection === "BEARISH" ||
      structure10s.bosDirection === "BEARISH"
    ) &&

    momentum10s.direction === "BEARISH";

  if (bearishReversal) {

    return {
      type: "BEARISH REVERSAL",
      direction: "SELL"
    };
  }


  return {
    type: "NO CONFIRMED SETUP",
    direction: "WAIT"
  };
}


// ========================================================
// SCORE SETUP
// ========================================================

function scoreSetup(
  direction,
  setup,
  trend5m,
  structure1m,
  structure10s,
  liquidity1m,
  fvg1m,
  momentum10s,
  rsiContext,
  emaState
) {

  var score = 0;
  var confirmations = 0;

  var expected =
    direction === "BUY"
      ? "BULLISH"
      : "BEARISH";


  // ------------------------------------------------------
  // 5M BIAS
  // ------------------------------------------------------

  if (
    trend5m.direction === expected
  ) {

    score += 25;
    confirmations++;

  } else if (
    trend5m.direction === "RANGE"
  ) {

    score += 8;

  } else {

    score -= 15;
  }


  // ------------------------------------------------------
  // 1M STRUCTURE
  // ------------------------------------------------------

  if (
    structure1m.direction === expected
  ) {

    score += 20;
    confirmations++;

  } else if (
    structure1m.direction !== "RANGE"
  ) {

    score -= 10;
  }


  // ------------------------------------------------------
  // BOS
  // ------------------------------------------------------

  if (
    structure1m.bosDirection === expected
  ) {

    score += 20;
    confirmations++;

  } else if (
    structure10s.bosDirection === expected
  ) {

    score += 15;
    confirmations++;

  }


  // ------------------------------------------------------
  // MOMENTUM
  // ------------------------------------------------------

  if (
    momentum10s.direction === expected
  ) {

    score += 15;
    confirmations++;

  }


  // ------------------------------------------------------
  // FVG
  // ------------------------------------------------------

  var expectedFVG =
    direction === "BUY"
      ? "BULLISH FVG"
      : "BEARISH FVG";

  if (
    fvg1m.direction === expectedFVG &&
    fvg1m.active
  ) {

    score += 8;
    confirmations++;
  }


  // ------------------------------------------------------
  // EMA
  // ------------------------------------------------------

  var expectedEMA =
    direction === "BUY"
      ? "Bullish"
      : "Bearish";

  if (
    emaState === expectedEMA
  ) {

    score += 5;
  }


  // ------------------------------------------------------
  // LIQUIDITY
  // ------------------------------------------------------

  if (setup.indexOf("REVERSAL") >= 0) {

    var expectedSweep =
      direction === "BUY"
        ? "SELL-SIDE SWEEP"
        : "BUY-SIDE SWEEP";

    if (
      liquidity1m.direction === expectedSweep &&
      liquidity1m.age <= 4
    ) {

      score += 12;
      confirmations++;
    }

  } else {

    // For continuation setups, a
    // directly opposing fresh sweep is
    // treated as a warning.

    var opposingSweep =
      direction === "BUY"
        ? "BUY-SIDE SWEEP"
        : "SELL-SIDE SWEEP";

    if (
      liquidity1m.direction === opposingSweep &&
      liquidity1m.age <= 2
    ) {

      score -= 8;
    }
  }


  // ------------------------------------------------------
  // RSI
  // ------------------------------------------------------

  if (
    rsiContext.recovery &&
    rsiContext.recoveryDirection ===
      expected
  ) {

    score += 5;
  }


  // ------------------------------------------------------
  // FINAL SCORE
  // ------------------------------------------------------

  score =
    clamp(
      Math.round(score),
      0,
      100
    );

  return {
    score: score,
    confirmations: confirmations
  };
}


// ========================================================
// TRADE LEVELS
// ========================================================

function calculateLevels(
  direction,
  candles,
  atrValue
) {

  if (
    !candles ||
    candles.length < 12 ||
    !atrValue
  ) {

    return {
      entry: null,
      stopLoss: null,
      takeProfit: null,
      rr: 0
    };
  }

  var recent =
    candles.slice(
      Math.max(0, candles.length - 12)
    );

  var entry =
    candles[candles.length - 1].close;

  var low =
    Math.min.apply(
      null,
      recent.map(function(c) {
        return c.low;
      })
    );

  var high =
    Math.max.apply(
      null,
      recent.map(function(c) {
        return c.high;
      })
    );

  var riskBuffer =
    atrValue * 0.35;

  var stopLoss;
  var takeProfit;

  if (direction === "BUY") {

    stopLoss =
      low - riskBuffer;

    var risk =
      entry - stopLoss;

    if (risk <= 0) {
      return {
        entry: roundNumber(entry, 5),
        stopLoss: null,
        takeProfit: null,
        rr: 0
      };
    }

    takeProfit =
      entry + risk * 2;

  } else {

    stopLoss =
      high + riskBuffer;

    var sellRisk =
      stopLoss - entry;

    if (sellRisk <= 0) {
      return {
        entry: roundNumber(entry, 5),
        stopLoss: null,
        takeProfit: null,
        rr: 0
      };
    }

    takeProfit =
      entry - sellRisk * 2;
  }

  return {
    entry: roundNumber(entry, 5),
    stopLoss: roundNumber(stopLoss, 5),
    takeProfit: roundNumber(takeProfit, 5),
    rr: 2
  };
}


// ========================================================
// MAIN ANALYSIS
// ========================================================

async function analyse(symbol) {

  var ticks =
    await getDerivTicks(
      symbol,
      HISTORY_BATCHES
    );

  if (!ticks || ticks.length < 100) {

    throw new Error(
      "Not enough market data."
    );
  }


  // Build timeframes.
  var candles10s =
    buildCandles(ticks, 10);

  var candles1m =
    buildCandles(ticks, 60);

  var candles5m =
    buildCandles(ticks, 300);


  if (
    candles10s.length < 30 ||
    candles1m.length < 30 ||
    candles5m.length < 20
  ) {

    throw new Error(
      "Not enough candles for analysis."
    );
  }


  // ------------------------------------------------------
  // 5M
  // ------------------------------------------------------

  var trend5m =
    getTrend(candles5m);


  // ------------------------------------------------------
  // 1M
  // ------------------------------------------------------

  var structure1m =
    getStructure(candles1m);

  var liquidity1m =
    detectLiquiditySweep(candles1m);

  var fvg1m =
    detectFVG(candles1m);

  var emaState =
    getEMAState(candles1m);

  var rsiContext =
    getRSIContext(candles1m);

  var atrValue =
    atr(candles1m, 14);


  // ------------------------------------------------------
  // 10S TRIGGER
  // ------------------------------------------------------

  var structure10s =
    getStructure(candles10s);

  var momentum10s =
    getMomentum(candles10s);

  var trigger =
    getTriggerQuality(
      candles10s,
      structure10s,
      momentum10s
    );


  // ------------------------------------------------------
  // SETUP
  // ------------------------------------------------------

  var setup =
    determineSetup(
      trend5m,
      structure1m,
      liquidity1m,
      fvg1m,
      momentum10s,
      structure10s,
      rsiContext
    );


  // ------------------------------------------------------
  // DEFAULT WAIT
  // ------------------------------------------------------

  var action = "WAIT";

  var confidence = "LOW";

  var score = 0;

  var confirmations = 0;

  var levels = {
    entry: null,
    stopLoss: null,
    takeProfit: null,
    rr: 0
  };

  var reason =
    "Conditions are not sufficiently aligned.";

  var watchFor = [];


  // ------------------------------------------------------
  // VALIDATE SETUP
  // ------------------------------------------------------

  if (
    setup.direction === "BUY" ||
    setup.direction === "SELL"
  ) {

    var setupScore =
      scoreSetup(
        setup.direction,
        setup.type,
        trend5m,
        structure1m,
        structure10s,
        liquidity1m,
        fvg1m,
        momentum10s,
        rsiContext,
        emaState
      );

    score =
      setupScore.score;

    confirmations =
      setupScore.confirmations;


    /*
    ------------------------------------------------------
    CONTINUATION REQUIREMENTS
    ------------------------------------------------------
    */

    var continuationValid =
      setup.type.indexOf("CONTINUATION") >= 0 &&
      score >= 65 &&
      confirmations >= 4 &&
      trigger.score >= 50;


    /*
    ------------------------------------------------------
    REVERSAL REQUIREMENTS
    ------------------------------------------------------
    */

    var reversalValid =
      setup.type.indexOf("REVERSAL") >= 0 &&
      score >= 65 &&
      confirmations >= 4 &&
      trigger.score >= 60;


    if (
      continuationValid ||
      reversalValid
    ) {

      action =
        setup.direction;

      levels =
        calculateLevels(
          action,
          candles1m,
          atrValue
        );


      if (
        score >= 80 &&
        trigger.score >= 75
      ) {

        confidence = "HIGH";

      } else if (
        score >= 70
      ) {

        confidence = "MEDIUM";

      } else {

        confidence = "LOW";
      }


      if (
        setup.type ===
        "BULLISH CONTINUATION"
      ) {

        reason =
          "5M bullish bias, bullish 1M structure and a confirmed bullish trigger support continuation.";

      } else if (
        setup.type ===
        "BEARISH CONTINUATION"
      ) {

        reason =
          "5M bearish bias, bearish 1M structure and a confirmed bearish trigger support continuation.";

      } else if (
        setup.type ===
        "BULLISH REVERSAL"
      ) {

        reason =
          "Sell-side liquidity was swept and bullish structure plus momentum confirmed a potential reversal.";

      } else if (
        setup.type ===
        "BEARISH REVERSAL"
      ) {

        reason =
          "Buy-side liquidity was swept and bearish structure plus momentum confirmed a potential reversal.";
      }

    } else {

      action = "WAIT";

      if (
        setup.type.indexOf("CONTINUATION") >= 0
      ) {

        reason =
          "A directional continuation structure was detected, but trigger confirmation is not strong enough.";

      } else {

        reason =
          "A possible reversal structure was detected, but confirmation is insufficient.";
      }
    }
  }


  // ------------------------------------------------------
  // WATCH FOR
  // ------------------------------------------------------

  if (
    trend5m.direction === "RANGE"
  ) {

    watchFor.push(
      "5M directional breakout"
    );
  }

  if (
    structure1m.bosDirection === "NONE"
  ) {

    watchFor.push(
      "1M directional BOS"
    );
  }

  if (
    momentum10s.direction === "NEUTRAL"
  ) {

    watchFor.push(
      "10S momentum confirmation"
    );
  }

  if (
    trigger.score < 60
  ) {

    watchFor.push(
      "Stronger entry trigger"
    );
  }

  if (
    liquidity1m.direction !== "NONE" &&
    liquidity1m.age !== null &&
    liquidity1m.age > 4
  ) {

    watchFor.push(
      "Fresh liquidity confirmation"
    );
  }

  if (
    fvg1m.direction === "NONE"
  ) {

    watchFor.push(
      "Active FVG"
    );
  }


  // Remove duplicate watch items.
  watchFor =
    watchFor.filter(
      function(item, index, array) {
        return array.indexOf(item) === index;
      }
    );


  // ------------------------------------------------------
  // ANALYSIS RESPONSE
  // ------------------------------------------------------

  return {

    success: true,

    symbol: symbol,

    dataSource:
      "Deriv public market data",

    mode:
      "paper analysis only",

    version:
      "V3",

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

      trend5m:
        trend5m.direction,

      trend5mStrength:
        trend5m.strength,

      structure1m:
        structure1m.direction,

      bos:
        structure1m.bos,

      bosDirection:
        structure1m.bosDirection,

      triggerBosDirection:
        structure10s.bosDirection,

      liquidity:
        liquidity1m.direction,

      liquidityStrength:
        liquidity1m.strength,

      liquidityAge:
        liquidity1m.age,

      fvg:
        fvg1m.direction,

      fvgStrength:
        fvg1m.strength,

      fvgAge:
        fvg1m.age,

      fvgActive:
        fvg1m.active,

      momentum:
        momentum10s.direction,

      momentumStrength:
        momentum10s.strength,

      triggerQuality:
        trigger.quality,

      triggerScore:
        trigger.score,

      ema:
        emaState,

      rsi:
        rsiContext.value !== null
          ? roundNumber(rsiContext.value, 2)
          : null,

      rsiContext:
        rsiContext.context,

      rsiRecovery:
        rsiContext.recovery,

      rsiRecoveryDirection:
        rsiContext.recoveryDirection,

      atr:
        atrValue !== null
          ? roundNumber(atrValue, 5)
          : null
    },

    conclusion: {

      action:
        action,

      setupType:
        action === "WAIT"
          ? "NO CONFIRMED SETUP"
          : setup.type,

      confidence:
        confidence,

      score:
        score,

      confirmations:
        confirmations,

      entry:
        levels.entry,

      stopLoss:
        levels.stopLoss,

      takeProfit:
        levels.takeProfit,

      rr:
        levels.rr,

      reason:
        reason,

      watchFor:
        watchFor
    }
  };
}


// ========================================================
// HOME
// ========================================================

app.get("/", function(req, res) {

  res.json({
    success: true,
    service: "Keamz Fx",
    status: "online",
    version: "V3",
    mode: "paper analysis only"
  });

});


// ========================================================
// SIGNAL ENDPOINT
// ========================================================

app.get("/signal", async function(req, res) {

  try {

    var symbol =
      String(
        req.query.symbol ||
        DEFAULT_SYMBOL
      ).trim();

    if (!symbol) {
      symbol = DEFAULT_SYMBOL;
    }

    console.log(
      "Analysing instrument: " +
      symbol
    );

    var result =
      await analyse(symbol);

    res.json(result);

  } catch (error) {

    console.error(
      "Signal error:",
      error
    );

    res.status(500).json({

      success: false,

      error:
        error.message ||
        "Analysis failed.",

      mode:
        "paper analysis only"
    });
  }

});


// ========================================================
// START SERVER
// ========================================================

app.listen(
  PORT,
  "0.0.0.0",
  function() {

    console.log(
      "Keamz Fx V3 running on port " +
      PORT
    );

  }
);
