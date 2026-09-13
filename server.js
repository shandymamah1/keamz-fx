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
 KEAMZ FX V3.3
==========================================================

5M  = MARKET BIAS
1M  = SETUP / STRUCTURE
10S = ENTRY TRIGGER

Pipeline:

MARKET DATA
     ↓
5M BIAS
     ↓
1M STRUCTURE
     ↓
SETUP FORMATION
     ↓
10S TRIGGER
     ↓
BUY / SELL / WAIT

Important:

A bias is NOT a trade signal.

The bot can therefore say:

BEARISH BIAS
BEARISH SETUP DEVELOPING
TRIGGER NOT CONFIRMED
WAIT

This is intentional.

Paper analysis only.
==========================================================
*/


// ========================================================
// HELPERS
// ========================================================

function sleep(ms) {
  return new Promise(function(resolve) {
    setTimeout(resolve, ms);
  });
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
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

    function success(data) {

      if (finished) return;

      finished = true;

      try {
        ws.close();
      } catch (e) {}

      resolve(data);
    }

    function failure(error) {

      if (finished) return;

      finished = true;

      try {
        ws.close();
      } catch (e) {}

      reject(error);
    }

    var timeout = setTimeout(function() {

      failure(
        new Error("Deriv WebSocket timeout.")
      );

    }, 15000);

    ws.on("open", function() {

      ws.send(JSON.stringify({
        ticks_history: symbol,
        end: endTime || "latest",
        count: TICKS_PER_BATCH,
        style: "ticks"
      }));

    });

    ws.on("message", function(message) {

      try {

        var data =
          JSON.parse(message.toString());

        if (data.error) {

          clearTimeout(timeout);

          failure(
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

          success({
            times: data.history.times,
            prices: data.history.prices
          });
        }

      } catch (error) {

        clearTimeout(timeout);
        failure(error);

      }

    });

    ws.on("error", function(error) {

      clearTimeout(timeout);
      failure(error);

    });

    ws.on("close", function() {

      if (!finished) {

        clearTimeout(timeout);

        failure(
          new Error(
            "Deriv WebSocket closed unexpectedly."
          )
        );
      }

    });

  });
}


// ========================================================
// GET TICKS
// ========================================================

async function getDerivTicks(symbol, batches) {

  var allTicks = [];

  var endTime = "latest";

  for (var i = 0; i < batches; i++) {

    var batch =
      await getDerivHistoryBatch(
        symbol,
        endTime
      );

    if (
      !batch ||
      !batch.times ||
      !batch.prices
    ) {
      break;
    }

    for (
      var j = 0;
      j < batch.times.length;
      j++
    ) {

      allTicks.push({
        time: Number(batch.times[j]),
        price: Number(batch.prices[j])
      });

    }

    var oldest =
      Math.min.apply(
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


  allTicks =
    allTicks.filter(function(tick) {

      return (
        Number.isFinite(tick.time) &&
        Number.isFinite(tick.price)
      );

    });


  allTicks.sort(function(a, b) {

    return a.time - b.time;

  });


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
// CANDLES
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

    if (
      !current ||
      current.time !== bucket
    ) {

      if (current) {

        current.close =
          current.lastPrice;

        delete current.lastPrice;

        current.range =
          current.high -
          current.low;

        current.body =
          Math.abs(
            current.close -
            current.open
          );

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
        Math.max(
          current.high,
          tick.price
        );

      current.low =
        Math.min(
          current.low,
          tick.price
        );

      current.lastPrice =
        tick.price;
    }
  }


  if (current) {

    current.close =
      current.lastPrice;

    delete current.lastPrice;

    current.range =
      current.high -
      current.low;

    current.body =
      Math.abs(
        current.close -
        current.open
      );

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

  if (
    !candles ||
    candles.length < period
  ) {
    return null;
  }

  var multiplier =
    2 / (period + 1);

  var value = 0;

  for (
    var i = 0;
    i < period;
    i++
  ) {

    value +=
      candles[i].close;
  }

  value /= period;

  for (
    var j = period;
    j < candles.length;
    j++
  ) {

    value =
      (
        (candles[j].close - value) *
        multiplier
      ) + value;
  }

  return value;
}


// ========================================================
// RSI
// ========================================================

function rsi(candles, period) {

  if (
    !candles ||
    candles.length <= period
  ) {
    return null;
  }

  var gains = 0;
  var losses = 0;

  for (
    var i = 1;
    i <= period;
    i++
  ) {

    var difference =
      candles[i].close -
      candles[i - 1].close;

    if (difference >= 0) {

      gains += difference;

    } else {

      losses +=
        Math.abs(difference);
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
      change > 0
        ? change
        : 0;

    var loss =
      change < 0
        ? Math.abs(change)
        : 0;

    averageGain =
      (
        averageGain * (period - 1) +
        gain
      ) / period;

    averageLoss =
      (
        averageLoss * (period - 1) +
        loss
      ) / period;
  }

  if (averageLoss === 0) {
    return 100;
  }

  var rs =
    averageGain /
    averageLoss;

  return 100 -
    (100 / (1 + rs));
}


// ========================================================
// ATR
// ========================================================

function atr(candles, period) {

  if (
    !candles ||
    candles.length <= period
  ) {
    return null;
  }

  var trs = [];

  for (
    var i = 1;
    i < candles.length;
    i++
  ) {

    var current =
      candles[i];

    var previous =
      candles[i - 1];

    var tr =
      Math.max(
        current.high -
          current.low,

        Math.abs(
          current.high -
          previous.close
        ),

        Math.abs(
          current.low -
          previous.close
        )
      );

    trs.push(tr);
  }

  if (trs.length < period) {
    return null;
  }

  var value = 0;

  for (
    var j = 0;
    j < period;
    j++
  ) {

    value += trs[j];
  }

  value /= period;

  for (
    var k = period;
    k < trs.length;
    k++
  ) {

    value =
      (
        value * (period - 1) +
        trs[k]
      ) / period;
  }

  return value;
}


// ========================================================
// 5M TREND
// ========================================================

function getTrend(candles) {

  if (
    !candles ||
    candles.length < 30
  ) {

    return {
      direction: "RANGE",
      strength: 0
    };
  }

  var fast =
    ema(candles, 9);

  var slow =
    ema(candles, 21);

  var atrValue =
    atr(candles, 14);

  if (
    fast === null ||
    slow === null ||
    !atrValue ||
    atrValue <= 0
  ) {

    return {
      direction: "RANGE",
      strength: 0
    };
  }

  var distance =
    Math.abs(
      fast - slow
    );

  var separation =
    distance / atrValue;

  var strength =
    clamp(
      Math.round(
        separation * 100
      ),
      0,
      100
    );

  var last =
    candles[candles.length - 1];

  if (
    fast > slow &&
    last.close >= fast
  ) {

    return {
      direction: "BULLISH",
      strength: strength
    };
  }

  if (
    fast < slow &&
    last.close <= fast
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
// STRUCTURE
// ========================================================

function getStructure(candles) {

  if (
    !candles ||
    candles.length < 15
  ) {

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
    Math.max(
      2,
      candles.length - 30
    );

  var end =
    candles.length - 3;


  for (
    var i = start;
    i <= end;
    i++
  ) {

    if (
      candles[i].high >
      candles[i - 1].high &&
      candles[i].high >
      candles[i + 1].high
    ) {

      swingHigh =
        candles[i].high;
    }

    if (
      candles[i].low <
      candles[i - 1].low &&
      candles[i].low <
      candles[i + 1].low
    ) {

      swingLow =
        candles[i].low;
    }
  }


  var last =
    candles[candles.length - 1];

  var previous =
    candles[candles.length - 2];

  var atrValue =
    atr(candles, 14) || 0;

  var buffer =
    atrValue * 0.08;

  var bos = false;

  var bosDirection =
    "NONE";


  if (
    swingHigh !== null &&
    last.close >
      swingHigh + buffer &&
    previous.close <=
      swingHigh + buffer
  ) {

    bos = true;
    bosDirection =
      "BULLISH";
  }


  if (
    swingLow !== null &&
    last.close <
      swingLow - buffer &&
    previous.close >=
      swingLow - buffer
  ) {

    bos = true;
    bosDirection =
      "BEARISH";
  }


  var recent =
    candles.slice(
      Math.max(
        0,
        candles.length - 8
      )
    );


  var firstClose =
    recent[0].close;

  var lastClose =
    recent[
      recent.length - 1
    ].close;


  var direction =
    "RANGE";


  if (
    lastClose > firstClose &&
    last.close > last.open
  ) {

    direction =
      "BULLISH";
  }


  if (
    lastClose < firstClose &&
    last.close < last.open
  ) {

    direction =
      "BEARISH";
  }


  return {

    direction: direction,

    bos: bos,

    bosDirection:
      bosDirection,

    swingHigh:
      swingHigh,

    swingLow:
      swingLow
  };
}


// ========================================================
// LIQUIDITY
// ========================================================

function detectLiquiditySweep(candles) {

  if (
    !candles ||
    candles.length < 10
  ) {

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
    Math.max(
      2,
      candles.length - 8
    );


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


    // BUY-SIDE SWEEP
    if (
      candle.high >
        previousHigh &&
      candle.close <
        previousHigh
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
                wick / range * 100
              ),
              0,
              100
            )
          : 0;

      var age =
        candles.length -
        1 -
        i;

      if (
        strength >
        best.strength
      ) {

        best = {

          direction:
            "BUY-SIDE SWEEP",

          strength:
            strength,

          age:
            age
        };
      }
    }


    // SELL-SIDE SWEEP
    if (
      candle.low <
        previousLow &&
      candle.close >
        previousLow
    ) {

      var lowerWick =
        Math.min(
          candle.open,
          candle.close
        ) -
        candle.low;

      var lowerRange =
        candle.high -
        candle.low;

      var lowerStrength =
        lowerRange > 0
          ? clamp(
              Math.round(
                lowerWick /
                  lowerRange *
                  100
              ),
              0,
              100
            )
          : 0;

      var lowerAge =
        candles.length -
        1 -
        i;

      if (
        lowerStrength >
        best.strength
      ) {

        best = {

          direction:
            "SELL-SIDE SWEEP",

          strength:
            lowerStrength,

          age:
            lowerAge
        };
      }
    }
  }


  return best;
}


// ========================================================
// FVG
// ========================================================

function detectFVG(candles) {

  if (
    !candles ||
    candles.length < 5
  ) {

    return {

      direction: "NONE",

      strength: 0,

      age: null,

      gap: 0,

      active: false,

      freshness: "NONE"
    };
  }


  var best = {

    direction: "NONE",

    strength: 0,

    age: null,

    gap: 0,

    active: false,

    freshness: "NONE"
  };


  var start =
    Math.max(
      2,
      candles.length - 12
    );


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


    // BULLISH FVG
    if (
      right.low >
      left.high
    ) {

      var gap =
        right.low -
        left.high;

      var range =
        middle.high -
        middle.low;

      var rawStrength =
        range > 0
          ? clamp(
              Math.round(
                gap / range * 100
              ),
              0,
              100
            )
          : 0;

      var age =
        candles.length -
        1 -
        i;


      /*
      Freshness:

      0-2 = FRESH
      3-4 = RECENT
      5-6 = AGING
      7+  = OLD
      */

      var freshness =
        age <= 2
          ? "FRESH"
          : age <= 4
            ? "RECENT"
            : age <= 6
              ? "AGING"
              : "OLD";


      var active =
        candles[
          candles.length - 1
        ].close >
        left.high;


      /*
      Age-adjusted strength.
      */

      var adjustedStrength =
        rawStrength;

      if (age >= 3) {
        adjustedStrength *= 0.75;
      }

      if (age >= 5) {
        adjustedStrength *= 0.50;
      }

      if (age >= 7) {
        adjustedStrength *= 0.25;
      }

      adjustedStrength =
        Math.round(
          adjustedStrength
        );


      if (
        active &&
        adjustedStrength >
          best.strength
      ) {

        best = {

          direction:
            "BULLISH FVG",

          strength:
            adjustedStrength,

          rawStrength:
            rawStrength,

          age:
            age,

          gap:
            gap,

          active:
            true,

          freshness:
            freshness
        };
      }
    }


    // BEARISH FVG
    if (
      right.high <
      left.low
    ) {

      var bearishGap =
        left.low -
        right.high;

      var bearishRange =
        middle.high -
        middle.low;

      var bearishRaw =
        bearishRange > 0
          ? clamp(
              Math.round(
                bearishGap /
                  bearishRange *
                  100
              ),
              0,
              100
            )
          : 0;

      var bearishAge =
        candles.length -
        1 -
        i;


      var bearishFreshness =
        bearishAge <= 2
          ? "FRESH"
          : bearishAge <= 4
            ? "RECENT"
            : bearishAge <= 6
              ? "AGING"
              : "OLD";


      var bearishActive =
        candles[
          candles.length - 1
        ].close <
        left.low;


      var bearishAdjusted =
        bearishRaw;

      if (bearishAge >= 3) {
        bearishAdjusted *= 0.75;
      }

      if (bearishAge >= 5) {
        bearishAdjusted *= 0.50;
      }

      if (bearishAge >= 7) {
        bearishAdjusted *= 0.25;
      }

      bearishAdjusted =
        Math.round(
          bearishAdjusted
        );


      if (
        bearishActive &&
        bearishAdjusted >
          best.strength
      ) {

        best = {

          direction:
            "BEARISH FVG",

          strength:
            bearishAdjusted,

          rawStrength:
            bearishRaw,

          age:
            bearishAge,

          gap:
            bearishGap,

          active:
            true,

          freshness:
            bearishFreshness
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

  if (
    !candles ||
    candles.length < 8
  ) {

    return {
      direction: "NEUTRAL",
      strength: 0
    };
  }


  var recent =
    candles.slice(
      Math.max(
        0,
        candles.length - 5
      )
    );


  var bullish = 0;
  var bearish = 0;


  for (
    var i = 0;
    i < recent.length;
    i++
  ) {

    var candle =
      recent[i];

    var range =
      candle.high -
      candle.low;

    if (range <= 0) {
      continue;
    }

    var bodyRatio =
      candle.body /
      range;


    if (
      candle.close >
        candle.open &&
      bodyRatio >= 0.45
    ) {

      bullish++;
    }


    if (
      candle.close <
        candle.open &&
      bodyRatio >= 0.45
    ) {

      bearish++;
    }
  }


  var difference =
    Math.abs(
      bullish -
      bearish
    );


  var strength =
    clamp(
      difference * 20,
      0,
      100
    );


  if (
    bullish >= 3 &&
    bullish > bearish
  ) {

    return {

      direction:
        "BULLISH",

      strength:
        strength
    };
  }


  if (
    bearish >= 3 &&
    bearish > bullish
  ) {

    return {

      direction:
        "BEARISH",

      strength:
        strength
    };
  }


  return {

    direction:
      "NEUTRAL",

    strength:
      strength
  };
}


// ========================================================
// RSI CONTEXT
// ========================================================

function getRSIContext(candles) {

  if (
    !candles ||
    candles.length < 25
  ) {

    return {

      value: null,

      context: "N/A",

      recovery: false,

      recoveryDirection:
        "NONE"
    };
  }


  var current =
    rsi(candles, 14);


  var previousCandles =
    candles.slice(
      0,
      candles.length - 1
    );


  var previous =
    rsi(
      previousCandles,
      14
    );


  if (
    current === null ||
    previous === null
  ) {

    return {

      value:
        current,

      context:
        "N/A",

      recovery:
        false,

      recoveryDirection:
        "NONE"
    };
  }


  var change =
    current -
    previous;


  var context =
    "NEUTRAL";


  if (current < 30) {

    context =
      "OVERSOLD";

  } else if (current > 70) {

    context =
      "OVERBOUGHT";
  }


  var recovery =
    false;

  var direction =
    "NONE";


  if (
    previous < 35 &&
    current > previous &&
    change >= 1.5
  ) {

    recovery =
      true;

    direction =
      "BULLISH";
  }


  if (
    previous > 65 &&
    current < previous &&
    change <= -1.5
  ) {

    recovery =
      true;

    direction =
      "BEARISH";
  }


  return {

    value:
      current,

    context:
      context,

    recovery:
      recovery,

    recoveryDirection:
      direction
  };
}


// ========================================================
// EMA
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

    score += 50;
  }


  if (
    structure10s.bosDirection !==
      "NONE" &&
    structure10s.bosDirection ===
      momentum10s.direction
  ) {

    score += 25;
  }


  if (
    momentum10s.direction !==
      "NEUTRAL" &&
    momentum10s.strength >= 40
  ) {

    score += 15;
  }


  var last =
    candles10s[
      candles10s.length - 1
    ];


  if (last) {

    var range =
      last.high -
      last.low;

    if (
      range > 0 &&
      last.body / range >= 0.55
    ) {

      score += 10;
    }
  }


  score =
    clamp(
      score,
      0,
      100
    );


  var quality =
    "WEAK";


  if (score >= 75) {

    quality =
      "STRONG";

  } else if (score >= 50) {

    quality =
      "MODERATE";
  }


  return {

    score:
      score,

    quality:
      quality
  };
}


// ========================================================
// MARKET BIAS
// ========================================================

function calculateBias(
  trend5m,
  structure1m,
  momentum10s,
  fvg1m,
  emaState,
  liquidity1m
) {

  var buy = 0;
  var sell = 0;


  // 5M TREND
  if (
    trend5m.direction ===
    "BULLISH"
  ) {

    buy +=
      30 *
      (trend5m.strength / 100);

  } else if (
    trend5m.direction ===
    "BEARISH"
  ) {

    sell +=
      30 *
      (trend5m.strength / 100);
  }


  // 1M STRUCTURE
  if (
    structure1m.direction ===
    "BULLISH"
  ) {

    buy += 25;

  } else if (
    structure1m.direction ===
    "BEARISH"
  ) {

    sell += 25;
  }


  // MOMENTUM
  if (
    momentum10s.direction ===
    "BULLISH"
  ) {

    buy +=
      15 *
      (momentum10s.strength / 100);

  } else if (
    momentum10s.direction ===
    "BEARISH"
  ) {

    sell +=
      15 *
      (momentum10s.strength / 100);
  }


  // EMA
  if (
    emaState === "Bullish"
  ) {

    buy += 10;

  } else if (
    emaState === "Bearish"
  ) {

    sell += 10;
  }


  // FVG
  if (
    fvg1m.direction ===
      "BULLISH FVG" &&
    fvg1m.active
  ) {

    buy +=
      10 *
      (fvg1m.strength / 100);

  } else if (
    fvg1m.direction ===
      "BEARISH FVG" &&
    fvg1m.active
  ) {

    sell +=
      10 *
      (fvg1m.strength / 100);
  }


  // LIQUIDITY
  if (
    liquidity1m.direction ===
    "SELL-SIDE SWEEP"
  ) {

    buy +=
      10 *
      (
        liquidity1m.strength /
        100
      );

  } else if (
    liquidity1m.direction ===
    "BUY-SIDE SWEEP"
  ) {

    sell +=
      10 *
      (
        liquidity1m.strength /
        100
      );
  }


  buy =
    Math.round(
      clamp(
        buy,
        0,
        100
      )
    );

  sell =
    Math.round(
      clamp(
        sell,
        0,
        100
      )
    );


  var difference =
    Math.abs(
      buy -
      sell
    );


  var bias =
    "RANGE";


  if (
    buy >= 55 &&
    buy >= sell + 10
  ) {

    bias =
      "BULLISH";

  } else if (
    sell >= 55 &&
    sell >= buy + 10
  ) {

    bias =
      "BEARISH";
  }


  return {

    bias:
      bias,

    buyBias:
      buy,

    sellBias:
      sell,

    biasStrength:
      Math.max(
        buy,
        sell
      )
  };
}


// ========================================================
// SETUP STATE
// ========================================================

function determineSetup(
  bias,
  trend5m,
  structure1m,
  liquidity1m,
  fvg1m,
  momentum10s,
  structure10s
) {

  /*
  --------------------------------------------------------
  BULLISH CONTINUATION
  --------------------------------------------------------
  */

  if (
    bias.bias ===
      "BULLISH" &&

    trend5m.direction ===
      "BULLISH" &&

    structure1m.direction ===
      "BULLISH"
  ) {

    if (
      structure10s.bosDirection ===
        "BULLISH" &&
      momentum10s.direction ===
        "BULLISH"
    ) {

      return {

        type:
          "BULLISH CONTINUATION",

        direction:
          "BUY",

        state:
          "CONFIRMED"
      };
    }


    return {

      type:
        "BULLISH CONTINUATION",

      direction:
        "BUY",

      state:
        "DEVELOPING"
    };
  }


  /*
  --------------------------------------------------------
  BEARISH CONTINUATION
  --------------------------------------------------------
  */

  if (
    bias.bias ===
      "BEARISH" &&

    trend5m.direction ===
      "BEARISH" &&

    structure1m.direction ===
      "BEARISH"
  ) {

    if (
      structure10s.bosDirection ===
        "BEARISH" &&
      momentum10s.direction ===
        "BEARISH"
    ) {

      return {

        type:
          "BEARISH CONTINUATION",

        direction:
          "SELL",

        state:
          "CONFIRMED"
      };
    }


    return {

      type:
        "BEARISH CONTINUATION",

      direction:
        "SELL",

      state:
        "DEVELOPING"
    };
  }


  /*
  --------------------------------------------------------
  BULLISH REVERSAL
  --------------------------------------------------------
  */

  if (
    liquidity1m.direction ===
      "SELL-SIDE SWEEP" &&

    liquidity1m.age !== null &&
    liquidity1m.age <= 4
  ) {

    if (
      structure10s.bosDirection ===
        "BULLISH" &&
      momentum10s.direction ===
        "BULLISH"
    ) {

      return {

        type:
          "BULLISH REVERSAL",

        direction:
          "BUY",

        state:
          "CONFIRMED"
      };
    }


    return {

      type:
        "BULLISH REVERSAL",

      direction:
        "BUY",

      state:
        "DEVELOPING"
    };
  }


  /*
  --------------------------------------------------------
  BEARISH REVERSAL
  --------------------------------------------------------
  */

  if (
    liquidity1m.direction ===
      "BUY-SIDE SWEEP" &&

    liquidity1m.age !== null &&
    liquidity1m.age <= 4
  ) {

    if (
      structure10s.bosDirection ===
        "BEARISH" &&
      momentum10s.direction ===
        "BEARISH"
    ) {

      return {

        type:
          "BEARISH REVERSAL",

        direction:
          "SELL",

        state:
          "CONFIRMED"
      };
    }


    return {

      type:
        "BEARISH REVERSAL",

      direction:
        "SELL",

      state:
        "DEVELOPING"
    };
  }


  /*
  --------------------------------------------------------
  FVG + STRUCTURE DEVELOPING
  --------------------------------------------------------
  */

  if (
    bias.bias ===
      "BULLISH" &&

    fvg1m.direction ===
      "BULLISH FVG"
  ) {

    return {

      type:
        "BULLISH CONTINUATION",

      direction:
        "BUY",

      state:
        "DEVELOPING"
    };
  }


  if (
    bias.bias ===
      "BEARISH" &&

    fvg1m.direction ===
      "BEARISH FVG"
  ) {

    return {

      type:
        "BEARISH CONTINUATION",

      direction:
        "SELL",

      state:
        "DEVELOPING"
    };
  }


  return {

    type:
      "NO CONFIRMED SETUP",

    direction:
      "WAIT",

    state:
      "NONE"
  };
}


// ========================================================
// SETUP SCORE
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
  emaState
) {

  var expected =
    direction === "BUY"
      ? "BULLISH"
      : "BEARISH";


  var score = 0;

  var confirmations = 0;


  // 5M
  if (
    trend5m.direction ===
    expected
  ) {

    score +=
      Math.round(
        25 *
        (
          trend5m.strength /
          100
        )
      );

    confirmations++;
  }


  // 1M structure
  if (
    structure1m.direction ===
    expected
  ) {

    score += 25;

    confirmations++;
  }


  // 10S BOS
  if (
    structure10s.bosDirection ===
    expected
  ) {

    score += 25;

    confirmations++;
  }


  // Momentum
  if (
    momentum10s.direction ===
      expected &&
    momentum10s.strength >= 40
  ) {

    score += 15;

    confirmations++;
  }


  // EMA
  var expectedEMA =
    direction === "BUY"
      ? "Bullish"
      : "Bearish";


  if (
    emaState ===
    expectedEMA
  ) {

    score += 5;
  }


  // FVG
  var expectedFVG =
    direction === "BUY"
      ? "BULLISH FVG"
      : "BEARISH FVG";


  if (
    fvg1m.direction ===
      expectedFVG &&
    fvg1m.active &&
    fvg1m.strength >= 40
  ) {

    score += 5;
  }


  // Reversal liquidity
  if (
    setup.indexOf(
      "REVERSAL"
    ) >= 0
  ) {

    var expectedSweep =
      direction === "BUY"
        ? "SELL-SIDE SWEEP"
        : "BUY-SIDE SWEEP";


    if (
      liquidity1m.direction ===
        expectedSweep &&
      liquidity1m.age <= 4
    ) {

      score += 10;

      confirmations++;
    }
  }


  return {

    score:
      clamp(
        Math.round(score),
        0,
        100
      ),

    confirmations:
      confirmations
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
      Math.max(
        0,
        candles.length - 12
      )
    );


  var entry =
    candles[
      candles.length - 1
    ].close;


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


  var buffer =
    atrValue * 0.35;


  var stopLoss;
  var takeProfit;


  if (
    direction === "BUY"
  ) {

    stopLoss =
      low - buffer;

    var risk =
      entry - stopLoss;

    if (risk <= 0) {

      return {

        entry:
          roundNumber(entry, 5),

        stopLoss:
          null,

        takeProfit:
          null,

        rr:
          0
      };
    }


    takeProfit =
      entry +
      risk * 2;

  } else {

    stopLoss =
      high + buffer;

    var sellRisk =
      stopLoss -
      entry;


    if (sellRisk <= 0) {

      return {

        entry:
          roundNumber(entry, 5),

        stopLoss:
          null,

        takeProfit:
          null,

        rr:
          0
      };
    }


    takeProfit =
      entry -
      sellRisk * 2;
  }


  return {

    entry:
      roundNumber(entry, 5),

    stopLoss:
      roundNumber(stopLoss, 5),

    takeProfit:
      roundNumber(takeProfit, 5),

    rr:
      2
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


  if (
    !ticks ||
    ticks.length < 100
  ) {

    throw new Error(
      "Not enough market data."
    );
  }


  var candles10s =
    buildCandles(
      ticks,
      10
    );


  var candles1m =
    buildCandles(
      ticks,
      60
    );


  var candles5m =
    buildCandles(
      ticks,
      300
    );


  if (
    candles10s.length < 30 ||
    candles1m.length < 30 ||
    candles5m.length < 20
  ) {

    throw new Error(
      "Not enough candles."
    );
  }


  // ======================================================
  // TIMEFRAME ANALYSIS
  // ======================================================

  var trend5m =
    getTrend(
      candles5m
    );


  var structure1m =
    getStructure(
      candles1m
    );


  var structure10s =
    getStructure(
      candles10s
    );


  var liquidity1m =
    detectLiquiditySweep(
      candles1m
    );


  var fvg1m =
    detectFVG(
      candles1m
    );


  var momentum10s =
    getMomentum(
      candles10s
    );


  var rsiContext =
    getRSIContext(
      candles1m
    );


  var emaState =
    getEMAState(
      candles1m
    );


  var atrValue =
    atr(
      candles1m,
      14
    );


  var trigger =
    getTriggerQuality(
      candles10s,
      structure10s,
      momentum10s
    );


  // ======================================================
  // BIAS
  // ======================================================

  var bias =
    calculateBias(
      trend5m,
      structure1m,
      momentum10s,
      fvg1m,
      emaState,
      liquidity1m
    );


  // ======================================================
  // SETUP
  // ======================================================

  var setup =
    determineSetup(
      bias,
      trend5m,
      structure1m,
      liquidity1m,
      fvg1m,
      momentum10s,
      structure10s
    );


  var action =
    "WAIT";


  var marketState =
    "WAIT";


  var confidence =
    "LOW";


  var score =
    0;


  var confirmations =
    0;


  var levels = {

    entry:
      null,

    stopLoss:
      null,

    takeProfit:
      null,

    rr:
      0
  };


  var reason =
    "Conditions are not sufficiently aligned.";


  var watchFor = [];


  // ======================================================
  // SETUP SCORING
  // ======================================================

  if (
    setup.direction ===
      "BUY" ||
    setup.direction ===
      "SELL"
  ) {

    var scored =
      scoreSetup(
        setup.direction,
        setup.type,
        trend5m,
        structure1m,
        structure10s,
        liquidity1m,
        fvg1m,
        momentum10s,
        emaState
      );


    score =
      scored.score;


    confirmations =
      scored.confirmations;


    /*
    ------------------------------------------------------
    CONFIRMED BUY
    ------------------------------------------------------
    */

    if (
      setup.direction ===
        "BUY" &&
      setup.state ===
        "CONFIRMED" &&
      score >= 70 &&
      trigger.score >= 60
    ) {

      action =
        "BUY";

      marketState =
        "BUY SIGNAL";

      levels =
        calculateLevels(
          "BUY",
          candles1m,
          atrValue
        );


      if (
        score >= 82 &&
        trigger.score >= 75
      ) {

        confidence =
          "HIGH";

      } else {

        confidence =
          "MEDIUM";
      }


      reason =
        "Bullish bias, structure and entry trigger are aligned.";
    }


    /*
    ------------------------------------------------------
    CONFIRMED SELL
    ------------------------------------------------------
    */

    else if (
      setup.direction ===
        "SELL" &&
      setup.state ===
        "CONFIRMED" &&
      score >= 70 &&
      trigger.score >= 60
    ) {

      action =
        "SELL";

      marketState =
        "SELL SIGNAL";

      levels =
        calculateLevels(
          "SELL",
          candles1m,
          atrValue
        );


      if (
        score >= 82 &&
        trigger.score >= 75
      ) {

        confidence =
          "HIGH";

      } else {

        confidence =
          "MEDIUM";
      }


      reason =
        "Bearish bias, structure and entry trigger are aligned.";
    }


    /*
    ------------------------------------------------------
    DEVELOPING SETUP
    ------------------------------------------------------
    */

    else {

      action =
        "WAIT";

      marketState =
        setup.direction ===
          "BUY"
          ? "BULLISH SETUP DEVELOPING"
          : "BEARISH SETUP DEVELOPING";


      if (
        setup.type ===
          "BULLISH CONTINUATION"
      ) {

        reason =
          "Bullish conditions are developing, but a confirmed bullish entry trigger is still missing.";

      } else if (
        setup.type ===
          "BEARISH CONTINUATION"
      ) {

        reason =
          "Bearish conditions are developing, but a confirmed bearish entry trigger is still missing.";

      } else if (
        setup.type ===
          "BULLISH REVERSAL"
      ) {

        reason =
          "A possible bullish reversal is developing, but bullish structure and trigger confirmation are missing.";

      } else if (
        setup.type ===
          "BEARISH REVERSAL"
      ) {

        reason =
          "A possible bearish reversal is developing, but bearish structure and trigger confirmation are missing.";
      }
    }

  } else {

    marketState =
      bias.bias ===
        "BULLISH"
        ? "BULLISH BIAS"
        : bias.bias ===
          "BEARISH"
            ? "BEARISH BIAS"
            : "RANGE / MIXED";
  }


  // ======================================================
  // WATCH FOR
  // ======================================================

  if (
    bias.bias ===
      "BULLISH"
  ) {

    if (
      structure10s.bosDirection !==
        "BULLISH"
    ) {

      watchFor.push(
        "Bullish 10S BOS"
      );
    }


    if (
      momentum10s.direction !==
        "BULLISH" ||
      momentum10s.strength < 40
    ) {

      watchFor.push(
        "Stronger bullish momentum"
      );
    }

  } else if (
    bias.bias ===
      "BEARISH"
  ) {

    if (
      structure10s.bosDirection !==
        "BEARISH"
    ) {

      watchFor.push(
        "Bearish 10S BOS"
      );
    }


    if (
      momentum10s.direction !==
        "BEARISH" ||
      momentum10s.strength < 40
    ) {

      watchFor.push(
        "Stronger bearish momentum"
      );
    }

  } else {

    watchFor.push(
      "Clear directional bias"
    );
  }


  if (
    trend5m.direction ===
      "RANGE" ||
    trend5m.strength < 20
  ) {

    watchFor.push(
      "5M directional breakout"
    );
  }


  if (
    structure1m.direction ===
      "RANGE"
  ) {

    watchFor.push(
      "1M directional structure"
    );
  }


  if (
    trigger.score < 60
  ) {

    watchFor.push(
      "Stronger entry trigger"
    );
  }


  /*
  Remove duplicates.
  */

  watchFor =
    watchFor.filter(
      function(item, index, array) {

        return (
          array.indexOf(item) ===
          index
        );
      }
    );


  // ======================================================
  // FINAL RESPONSE
  // ======================================================

  return {

    success:
      true,

    symbol:
      symbol,

    dataSource:
      "Deriv public market data",

    mode:
      "paper analysis only",

    version:
      "V3.3",


    history: {

      ticks:
        ticks.length,

      batches:
        HISTORY_BATCHES
    },


    candles: {

      tenSecond:
        candles10s.length,

      oneMinute:
        candles1m.length,

      fiveMinute:
        candles5m.length
    },


    analysis: {

      bias:
        bias.bias,

      biasStrength:
        bias.biasStrength,

      buyBias:
        bias.buyBias,

      sellBias:
        bias.sellBias,


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


      triggerQuality:
        trigger.quality,

      triggerScore:
        trigger.score,


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

      fvgRawStrength:
        fvg1m.rawStrength !== undefined
          ? fvg1m.rawStrength
          : 0,

      fvgAge:
        fvg1m.age,

      fvgFreshness:
        fvg1m.freshness,

      fvgActive:
        fvg1m.active,


      momentum:
        momentum10s.direction,

      momentumStrength:
        momentum10s.strength,


      ema:
        emaState,


      rsi:
        rsiContext.value !== null
          ? roundNumber(
              rsiContext.value,
              2
            )
          : null,

      rsiContext:
        rsiContext.context,

      rsiRecovery:
        rsiContext.recovery,

      rsiRecoveryDirection:
        rsiContext.recoveryDirection,


      atr:
        atrValue !== null
          ? roundNumber(
              atrValue,
              5
            )
          : null
    },


    conclusion: {

      action:
        action,

      marketState:
        marketState,

      setupType:
        setup.type,

      setupState:
        setup.state,

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

    success:
      true,

    service:
      "Keamz Fx",

    status:
      "online",

    version:
      "V3.3",

    mode:
      "paper analysis only"
  });

});


// ========================================================
// SIGNAL
// ========================================================

app.get(
  "/signal",
  async function(req, res) {

    try {

      var symbol =
        String(
          req.query.symbol ||
          DEFAULT_SYMBOL
        ).trim();


      if (!symbol) {
        symbol =
          DEFAULT_SYMBOL;
      }


      console.log(
        "Keamz Fx V3.3 analysing: " +
        symbol
      );


      var result =
        await analyse(
          symbol
        );


      res.json(
        result
      );


    } catch (error) {

      console.error(
        "Signal error:",
        error
      );


      res.status(500).json({

        success:
          false,

        error:
          error.message ||
          "Analysis failed.",

        version:
          "V3.3",

        mode:
          "paper analysis only"
      });
    }
  }
);


// ========================================================
// START
// ========================================================

app.listen(
  PORT,
  "0.0.0.0",
  function() {

    console.log(
      "Keamz Fx V3.3 running on port " +
      PORT
    );

  }
);
