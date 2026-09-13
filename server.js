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
KEAMZ FX V3.1
==========================================================

5M  = MARKET BIAS
1M  = SETUP / STRUCTURE
10S = ENTRY TRIGGER

STATES:

1. BULLISH BIAS
2. BEARISH BIAS
3. BULLISH SETUP DEVELOPING
4. BEARISH SETUP DEVELOPING
5. BULLISH CONTINUATION
6. BEARISH CONTINUATION
7. BULLISH REVERSAL
8. BEARISH REVERSAL
9. WAIT

The system deliberately separates:

BIAS
SETUP
TRIGGER

A strong market direction does NOT automatically create
a trade signal.

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

  var factor =
    Math.pow(10, decimals || 2);

  return Math.round(value * factor) / factor;
}


// ========================================================
// DERIV HISTORY
// ========================================================

function getDerivHistoryBatch(symbol, endTime) {

  return new Promise(function(resolve, reject) {

    var ws =
      new WebSocket(DERIV_WS);

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

    var timeout =
      setTimeout(function() {

        failure(
          new Error(
            "Deriv WebSocket timeout."
          )
        );

      }, 15000);

    ws.on("open", function() {

      ws.send(
        JSON.stringify({
          ticks_history: symbol,
          end: endTime || "latest",
          count: TICKS_PER_BATCH,
          style: "ticks"
        })
      );

    });

    ws.on("message", function(message) {

      try {

        var data =
          JSON.parse(
            message.toString()
          );

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

  for (
    var i = 0;
    i < batches;
    i++
  ) {

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
        time:
          Number(batch.times[j]),

        price:
          Number(batch.prices[j])
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

    endTime =
      oldest - 1;

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

  for (
    var k = 0;
    k < allTicks.length;
    k++
  ) {

    var key =
      String(allTicks[k].time) +
      "_" +
      String(allTicks[k].price);

    if (!seen[key]) {

      seen[key] = true;

      unique.push(
        allTicks[k]
      );
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

  for (
    var i = 0;
    i < ticks.length;
    i++
  ) {

    var tick =
      ticks[i];

    var bucket =
      Math.floor(
        tick.time / seconds
      ) * seconds;

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
          current.close >
          current.open
            ? "BULLISH"
            : current.close <
              current.open
              ? "BEARISH"
              : "NEUTRAL";

        candles.push(current);
      }

      current = {

        time: bucket,

        open:
          tick.price,

        high:
          tick.price,

        low:
          tick.price,

        close:
          tick.price,

        lastPrice:
          tick.price
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
      current.close >
      current.open
        ? "BULLISH"
        : current.close <
          current.open
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
        candles[j].close -
        value
      ) *
      multiplier +
      value;
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

    var change =
      candles[i].close -
      candles[i - 1].close;

    if (change >= 0) {
      gains += change;
    } else {
      losses += Math.abs(change);
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

    var difference =
      candles[j].close -
      candles[j - 1].close;

    var gain =
      difference > 0
        ? difference
        : 0;

    var loss =
      difference < 0
        ? Math.abs(difference)
        : 0;

    averageGain =
      (
        averageGain *
        (period - 1) +
        gain
      ) / period;

    averageLoss =
      (
        averageLoss *
        (period - 1) +
        loss
      ) / period;
  }

  if (averageLoss === 0) {
    return 100;
  }

  var rs =
    averageGain /
    averageLoss;

  return (
    100 -
    100 / (1 + rs)
  );
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
        value *
        (period - 1) +
        trs[k]
      ) / period;
  }

  return value;
}


// ========================================================
// 5M TREND / BIAS
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

  var current =
    candles[
      candles.length - 1
    ].close;

  if (
    fast === null ||
    slow === null
  ) {

    return {
      direction: "RANGE",
      strength: 0
    };
  }

  var atrValue =
    atr(candles, 14);

  if (
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

  var normalized =
    distance / atrValue;

  var strength =
    clamp(
      Math.round(
        normalized * 100
      ),
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
      candles.length - 35
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
    candles[
      candles.length - 1
    ];

  var previous =
    candles[
      candles.length - 2
    ];

  var atrValue =
    atr(candles, 14) || 0;

  var buffer =
    atrValue * 0.06;

  var bos = false;
  var bosDirection = "NONE";

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

  /*
  --------------------------------------------------------
  STRUCTURE DIRECTION

  We use recent price progression instead of requiring BOS.
  This allows the bot to recognize a developing setup.
  --------------------------------------------------------
  */

  var lookback =
    Math.min(
      8,
      candles.length
    );

  var first =
    candles[
      candles.length -
      lookback
    ].close;

  var lastClose =
    last.close;

  var direction =
    "RANGE";

  if (
    lastClose > first &&
    last.close >= last.open
  ) {

    direction =
      "BULLISH";
  }

  if (
    lastClose < first &&
    last.close <= last.open
  ) {

    direction =
      "BEARISH";
  }

  return {

    direction:
      direction,

    bos:
      bos,

    bosDirection:
      bosDirection,

    swingHigh:
      swingHigh,

    swingLow:
      swingLow
  };
}


// ========================================================
// LIQUIDITY SWEEP
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

    /*
    BUY-SIDE SWEEP
    Price takes previous high,
    then closes back below it.
    */

    if (
      candle.high >
        previousHigh &&
      candle.close <
        previousHigh
    ) {

      var upperWick =
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
                upperWick /
                range *
                100
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

    /*
    SELL-SIDE SWEEP
    Price takes previous low,
    then closes back above it.
    */

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

    /*
    BULLISH FVG
    */

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

      var strength =
        range > 0
          ? clamp(
              Math.round(
                gap /
                range *
                100
              ),
              0,
              100
            )
          : 0;

      /*
      FVG remains active while price
      has not fully broken through it.
      */

      var currentPrice =
        candles[
          candles.length - 1
        ].close;

      var active =
        currentPrice >
        left.high;

      if (
        active &&
        strength >=
        best.strength
      ) {

        best = {

          direction:
            "BULLISH FVG",

          strength:
            strength,

          age:
            candles.length -
            1 -
            i,

          gap:
            gap,

          active:
            true
        };
      }
    }


    /*
    BEARISH FVG
    */

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

      var bearishStrength =
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

      var bearishPrice =
        candles[
          candles.length - 1
        ].close;

      var bearishActive =
        bearishPrice <
        left.low;

      if (
        bearishActive &&
        bearishStrength >=
        best.strength
      ) {

        best = {

          direction:
            "BEARISH FVG",

          strength:
            bearishStrength,

          age:
            candles.length -
            1 -
            i,

          gap:
            bearishGap,

          active:
            true
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

  var strength =
    Math.abs(
      bullish -
      bearish
    ) * 20;

  if (
    bullish >= 3 &&
    bullish > bearish
  ) {

    return {
      direction:
        "BULLISH",

      strength:
        clamp(
          strength,
          0,
          100
        )
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
        clamp(
          strength,
          0,
          100
        )
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

      value:
        null,

      context:
        "N/A",

      recovery:
        false,

      recoveryDirection:
        "NONE"
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
    rsi(
      previousCandles,
      14
    );

  if (
    currentRSI === null ||
    previousRSI === null
  ) {

    return {

      value:
        currentRSI,

      context:
        "N/A",

      recovery:
        false,

      recoveryDirection:
        "NONE"
    };
  }

  var difference =
    currentRSI -
    previousRSI;

  var context =
    "NEUTRAL";

  if (
    currentRSI < 30
  ) {

    context =
      "OVERSOLD";

  } else if (
    currentRSI > 70
  ) {

    context =
      "OVERBOUGHT";
  }

  var recovery =
    false;

  var recoveryDirection =
    "NONE";

  if (
    previousRSI < 35 &&
    currentRSI >
      previousRSI &&
    difference >= 1.5
  ) {

    recovery =
      true;

    recoveryDirection =
      "BULLISH";
  }

  if (
    previousRSI > 65 &&
    currentRSI <
      previousRSI &&
    difference <= -1.5
  ) {

    recovery =
      true;

    recoveryDirection =
      "BEARISH";
  }

  return {

    value:
      currentRSI,

    context:
      context,

    recovery:
      recovery,

    recoveryDirection:
      recoveryDirection
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
// BIAS SCORE
// ========================================================

function getBiasScore(
  trend5m,
  structure1m,
  momentum10s,
  emaState
) {

  var buy = 0;
  var sell = 0;

  if (
    trend5m.direction ===
    "BULLISH"
  ) {

    buy += 35;

  } else if (
    trend5m.direction ===
    "BEARISH"
  ) {

    sell += 35;
  }


  if (
    structure1m.direction ===
    "BULLISH"
  ) {

    buy += 30;

  } else if (
    structure1m.direction ===
    "BEARISH"
  ) {

    sell += 30;
  }


  if (
    momentum10s.direction ===
    "BULLISH"
  ) {

    buy += 20;

  } else if (
    momentum10s.direction ===
    "BEARISH"
  ) {

    sell += 20;
  }


  if (
    emaState ===
    "Bullish"
  ) {

    buy += 15;

  } else if (
    emaState ===
    "Bearish"
  ) {

    sell += 15;
  }

  return {

    buy:
      clamp(
        buy,
        0,
        100
      ),

    sell:
      clamp(
        sell,
        0,
        100
      )
  };
}


// ========================================================
// SETUP DEVELOPMENT
// ========================================================

function detectDevelopingSetup(
  trend5m,
  structure1m,
  liquidity1m,
  fvg1m,
  momentum10s
) {

  /*
  BULLISH CONTINUATION DEVELOPING
  */

  var bullishPoints = 0;

  if (
    trend5m.direction ===
    "BULLISH"
  ) {
    bullishPoints += 2;
  }

  if (
    structure1m.direction ===
    "BULLISH"
  ) {
    bullishPoints += 2;
  }

  if (
    momentum10s.direction ===
    "BULLISH"
  ) {
    bullishPoints += 1;
  }

  if (
    fvg1m.direction ===
      "BULLISH FVG" &&
    fvg1m.active
  ) {
    bullishPoints += 1;
  }

  if (
    bullishPoints >= 4
  ) {

    return {
      type:
        "BULLISH CONTINUATION DEVELOPING",

      direction:
        "BUY"
    };
  }


  /*
  BEARISH CONTINUATION DEVELOPING
  */

  var bearishPoints = 0;

  if (
    trend5m.direction ===
    "BEARISH"
  ) {
    bearishPoints += 2;
  }

  if (
    structure1m.direction ===
    "BEARISH"
  ) {
    bearishPoints += 2;
  }

  if (
    momentum10s.direction ===
    "BEARISH"
  ) {
    bearishPoints += 1;
  }

  if (
    fvg1m.direction ===
      "BEARISH FVG" &&
    fvg1m.active
  ) {
    bearishPoints += 1;
  }

  if (
    bearishPoints >= 4
  ) {

    return {
      type:
        "BEARISH CONTINUATION DEVELOPING",

      direction:
        "SELL"
    };
  }


  /*
  BULLISH REVERSAL DEVELOPING
  */

  if (
    liquidity1m.direction ===
      "SELL-SIDE SWEEP" &&
    liquidity1m.age !== null &&
    liquidity1m.age <= 4
  ) {

    if (
      momentum10s.direction ===
      "BULLISH" ||
      structure1m.direction ===
      "BULLISH"
    ) {

      return {
        type:
          "BULLISH REVERSAL DEVELOPING",

        direction:
          "BUY"
      };
    }
  }


  /*
  BEARISH REVERSAL DEVELOPING
  */

  if (
    liquidity1m.direction ===
      "BUY-SIDE SWEEP" &&
    liquidity1m.age !== null &&
    liquidity1m.age <= 4
  ) {

    if (
      momentum10s.direction ===
      "BEARISH" ||
      structure1m.direction ===
      "BEARISH"
    ) {

      return {
        type:
          "BEARISH REVERSAL DEVELOPING",

        direction:
          "SELL"
      };
    }
  }


  return {
    type:
      "NO SETUP",

    direction:
      "WAIT"
  };
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
    "NEUTRAL"
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
      last.body /
        range >=
        0.55
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

  if (
    score >= 75
  ) {

    quality =
      "STRONG";

  } else if (
    score >= 50
  ) {

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
// CONFIRMED SETUP
// ========================================================

function getConfirmedSetup(
  developing,
  trend5m,
  structure1m,
  structure10s,
  liquidity1m,
  momentum10s,
  fvg1m,
  trigger
) {

  if (
    developing.direction !==
    "BUY" &&
    developing.direction !==
    "SELL"
  ) {

    return {
      type:
        "NO CONFIRMED SETUP",

      direction:
        "WAIT"
    };
  }

  var direction =
    developing.direction;

  var expected =
    direction === "BUY"
      ? "BULLISH"
      : "BEARISH";


  /*
  CONTINUATION
  */

  if (
    developing.type.indexOf(
      "CONTINUATION"
    ) >= 0
  ) {

    var continuationConfirmed =
      trend5m.direction ===
        expected &&

      structure1m.direction ===
        expected &&

      (
        structure1m.bosDirection ===
          expected ||

        structure10s.bosDirection ===
          expected
      ) &&

      momentum10s.direction ===
        expected &&

      trigger.score >= 60;

    if (
      continuationConfirmed
    ) {

      return {

        type:
          direction === "BUY"
            ? "BULLISH CONTINUATION"
            : "BEARISH CONTINUATION",

        direction:
          direction
      };
    }
  }


  /*
  REVERSAL
  */

  if (
    developing.type.indexOf(
      "REVERSAL"
    ) >= 0
  ) {

    var expectedSweep =
      direction === "BUY"
        ? "SELL-SIDE SWEEP"
        : "BUY-SIDE SWEEP";

    var reversalConfirmed =
      liquidity1m.direction ===
        expectedSweep &&

      liquidity1m.age !== null &&
      liquidity1m.age <= 4 &&

      (
        structure1m.bosDirection ===
          expected ||

        structure10s.bosDirection ===
          expected
      ) &&

      momentum10s.direction ===
        expected &&

      trigger.score >= 60;

    if (
      reversalConfirmed
    ) {

      return {

        type:
          direction === "BUY"
            ? "BULLISH REVERSAL"
            : "BEARISH REVERSAL",

        direction:
          direction
      };
    }
  }


  return {
    type:
      "NO CONFIRMED SETUP",

    direction:
      "WAIT"
  };
}


// ========================================================
// SCORE
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


  if (
    trend5m.direction ===
    expected
  ) {

    score += 25;
    confirmations++;
  }


  if (
    structure1m.direction ===
    expected
  ) {

    score += 20;
    confirmations++;
  }


  if (
    structure1m.bosDirection ===
    expected
  ) {

    score += 20;
    confirmations++;

  } else if (
    structure10s.bosDirection ===
    expected
  ) {

    score += 15;
    confirmations++;
  }


  if (
    momentum10s.direction ===
    expected
  ) {

    score += 15;
    confirmations++;
  }


  var expectedFVG =
    direction === "BUY"
      ? "BULLISH FVG"
      : "BEARISH FVG";

  if (
    fvg1m.direction ===
      expectedFVG &&
    fvg1m.active
  ) {

    score += 8;
    confirmations++;
  }


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
      liquidity1m.age !== null &&
      liquidity1m.age <= 4
    ) {

      score += 12;
      confirmations++;
    }
  }


  if (
    rsiContext.recovery &&
    rsiContext.recoveryDirection ===
      expected
  ) {

    score += 5;
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
      entry -
      stopLoss;

    if (risk <= 0) {

      return {
        entry:
          roundNumber(
            entry,
            5
          ),

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

    if (
      sellRisk <= 0
    ) {

      return {
        entry:
          roundNumber(
            entry,
            5
          ),

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
      roundNumber(
        entry,
        5
      ),

    stopLoss:
      roundNumber(
        stopLoss,
        5
      ),

    takeProfit:
      roundNumber(
        takeProfit,
        5
      ),

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
      "Not enough candles for analysis."
    );
  }


  // ------------------------------------------------------
  // 5M
  // ------------------------------------------------------

  var trend5m =
    getTrend(
      candles5m
    );


  // ------------------------------------------------------
  // 1M
  // ------------------------------------------------------

  var structure1m =
    getStructure(
      candles1m
    );

  var liquidity1m =
    detectLiquiditySweep(
      candles1m
    );

  var fvg1m =
    detectFVG(
      candles1m
    );

  var emaState =
    getEMAState(
      candles1m
    );

  var rsiContext =
    getRSIContext(
      candles1m
    );

  var atrValue =
    atr(
      candles1m,
      14
    );


  // ------------------------------------------------------
  // 10S
  // ------------------------------------------------------

  var structure10s =
    getStructure(
      candles10s
    );

  var momentum10s =
    getMomentum(
      candles10s
    );

  var trigger =
    getTriggerQuality(
      candles10s,
      structure10s,
      momentum10s
    );


  // ------------------------------------------------------
  // BIAS
  // ------------------------------------------------------

  var bias =
    getBiasScore(
      trend5m,
      structure1m,
      momentum10s,
      emaState
    );


  var biasDirection =
    "RANGE";

  var biasStrength =
    Math.max(
      bias.buy,
      bias.sell
    );

  if (
    bias.buy >= 60 &&
    bias.buy >
      bias.sell + 10
  ) {

    biasDirection =
      "BULLISH";

  } else if (
    bias.sell >= 60 &&
    bias.sell >
      bias.buy + 10
  ) {

    biasDirection =
      "BEARISH";
  }


  // ------------------------------------------------------
  // DEVELOPING SETUP
  // ------------------------------------------------------

  var developing =
    detectDevelopingSetup(
      trend5m,
      structure1m,
      liquidity1m,
      fvg1m,
      momentum10s
    );


  // ------------------------------------------------------
  // CONFIRMED SETUP
  // ------------------------------------------------------

  var confirmed =
    getConfirmedSetup(
      developing,
      trend5m,
      structure1m,
      structure10s,
      liquidity1m,
      momentum10s,
      fvg1m,
      trigger
    );


  // ------------------------------------------------------
  // DEFAULT
  // ------------------------------------------------------

  var action =
    "WAIT";

  var setupType =
    "NO CONFIRMED SETUP";

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


  // ------------------------------------------------------
  // CONFIRMED TRADE
  // ------------------------------------------------------

  if (
    confirmed.direction ===
      "BUY" ||
    confirmed.direction ===
      "SELL"
  ) {

    var scored =
      scoreSetup(
        confirmed.direction,
        confirmed.type,
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
      scored.score;

    confirmations =
      scored.confirmations;


    if (
      score >= 65 &&
      confirmations >= 4 &&
      trigger.score >= 60
    ) {

      action =
        confirmed.direction;

      setupType =
        confirmed.type;

      marketState =
        "TRIGGER CONFIRMED";

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

        confidence =
          "HIGH";

      } else {

        confidence =
          "MEDIUM";
      }


      if (
        confirmed.type ===
        "BULLISH CONTINUATION"
      ) {

        reason =
          "Bullish 5M bias, bullish 1M structure and a confirmed bullish entry trigger are aligned.";

      } else if (
        confirmed.type ===
        "BEARISH CONTINUATION"
      ) {

        reason =
          "Bearish 5M bias, bearish 1M structure and a confirmed bearish entry trigger are aligned.";

      } else if (
        confirmed.type ===
        "BULLISH REVERSAL"
      ) {

        reason =
          "Sell-side liquidity was swept, followed by bullish structure and momentum confirmation.";

      } else if (
        confirmed.type ===
        "BEARISH REVERSAL"
      ) {

        reason =
          "Buy-side liquidity was swept, followed by bearish structure and momentum confirmation.";
      }

    }
  }


  // ------------------------------------------------------
  // DEVELOPING SETUP
  // ------------------------------------------------------

  if (
    action ===
      "WAIT" &&
    developing.direction !==
      "WAIT"
  ) {

    setupType =
      developing.type;

    marketState =
      "SETUP DEVELOPING";

    if (
      developing.direction ===
      "BUY"
    ) {

      action =
        "WAIT";

      reason =
        "Bullish conditions are developing, but the entry trigger is not confirmed.";

    } else {

      action =
        "WAIT";

      reason =
        "Bearish conditions are developing, but the entry trigger is not confirmed.";
    }
  }


  // ------------------------------------------------------
  // BIAS ONLY
  // ------------------------------------------------------

  if (
    action ===
      "WAIT" &&
    developing.direction ===
      "WAIT" &&
    biasDirection !==
      "RANGE"
  ) {

    marketState =
      biasDirection ===
        "BULLISH"
        ? "BULLISH BIAS"
        : "BEARISH BIAS";

    setupType =
      biasDirection ===
        "BULLISH"
        ? "BULLISH BIAS"
        : "BEARISH BIAS";

    reason =
      biasDirection ===
        "BULLISH"
        ? "The broader market bias is bullish, but no confirmed setup is currently present."
        : "The broader market bias is bearish, but no confirmed setup is currently present.";
  }


  // ------------------------------------------------------
  // WATCH FOR
  // ------------------------------------------------------

  if (
    trend5m.direction ===
    "RANGE"
  ) {

    watchFor.push(
      "5M directional breakout"
    );
  }

  if (
    structure1m.bosDirection ===
    "NONE"
  ) {

    watchFor.push(
      biasDirection ===
        "BULLISH"
        ? "Bullish 1M BOS"
        : biasDirection ===
          "BEARISH"
          ? "Bearish 1M BOS"
          : "1M directional BOS"
    );
  }

  if (
    momentum10s.direction ===
    "NEUTRAL"
  ) {

    watchFor.push(
      "10S momentum confirmation"
    );
  }

  if (
    trigger.score <
    60
  ) {

    watchFor.push(
      "Stronger entry trigger"
    );
  }

  if (
    fvg1m.direction ===
    "NONE"
  ) {

    watchFor.push(
      "Active FVG"
    );
  }

  if (
    liquidity1m.direction !==
      "NONE" &&
    liquidity1m.age !== null &&
    liquidity1m.age > 4
  ) {

    watchFor.push(
      "Fresh liquidity confirmation"
    );
  }


  watchFor =
    watchFor.filter(
      function(item, index, array) {

        return (
          array.indexOf(item) ===
          index
        );

      }
    );


  // ------------------------------------------------------
  // RESPONSE
  // ------------------------------------------------------

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
      "V3.1",


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

      /*
      MARKET BIAS
      */

      bias:
        biasDirection,

      biasStrength:
        biasStrength,

      buyBias:
        bias.buy,

      sellBias:
        bias.sell,


      /*
      5M
      */

      trend5m:
        trend5m.direction,

      trend5mStrength:
        trend5m.strength,


      /*
      1M
      */

      structure1m:
        structure1m.direction,

      bos:
        structure1m.bos,

      bosDirection:
        structure1m.bosDirection,


      /*
      10S
      */

      triggerBosDirection:
        structure10s.bosDirection,

      triggerQuality:
        trigger.quality,

      triggerScore:
        trigger.score,


      /*
      LIQUIDITY
      */

      liquidity:
        liquidity1m.direction,

      liquidityStrength:
        liquidity1m.strength,

      liquidityAge:
        liquidity1m.age,


      /*
      FVG
      */

      fvg:
        fvg1m.direction,

      fvgStrength:
        fvg1m.strength,

      fvgAge:
        fvg1m.age,

      fvgActive:
        fvg1m.active,


      /*
      MOMENTUM
      */

      momentum:
        momentum10s.direction,

      momentumStrength:
        momentum10s.strength,


      /*
      EMA
      */

      ema:
        emaState,


      /*
      RSI
      */

      rsi:
        rsiContext.value !==
          null
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


      /*
      VOLATILITY
      */

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
        setupType,

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
      "V3.1",

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
        "Analysing instrument: " +
        symbol
      );

      var result =
        await analyse(
          symbol
        );

      res.json(result);

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

        mode:
          "paper analysis only",

        version:
          "V3.1"
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
      "Keamz Fx V3.1 running on port " +
      PORT
    );

  }
);
