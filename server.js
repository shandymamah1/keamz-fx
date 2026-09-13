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
 KEAMZ FX V3.4
==========================================================

5M  = MARKET BIAS
1M  = SETUP / STRUCTURE
10S = ENTRY TRIGGER

V3.4 introduces:

• Market bias
• Bias strength
• Setup formation
• Setup stage
• Trigger quality
• Continuation setups
• Reversal setups
• Liquidity confirmation
• FVG confirmation
• Momentum confirmation
• RSI recovery
• Early setup detection
• Strict entry confirmation

IMPORTANT:
This is PAPER ANALYSIS ONLY.
It does not guarantee profitable trades.
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

  for (var i = 0; i < ticks.length; i++) {

    var tick = ticks[i];

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

    var change =
      candles[i].close -
      candles[i - 1].close;

    if (change >= 0) {

      gains += change;

    } else {

      losses +=
        Math.abs(change);
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
    (100 / (1 + rs))
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
// TREND
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
    distance /
    atrValue;

  var strength =
    clamp(
      Math.round(
        normalized * 100
      ),
      0,
      100
    );

  var current =
    candles[
      candles.length - 1
    ].close;

  /*
  ------------------------------------------
  BULLISH
  ------------------------------------------
  */

  if (
    fast > slow &&
    current >= fast
  ) {

    return {
      direction: "BULLISH",
      strength: strength
    };
  }

  /*
  ------------------------------------------
  BEARISH
  ------------------------------------------
  */

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
    atrValue * 0.08;

  var bos = false;

  var bosDirection =
    "NONE";

  /*
  ------------------------------------------
  BULLISH BOS
  ------------------------------------------
  */

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

  /*
  ------------------------------------------
  BEARISH BOS
  ------------------------------------------
  */

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
  ------------------------------------------
  RECENT STRUCTURE
  ------------------------------------------
  */

  var recent =
    candles.slice(
      Math.max(
        0,
        candles.length - 8
      )
    );

  var first =
    recent[0].close;

  var lastClose =
    recent[
      recent.length - 1
    ].close;

  var direction =
    "RANGE";

  if (
    lastClose > first
  ) {

    direction =
      "BULLISH";
  }

  if (
    lastClose < first
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

    /*
    BUY-SIDE SWEEP
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
                (
                  upperWick /
                  range
                ) * 100
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
                (
                  lowerWick /
                  lowerRange
                ) * 100
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
                (
                  gap /
                  range
                ) * 100
              ),
              0,
              100
            )
          : 0;

      var active =
        candles[
          candles.length - 1
        ].close >
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
                (
                  bearishGap /
                  bearishRange
                ) * 100
              ),
              0,
              100
            )
          : 0;

      var bearishActive =
        candles[
          candles.length - 1
        ].close <
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

      direction:
        "NEUTRAL",

      strength:
        0
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

    if (
      range <= 0
    ) {
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

      value: null,

      context:
        "N/A",

      recovery:
        false,

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
    current - previous;

  var context =
    "NEUTRAL";

  if (
    current < 30
  ) {

    context =
      "OVERSOLD";

  } else if (
    current > 70
  ) {

    context =
      "OVERBOUGHT";
  }

  var recovery =
    false;

  var recoveryDirection =
    "NONE";

  if (
    previous < 35 &&
    current > previous &&
    change >= 1.5
  ) {

    recovery =
      true;

    recoveryDirection =
      "BULLISH";
  }

  if (
    previous > 65 &&
    current < previous &&
    change <= -1.5
  ) {

    recovery =
      true;

    recoveryDirection =
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

  if (
    fast > slow
  ) {

    return "Bullish";
  }

  if (
    fast < slow
  ) {

    return "Bearish";
  }

  return "Neutral";
}


// ========================================================
// BIAS ENGINE
// ========================================================

function calculateBias(
  trend5m,
  structure1m,
  emaState,
  momentum10s,
  fvg1m
) {

  var buy = 0;

  var sell = 0;


  /*
  5M TREND
  */

  if (
    trend5m.direction ===
    "BULLISH"
  ) {

    buy += 45;

  } else if (
    trend5m.direction ===
    "BEARISH"
  ) {

    sell += 45;
  }


  /*
  1M STRUCTURE
  */

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


  /*
  EMA
  */

  if (
    emaState ===
    "Bullish"
  ) {

    buy += 10;

  } else if (
    emaState ===
    "Bearish"
  ) {

    sell += 10;
  }


  /*
  MOMENTUM
  */

  if (
    momentum10s.direction ===
    "BULLISH"
  ) {

    buy += 10;

  } else if (
    momentum10s.direction ===
    "BEARISH"
  ) {

    sell += 10;
  }


  /*
  FVG
  */

  if (
    fvg1m.direction ===
      "BULLISH FVG" &&
    fvg1m.active
  ) {

    buy += 10;

  } else if (
    fvg1m.direction ===
      "BEARISH FVG" &&
    fvg1m.active
  ) {

    sell += 10;
  }


  buy =
    clamp(
      buy,
      0,
      100
    );

  sell =
    clamp(
      sell,
      0,
      100
    );


  var difference =
    Math.abs(
      buy - sell
    );

  var bias =
    "RANGE";

  var strength = 0;


  if (
    buy >= 55 &&
    buy > sell + 15
  ) {

    bias =
      "BULLISH";

    strength =
      buy;

  } else if (
    sell >= 55 &&
    sell > buy + 15
  ) {

    bias =
      "BEARISH";

    strength =
      sell;

  } else {

    /*
    A smaller difference is
    intentionally classified as RANGE.
    */

    bias =
      "RANGE";

    strength =
      clamp(
        Math.round(
          50 -
          difference / 2
        ),
        0,
        50
      );
  }


  return {

    bias:
      bias,

    strength:
      strength,

    buyBias:
      buy,

    sellBias:
      sell
  };
}


// ========================================================
// SETUP FORMATION
// ========================================================

function detectSetupFormation(
  bias,
  structure1m,
  liquidity1m,
  fvg1m,
  momentum10s,
  rsiContext,
  emaState
) {

  var bullish = 0;

  var bearish = 0;


  /*
  BULLISH CONDITIONS
  */

  if (
    bias.bias ===
    "BULLISH"
  ) {
    bullish += 30;
  }

  if (
    structure1m.direction ===
    "BULLISH"
  ) {
    bullish += 20;
  }

  if (
    emaState ===
    "Bullish"
  ) {
    bullish += 10;
  }

  if (
    fvg1m.direction ===
      "BULLISH FVG" &&
    fvg1m.active
  ) {
    bullish += 15;
  }

  if (
    momentum10s.direction ===
    "BULLISH"
  ) {
    bullish += 15;
  }

  if (
    liquidity1m.direction ===
      "SELL-SIDE SWEEP" &&
    liquidity1m.age <= 4
  ) {
    bullish += 20;
  }

  if (
    rsiContext.recoveryDirection ===
    "BULLISH"
  ) {
    bullish += 10;
  }


  /*
  BEARISH CONDITIONS
  */

  if (
    bias.bias ===
    "BEARISH"
  ) {
    bearish += 30;
  }

  if (
    structure1m.direction ===
    "BEARISH"
  ) {
    bearish += 20;
  }

  if (
    emaState ===
    "Bearish"
  ) {
    bearish += 10;
  }

  if (
    fvg1m.direction ===
      "BEARISH FVG" &&
    fvg1m.active
  ) {
    bearish += 15;
  }

  if (
    momentum10s.direction ===
    "BEARISH"
  ) {
    bearish += 15;
  }

  if (
    liquidity1m.direction ===
      "BUY-SIDE SWEEP" &&
    liquidity1m.age <= 4
  ) {
    bearish += 20;
  }

  if (
    rsiContext.recoveryDirection ===
    "BEARISH"
  ) {
    bearish += 10;
  }


  bullish =
    clamp(
      bullish,
      0,
      100
    );

  bearish =
    clamp(
      bearish,
      0,
      100
    );


  /*
  BULLISH FORMING
  */

  if (
    bullish >= 55 &&
    bullish > bearish + 10
  ) {

    return {

      type:
        "BULLISH CONTINUATION FORMING",

      direction:
        "BUY",

      score:
        bullish
    };
  }


  /*
  BEARISH FORMING
  */

  if (
    bearish >= 55 &&
    bearish > bullish + 10
  ) {

    return {

      type:
        "BEARISH CONTINUATION FORMING",

      direction:
        "SELL",

      score:
        bearish
    };
  }


  /*
  REVERSAL FORMING
  */

  if (
    liquidity1m.direction ===
      "SELL-SIDE SWEEP" &&
    liquidity1m.age <= 4 &&
    (
      structure1m.direction ===
      "BULLISH" ||
      momentum10s.direction ===
      "BULLISH"
    )
  ) {

    return {

      type:
        "BULLISH REVERSAL FORMING",

      direction:
        "BUY",

      score:
        Math.max(
          bullish,
          55
        )
    };
  }


  if (
    liquidity1m.direction ===
      "BUY-SIDE SWEEP" &&
    liquidity1m.age <= 4 &&
    (
      structure1m.direction ===
      "BEARISH" ||
      momentum10s.direction ===
      "BEARISH"
    )
  ) {

    return {

      type:
        "BEARISH REVERSAL FORMING",

      direction:
        "SELL",

      score:
        Math.max(
          bearish,
          55
        )
    };
  }


  return {

    type:
      "NO CLEAR SETUP",

    direction:
      "WAIT",

    score:
      Math.max(
        bullish,
        bearish
      )
  };
}


// ========================================================
// TRIGGER ENGINE
// ========================================================

function calculateTrigger(
  direction,
  structure10s,
  momentum10s,
  rsiContext
) {

  var expected =
    direction === "BUY"
      ? "BULLISH"
      : "BEARISH";

  var score = 0;


  /*
  BOS
  */

  if (
    structure10s.bosDirection ===
    expected
  ) {

    score += 55;
  }


  /*
  MOMENTUM
  */

  if (
    momentum10s.direction ===
    expected
  ) {

    score += 30;
  }


  /*
  RSI RECOVERY
  */

  if (
    rsiContext.recoveryDirection ===
    expected
  ) {

    score += 15;
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
    score >= 80
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
      quality,

    confirmed:
      score >= 70 &&
      structure10s.bosDirection ===
        expected &&
      momentum10s.direction ===
        expected
  };
}


// ========================================================
// LEVELS
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

    if (
      risk <= 0
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


  /*
  TIMEFRAMES
  */

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


  /*
  5M
  */

  var trend5m =
    getTrend(
      candles5m
    );


  /*
  1M
  */

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


  /*
  10S
  */

  var structure10s =
    getStructure(
      candles10s
    );

  var momentum10s =
    getMomentum(
      candles10s
    );


  /*
  BIAS
  */

  var bias =
    calculateBias(
      trend5m,
      structure1m,
      emaState,
      momentum10s,
      fvg1m
    );


  /*
  SETUP
  */

  var setup =
    detectSetupFormation(
      bias,
      structure1m,
      liquidity1m,
      fvg1m,
      momentum10s,
      rsiContext,
      emaState
    );


  /*
  DEFAULT
  */

  var action =
    "WAIT";

  var marketState =
    "WAIT";

  var setupType =
    "NO CONFIRMED SETUP";

  var confidence =
    "LOW";

  var score =
    0;

  var confirmations =
    0;

  var levels = {

    entry: null,

    stopLoss: null,

    takeProfit: null,

    rr: 0
  };

  var reason =
    "Conditions are not sufficiently aligned.";

  var watchFor = [];


  /*
  TRIGGER
  */

  var trigger =
    calculateTrigger(
      setup.direction,
      structure10s,
      momentum10s,
      rsiContext
    );


  /*
  SETUP FORMING
  */

  if (
    setup.direction ===
      "BUY" ||
    setup.direction ===
      "SELL"
  ) {

    marketState =
      "SETUP_FORMING";

    setupType =
      setup.type;

    score =
      setup.score;


    /*
    CONFIRMATION
    */

    if (
      trigger.confirmed
    ) {

      action =
        setup.direction;

      marketState =
        "TRIGGER_CONFIRMED";

      confidence =
        score >= 80 &&
        trigger.score >= 80
          ? "HIGH"
          : "MEDIUM";

      setupType =
        setup.type.replace(
          " FORMING",
          ""
        );

      confirmations = 4;

      levels =
        calculateLevels(
          action,
          candles1m,
          atrValue
        );


      if (
        action ===
        "BUY"
      ) {

        reason =
          "Bullish setup conditions aligned and the 10-second trigger confirmed the entry.";

      } else {

        reason =
          "Bearish setup conditions aligned and the 10-second trigger confirmed the entry.";
      }

    } else {

      action =
        "WAIT";

      marketState =
        "SETUP_FORMING";

      confidence =
        "LOW";

      confirmations = 0;


      if (
        setup.direction ===
        "BUY"
      ) {

        reason =
          "Bullish conditions are developing, but the entry trigger is not confirmed.";

      } else {

        reason =
          "Bearish conditions are developing, but the entry trigger is not confirmed.";
      }
    }

  } else {

    marketState =
      "WAIT";

    setupType =
      "NO CONFIRMED SETUP";
  }


  /*
  WATCH FOR
  */

  if (
    bias.bias ===
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
      "1M directional BOS"
    );
  }


  if (
    trigger.score < 70
  ) {

    watchFor.push(
      "Stronger entry trigger"
    );
  }


  if (
    momentum10s.direction ===
    "NEUTRAL"
  ) {

    watchFor.push(
      "Momentum confirmation"
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


  /*
  REMOVE DUPLICATES
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


  /*
  RESPONSE
  */

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
      "V3.4",


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
        bias.strength,

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

      triggerConfirmed:
        trigger.confirmed,

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

app.get(
  "/",
  function(req, res) {

    res.json({

      success:
        true,

      service:
        "Keamz Fx",

      status:
        "online",

      version:
        "V3.4",

      mode:
        "paper analysis only"
    });

  }
);


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
        "Keamz Fx V3.4 analysing: " +
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
          "V3.4",

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
      "Keamz Fx V3.4 running on port " +
      PORT
    );

  }
);
