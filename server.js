const express = require("express");
const WebSocket = require("ws");

const app = express();

const PORT = process.env.PORT || 3000;

const DERIV_WS =
  "wss://api.derivws.com/trading/v1/options/ws/public";

const DEFAULT_SYMBOL = "stpRNG";

const HISTORY_BATCHES = 12;


// =====================================================
// DERIV HISTORY
// =====================================================

function getDerivHistoryBatch(symbol, endTime) {

  return new Promise(function(resolve, reject) {

    const ws = new WebSocket(DERIV_WS);

    let finished = false;

    const timeout = setTimeout(function() {

      if (!finished) {

        finished = true;

        try {
          ws.close();
        } catch (e) {}

        reject(
          new Error("Deriv request timed out")
        );
      }

    }, 20000);


    ws.on("open", function() {

      ws.send(
        JSON.stringify({

          ticks_history: symbol,

          end: endTime,

          count: 1000,

          style: "ticks"

        })
      );

    });


    ws.on("message", function(message) {

      try {

        const data =
          JSON.parse(
            message.toString()
          );


        if (data.error) {

          if (!finished) {

            finished = true;

            clearTimeout(timeout);

            try {
              ws.close();
            } catch (e) {}

            reject(
              new Error(
                data.error.message ||
                "Deriv API error"
              )
            );

          }

          return;

        }


        if (data.msg_type === "history") {

          if (
            !data.history ||
            !data.history.prices ||
            !data.history.times
          ) {

            throw new Error(
              "Deriv returned no usable history"
            );

          }


          const prices =
            data.history.prices;

          const times =
            data.history.times;

          const ticks = [];


          for (
            let i = 0;
            i < prices.length &&
            i < times.length;
            i++
          ) {

            ticks.push({

              price:
                Number(prices[i]),

              epoch:
                Number(times[i])

            });

          }


          if (!finished) {

            finished = true;

            clearTimeout(timeout);

            try {
              ws.close();
            } catch (e) {}

            resolve(ticks);

          }

        }

      } catch (error) {

        if (!finished) {

          finished = true;

          clearTimeout(timeout);

          try {
            ws.close();
          } catch (e) {}

          reject(error);

        }

      }

    });


    ws.on("error", function(error) {

      if (!finished) {

        finished = true;

        clearTimeout(timeout);

        reject(error);

      }

    });

  });

}


// =====================================================
// GET TICKS
// =====================================================

async function getDerivTicks(symbol, batches) {

  let allTicks = [];

  let endTime = "latest";


  for (
    let batchNumber = 0;
    batchNumber < batches;
    batchNumber++
  ) {

    console.log(
      "Downloading history batch " +
      (batchNumber + 1) +
      "/" +
      batches
    );


    const batch =
      await getDerivHistoryBatch(
        symbol,
        endTime
      );


    if (
      !batch ||
      batch.length === 0
    ) {

      break;

    }


    allTicks =
      allTicks.concat(batch);


    const oldest =
      batch[0];


    if (
      !oldest ||
      !oldest.epoch
    ) {

      break;

    }


    endTime =
      oldest.epoch - 1;


    if (batch.length < 1000) {

      break;

    }

  }


  const unique = {};


  allTicks.forEach(
    function(tick) {

      const key =
        tick.epoch +
        "_" +
        tick.price;

      unique[key] =
        tick;

    }
  );


  return Object.keys(unique)
    .map(function(key) {

      return unique[key];

    })
    .sort(function(a, b) {

      return a.epoch - b.epoch;

    });

}


// =====================================================
// CANDLE BUILDER
// =====================================================

function buildCandles(ticks, seconds) {

  const groups = {};


  ticks.forEach(
    function(tick) {

      const bucket =
        Math.floor(
          tick.epoch / seconds
        ) * seconds;


      if (!groups[bucket]) {

        groups[bucket] = {

          epoch: bucket,

          open:
            tick.price,

          high:
            tick.price,

          low:
            tick.price,

          close:
            tick.price

        };

      } else {

        const candle =
          groups[bucket];


        if (
          tick.price >
          candle.high
        ) {

          candle.high =
            tick.price;

        }


        if (
          tick.price <
          candle.low
        ) {

          candle.low =
            tick.price;

        }


        candle.close =
          tick.price;

      }

    }
  );


  return Object.keys(groups)
    .map(function(key) {

      return groups[key];

    })
    .sort(function(a, b) {

      return a.epoch - b.epoch;

    });

}


// =====================================================
// EMA
// =====================================================

function EMA(values, period) {

  if (
    !values ||
    values.length === 0
  ) {

    return 0;

  }


  if (
    values.length < period
  ) {

    return values[
      values.length - 1
    ];

  }


  const multiplier =
    2 / (period + 1);


  let ema = 0;


  for (
    let i = 0;
    i < period;
    i++
  ) {

    ema += values[i];

  }


  ema =
    ema / period;


  for (
    let i = period;
    i < values.length;
    i++
  ) {

    ema =
      (
        values[i] *
        multiplier
      ) +
      (
        ema *
        (1 - multiplier)
      );

  }


  return ema;

}


// =====================================================
// WILDER RSI
// =====================================================

function RSI(values, period) {

  if (
    values.length <
    period + 1
  ) {

    return 50;

  }


  let gain = 0;

  let loss = 0;


  for (
    let i = 1;
    i <= period;
    i++
  ) {

    const difference =
      values[i] -
      values[i - 1];


    if (
      difference > 0
    ) {

      gain += difference;

    } else {

      loss +=
        Math.abs(
          difference
        );

    }

  }


  let averageGain =
    gain / period;

  let averageLoss =
    loss / period;


  for (
    let i = period + 1;
    i < values.length;
    i++
  ) {

    const difference =
      values[i] -
      values[i - 1];


    const currentGain =
      difference > 0
        ? difference
        : 0;


    const currentLoss =
      difference < 0
        ? Math.abs(difference)
        : 0;


    averageGain =
      (
        averageGain *
        (period - 1) +
        currentGain
      ) / period;


    averageLoss =
      (
        averageLoss *
        (period - 1) +
        currentLoss
      ) / period;

  }


  if (
    averageLoss === 0
  ) {

    return 100;

  }


  const rs =
    averageGain /
    averageLoss;


  return (
    100 -
    100 / (1 + rs)
  );

}


// =====================================================
// ATR
// =====================================================

function ATR(candles, period) {

  if (
    candles.length <
    period + 1
  ) {

    return 0;

  }


  const trueRanges = [];


  for (
    let i = 1;
    i < candles.length;
    i++
  ) {

    const current =
      candles[i];

    const previous =
      candles[i - 1];


    const tr =
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


    trueRanges.push(tr);

  }


  const recent =
    trueRanges.slice(
      -period
    );


  return (
    recent.reduce(
      function(total, value) {

        return total + value;

      },
      0
    ) /
    recent.length
  );

}


// =====================================================
// MARKET STRUCTURE + DIRECTIONAL BOS
// =====================================================

function getStructure(candles) {

  if (
    candles.length < 15
  ) {

    return {

      trend: "RANGE",

      bos: false,

      bosDirection: "NONE",

      lastHigh: null,

      lastLow: null,

      previousHigh: null,

      previousLow: null

    };

  }


  const swingHighs = [];

  const swingLows = [];


  for (
    let i = 2;
    i < candles.length - 2;
    i++
  ) {

    const candle =
      candles[i];


    if (

      candle.high >
      candles[i - 1].high &&

      candle.high >
      candles[i - 2].high &&

      candle.high >
      candles[i + 1].high &&

      candle.high >
      candles[i + 2].high

    ) {

      swingHighs.push({

        index: i,

        value:
          candle.high

      });

    }


    if (

      candle.low <
      candles[i - 1].low &&

      candle.low <
      candles[i - 2].low &&

      candle.low <
      candles[i + 1].low &&

      candle.low <
      candles[i + 2].low

    ) {

      swingLows.push({

        index: i,

        value:
          candle.low

      });

    }

  }


  if (
    swingHighs.length < 2 ||
    swingLows.length < 2
  ) {

    return {

      trend: "RANGE",

      bos: false,

      bosDirection: "NONE",

      lastHigh: null,

      lastLow: null,

      previousHigh: null,

      previousLow: null

    };

  }


  const lastHigh =
    swingHighs[
      swingHighs.length - 1
    ];


  const previousHigh =
    swingHighs[
      swingHighs.length - 2
    ];


  const lastLow =
    swingLows[
      swingLows.length - 1
    ];


  const previousLow =
    swingLows[
      swingLows.length - 2
    ];


  const current =
    candles[
      candles.length - 1
    ];


  let trend =
    "RANGE";


  if (

    lastHigh.value >
    previousHigh.value &&

    lastLow.value >
    previousLow.value

  ) {

    trend =
      "BULLISH";

  }


  if (

    lastHigh.value <
    previousHigh.value &&

    lastLow.value <
    previousLow.value

  ) {

    trend =
      "BEARISH";

  }


  // -----------------------------------------------
  // Directional BOS
  // -----------------------------------------------

  let bos =
    false;

  let bosDirection =
    "NONE";


  if (
    current.close >
    lastHigh.value
  ) {

    bos =
      true;

    bosDirection =
      "BULLISH";

  }


  else if (
    current.close <
    lastLow.value
  ) {

    bos =
      true;

    bosDirection =
      "BEARISH";

  }


  return {

    trend:
      trend,

    bos:
      bos,

    bosDirection:
      bosDirection,

    lastHigh:
      lastHigh.value,

    lastLow:
      lastLow.value,

    previousHigh:
      previousHigh.value,

    previousLow:
      previousLow.value

  };

}


// =====================================================
// STRONGER LIQUIDITY SWEEP
// =====================================================

function liquiditySweep(candles) {

  if (
    candles.length < 12
  ) {

    return {

      direction: "NONE",

      strength: 0,

      age: null

    };

  }


  /*
   * Look back several recent candles.
   * This prevents the bot from missing a sweep
   * simply because the sweep happened one or two
   * candles ago.
   */

  const lookback =
    Math.min(
      5,
      candles.length - 8
    );


  for (
    let offset = 0;
    offset < lookback;
    offset++
  ) {

    const index =
      candles.length -
      1 -
      offset;


    const current =
      candles[index];


    const referenceStart =
      Math.max(
        0,
        index - 7
      );


    const reference =
      candles.slice(
        referenceStart,
        index
      );


    if (
      reference.length < 5
    ) {

      continue;

    }


    let highest =
      -Infinity;

    let lowest =
      Infinity;


    reference.forEach(
      function(candle) {

        highest =
          Math.max(
            highest,
            candle.high
          );


        lowest =
          Math.min(
            lowest,
            candle.low
          );

      }
    );


    // Sell-side liquidity swept,
    // then price closed back above it.
    if (

      current.low <
      lowest &&

      current.close >
      lowest

    ) {

      return {

        direction:
          "SELL-SIDE SWEEP",

        strength:
          offset === 0
            ? 3
            : 2,

        age:
          offset

      };

    }


    // Buy-side liquidity swept,
    // then price closed back below it.
    if (

      current.high >
      highest &&

      current.close <
      highest

    ) {

      return {

        direction:
          "BUY-SIDE SWEEP",

        strength:
          offset === 0
            ? 3
            : 2,

        age:
          offset

      };

    }

  }


  return {

    direction:
      "NONE",

    strength:
      0,

    age:
      null

  };

}


// =====================================================
// STRONGER FVG DETECTION
// =====================================================

function detectFVG(candles) {

  if (
    candles.length < 5
  ) {

    return {

      direction: "NONE",

      strength: 0,

      age: null

    };

  }


  /*
   * Search the latest 8 possible FVG formations.
   * The most recent valid FVG gets priority.
   */

  const maxLookback =
    Math.min(
      8,
      candles.length - 2
    );


  for (
    let offset = 0;
    offset < maxLookback;
    offset++
  ) {

    const cIndex =
      candles.length -
      1 -
      offset;


    const a =
      candles[
        cIndex - 2
      ];


    const middle =
      candles[
        cIndex - 1
      ];


    const c =
      candles[
        cIndex
      ];


    if (!a || !middle || !c) {

      continue;

    }


    // Bullish FVG
    if (
      c.low >
      a.high
    ) {

      const gap =
        c.low -
        a.high;


      return {

        direction:
          "BULLISH FVG",

        strength:
          offset === 0
            ? 3
            : 2,

        age:
          offset,

        gap:
          gap

      };

    }


    // Bearish FVG
    if (
      c.high <
      a.low
    ) {

      const gap =
        a.low -
        c.high;


      return {

        direction:
          "BEARISH FVG",

        strength:
          offset === 0
            ? 3
            : 2,

        age:
          offset,

        gap:
          gap

      };

    }

  }


  return {

    direction:
      "NONE",

    strength:
      0,

    age:
      null,

    gap:
      0

  };

}


// =====================================================
// MOMENTUM
// =====================================================

function candleMomentum(candles) {

  if (
    candles.length < 4
  ) {

    return "NEUTRAL";

  }


  const current =
    candles[
      candles.length - 1
    ];


  const previous =
    candles[
      candles.length - 2
    ];


  const body =
    Math.abs(
      current.close -
      current.open
    );


  const range =
    current.high -
    current.low;


  if (
    range <= 0
  ) {

    return "NEUTRAL";

  }


  const bodyRatio =
    body / range;


  if (

    current.close >
    current.open &&

    current.close >
    previous.close &&

    bodyRatio >= 0.45

  ) {

    return "BULLISH";

  }


  if (

    current.close <
    current.open &&

    current.close <
    previous.close &&

    bodyRatio >= 0.45

  ) {

    return "BEARISH";

  }


  return "NEUTRAL";

}


// =====================================================
// SETUP TYPE
// =====================================================

function determineSetupType(
  action,
  trend5m,
  structure1,
  liquidity,
  fvg
) {

  if (
    action === "WAIT"
  ) {

    return "NO CONFIRMED SETUP";

  }


  if (
    action === "BUY"
  ) {

    if (

      liquidity.direction ===
        "SELL-SIDE SWEEP" &&

      structure1.bosDirection ===
        "BULLISH"

    ) {

      return "BULLISH LIQUIDITY REVERSAL";

    }


    if (
      structure1.bosDirection ===
      "BULLISH"
    ) {

      return "BULLISH BREAKOUT";

    }


    if (
      trend5m === "BULLISH"
    ) {

      return "BULLISH CONTINUATION";

    }


    if (
      fvg.direction ===
      "BULLISH FVG"
    ) {

      return "BULLISH FVG SETUP";

    }


    return "BULLISH SETUP";

  }


  if (
    action === "SELL"
  ) {

    if (

      liquidity.direction ===
        "BUY-SIDE SWEEP" &&

      structure1.bosDirection ===
        "BEARISH"

    ) {

      return "BEARISH LIQUIDITY REVERSAL";

    }


    if (
      structure1.bosDirection ===
      "BEARISH"
    ) {

      return "BEARISH BREAKOUT";

    }


    if (
      trend5m === "BEARISH"
    ) {

      return "BEARISH CONTINUATION";

    }


    if (
      fvg.direction ===
      "BEARISH FVG"
    ) {

      return "BEARISH FVG SETUP";

    }


    return "BEARISH SETUP";

  }


  return "NO CONFIRMED SETUP";

}


// =====================================================
// MAIN ANALYSIS
// =====================================================

async function analyse(symbol) {

  const ticks =
    await getDerivTicks(
      symbol,
      HISTORY_BATCHES
    );


  if (
    ticks.length < 100
  ) {

    throw new Error(
      "Not enough tick data returned by Deriv: " +
      ticks.length
    );

  }


  const candles10 =
    buildCandles(
      ticks,
      10
    );


  const candles1 =
    buildCandles(
      ticks,
      60
    );


  const candles5 =
    buildCandles(
      ticks,
      300
    );


  if (

    candles10.length < 30 ||
    candles1.length < 30 ||
    candles5.length < 30

  ) {

    throw new Error(

      "Not enough candles | ticks=" +
      ticks.length +
      " | 10s=" +
      candles10.length +
      " | 1m=" +
      candles1.length +
      " | 5m=" +
      candles5.length

    );

  }


  // ===================================================
  // 5M
  // ===================================================

  const close5 =
    candles5.map(
      function(c) {

        return c.close;

      }
    );


  const ema5Fast =
    EMA(
      close5,
      5
    );


  const ema5Slow =
    EMA(
      close5,
      15
    );


  const structure5 =
    getStructure(
      candles5
    );


  let trend5m =
    "RANGE";


  if (

    ema5Fast >
    ema5Slow &&

    structure5.trend ===
    "BULLISH"

  ) {

    trend5m =
      "BULLISH";

  }


  if (

    ema5Fast <
    ema5Slow &&

    structure5.trend ===
    "BEARISH"

  ) {

    trend5m =
      "BEARISH";

  }


  // ===================================================
  // 1M
  // ===================================================

  const close1 =
    candles1.map(
      function(c) {

        return c.close;

      }
    );


  const ema1Fast =
    EMA(
      close1,
      5
    );


  const ema1Slow =
    EMA(
      close1,
      15
    );


  const structure1 =
    getStructure(
      candles1
    );


  // ===================================================
  // 10 SECOND
  // ===================================================

  const close10 =
    candles10.map(
      function(c) {

        return c.close;

      }
    );


  const current =
    close10[
      close10.length - 1
    ];


  const ema10Fast =
    EMA(
      close10,
      5
    );


  const ema10Slow =
    EMA(
      close10,
      15
    );


  const rsi =
    RSI(
      close10,
      14
    );


  const atr =
    ATR(
      candles10,
      14
    );


  const liquidity =
    liquiditySweep(
      candles10
    );


  const fvg =
    detectFVG(
      candles10
    );


  const momentum =
    candleMomentum(
      candles10
    );


  // ===================================================
  // RSI CONTEXT
  // ===================================================

  let rsiContext =
    "NEUTRAL";


  if (
    rsi <= 30
  ) {

    rsiContext =
      "OVERSOLD";

  }


  else if (
    rsi >= 70
  ) {

    rsiContext =
      "OVERBOUGHT";

  }


  else if (
    rsi > 50
  ) {

    rsiContext =
      "BULLISH";

  }


  else {

    rsiContext =
      "BEARISH";

  }


  // ===================================================
  // SCORE
  // ===================================================

  /*
   * Directional maximum = 100
   *
   * 5M trend       20
   * 1M structure   20
   * BOS            20
   * Liquidity      15
   * FVG            10
   * EMA             5
   * Momentum        5
   * RSI context     5
   *
   * TOTAL           100
   */

  let buyScore = 0;

  let sellScore = 0;


  // -----------------------------------------------
  // 5M TREND
  // -----------------------------------------------

  if (
    trend5m ===
    "BULLISH"
  ) {

    buyScore += 20;

  }


  if (
    trend5m ===
    "BEARISH"
  ) {

    sellScore += 20;

  }


  // -----------------------------------------------
  // 1M STRUCTURE
  // -----------------------------------------------

  if (
    structure1.trend ===
    "BULLISH"
  ) {

    buyScore += 20;

  }


  if (
    structure1.trend ===
    "BEARISH"
  ) {

    sellScore += 20;

  }


  // -----------------------------------------------
  // DIRECTIONAL BOS
  // -----------------------------------------------

  if (
    structure1.bosDirection ===
    "BULLISH"
  ) {

    buyScore += 20;

  }


  if (
    structure1.bosDirection ===
    "BEARISH"
  ) {

    sellScore += 20;

  }


  // -----------------------------------------------
  // LIQUIDITY
  // -----------------------------------------------

  if (
    liquidity.direction ===
    "SELL-SIDE SWEEP"
  ) {

    buyScore +=
      liquidity.strength >= 3
        ? 15
        : 10;

  }


  if (
    liquidity.direction ===
    "BUY-SIDE SWEEP"
  ) {

    sellScore +=
      liquidity.strength >= 3
        ? 15
        : 10;

  }


  // -----------------------------------------------
  // FVG
  // -----------------------------------------------

  if (
    fvg.direction ===
    "BULLISH FVG"
  ) {

    buyScore +=
      fvg.strength >= 3
        ? 10
        : 7;

  }


  if (
    fvg.direction ===
    "BEARISH FVG"
  ) {

    sellScore +=
      fvg.strength >= 3
        ? 10
        : 7;

  }


  // -----------------------------------------------
  // EMA
  // -----------------------------------------------

  if (
    ema10Fast >
    ema10Slow
  ) {

    buyScore += 5;

  }


  if (
    ema10Fast <
    ema10Slow
  ) {

    sellScore += 5;

  }


  // -----------------------------------------------
  // MOMENTUM
  // -----------------------------------------------

  if (
    momentum ===
    "BULLISH"
  ) {

    buyScore += 5;

  }


  if (
    momentum ===
    "BEARISH"
  ) {

    sellScore += 5;

  }


  // -----------------------------------------------
  // RSI
  // -----------------------------------------------

  /*
   * RSI does NOT blindly give a BUY at oversold
   * or SELL at overbought.
   *
   * Reversal needs supporting momentum.
   */

  if (
    rsiContext ===
      "BULLISH" &&

    rsi >= 52 &&
    rsi <= 68
  ) {

    buyScore += 5;

  }


  if (
    rsiContext ===
      "BEARISH" &&

    rsi >= 32 &&
    rsi <= 48
  ) {

    sellScore += 5;

  }


  if (

    rsiContext ===
      "OVERSOLD" &&

    momentum ===
      "BULLISH"

  ) {

    buyScore += 5;

  }


  if (

    rsiContext ===
      "OVERBOUGHT" &&

    momentum ===
      "BEARISH"

  ) {

    sellScore += 5;

  }


  // ===================================================
  // HARD LIMIT
  // ===================================================

  buyScore =
    Math.min(
      100,
      Math.round(
        buyScore
      )
    );


  sellScore =
    Math.min(
      100,
      Math.round(
        sellScore
      )
    );


  const score =
    Math.max(
      buyScore,
      sellScore
    );


  // ===================================================
  // CONFIRMATIONS
  // ===================================================

  let buyConfirmations = 0;

  let sellConfirmations = 0;


  if (
    trend5m ===
    "BULLISH"
  ) {

    buyConfirmations++;

  }


  if (
    structure1.trend ===
    "BULLISH"
  ) {

    buyConfirmations++;

  }


  if (
    structure1.bosDirection ===
    "BULLISH"
  ) {

    buyConfirmations++;

  }


  if (
    liquidity.direction ===
    "SELL-SIDE SWEEP"
  ) {

    buyConfirmations++;

  }


  if (
    fvg.direction ===
    "BULLISH FVG"
  ) {

    buyConfirmations++;

  }


  if (
    ema10Fast >
    ema10Slow
  ) {

    buyConfirmations++;

  }


  if (
    momentum ===
    "BULLISH"
  ) {

    buyConfirmations++;

  }


  if (
    rsiContext ===
      "OVERSOLD" &&
    momentum ===
      "BULLISH"
  ) {

    buyConfirmations++;

  }


  // SELL

  if (
    trend5m ===
    "BEARISH"
  ) {

    sellConfirmations++;

  }


  if (
    structure1.trend ===
    "BEARISH"
  ) {

    sellConfirmations++;

  }


  if (
    structure1.bosDirection ===
    "BEARISH"
  ) {

    sellConfirmations++;

  }


  if (
    liquidity.direction ===
    "BUY-SIDE SWEEP"
  ) {

    sellConfirmations++;

  }


  if (
    fvg.direction ===
    "BEARISH FVG"
  ) {

    sellConfirmations++;

  }


  if (
    ema10Fast <
    ema10Slow
  ) {

    sellConfirmations++;

  }


  if (
    momentum ===
    "BEARISH"
  ) {

    sellConfirmations++;

  }


  if (
    rsiContext ===
      "OVERBOUGHT" &&
    momentum ===
      "BEARISH"
  ) {

    sellConfirmations++;

  }


  // ===================================================
  // ACTION
  // ===================================================

  let action =
    "WAIT";


  let reason =
    "Conditions are not sufficiently aligned.";


  // ===================================================
  // HIGH QUALITY BUY
  // ===================================================

  if (

    buyScore >= 65 &&

    buyConfirmations >= 4 &&

    buyScore >=
      sellScore + 10 &&

    structure1.trend ===
      "BULLISH" &&

    trend5m !==
      "BEARISH"

  ) {

    action =
      "BUY";


    reason =
      "Bullish structure is supported by multiple independent confirmations.";

  }


  // ===================================================
  // HIGH QUALITY SELL
  // ===================================================

  if (

    sellScore >= 65 &&

    sellConfirmations >= 4 &&

    sellScore >=
      buyScore + 10 &&

    structure1.trend ===
      "BEARISH" &&

    trend5m !==
      "BULLISH"

  ) {

    action =
      "SELL";


    reason =
      "Bearish structure is supported by multiple independent confirmations.";

  }


  // ===================================================
  // STRONG REVERSAL BUY
  // ===================================================

  if (

    action ===
      "WAIT" &&

    liquidity.direction ===
      "SELL-SIDE SWEEP" &&

    structure1.bosDirection ===
      "BULLISH" &&

    momentum ===
      "BULLISH" &&

    buyScore >= 60 &&

    buyScore >=
      sellScore + 10

  ) {

    action =
      "BUY";


    reason =
      "Sell-side liquidity was swept and followed by a bullish BOS and bullish momentum.";

  }


  // ===================================================
  // STRONG REVERSAL SELL
  // =====================================================

  if (

    action ===
      "WAIT" &&

    liquidity.direction ===
      "BUY-SIDE SWEEP" &&

    structure1.bosDirection ===
      "BEARISH" &&

    momentum ===
      "BEARISH" &&

    sellScore >= 60 &&

    sellScore >=
      buyScore + 10

  ) {

    action =
      "SELL";


    reason =
      "Buy-side liquidity was swept and followed by a bearish BOS and bearish momentum.";

  }


  // ===================================================
  // CONFLICT PROTECTION
  // ===================================================

  if (

    buyScore >= 60 &&

    sellScore >= 60 &&

    Math.abs(
      buyScore -
      sellScore
    ) < 10

  ) {

    action =
      "WAIT";


    reason =
      "Strong bullish and bearish conditions are competing; there is no clear directional advantage.";

  }


  // ===================================================
  // TRADE LEVELS
  // ===================================================

  let entry =
    Number(
      current.toFixed(2)
    );


  let stopLoss =
    null;


  let takeProfit =
    null;


  let rr =
    0;


  if (
    action === "BUY" &&
    atr > 0
  ) {

    const recentLow =
      Math.min.apply(
        null,
        candles10
          .slice(-12)
          .map(function(c) {

            return c.low;

          })
      );


    const atrStop =
      current -
      atr * 1.2;


    stopLoss =
      Math.min(
        recentLow,
        atrStop
      );


    const risk =
      Math.abs(
        entry -
        stopLoss
      );


    if (
      risk > 0
    ) {

      takeProfit =
        entry +
        risk * 2;


      rr =
        2;

    }

  }


  if (
    action === "SELL" &&
    atr > 0
  ) {

    const recentHigh =
      Math.max.apply(
        null,
        candles10
          .slice(-12)
          .map(function(c) {

            return c.high;

          })
      );


    const atrStop =
      current +
      atr * 1.2;


    stopLoss =
      Math.max(
        recentHigh,
        atrStop
      );


    const risk =
      Math.abs(
        entry -
        stopLoss
      );


    if (
      risk > 0
    ) {

      takeProfit =
        entry -
        risk * 2;


      rr =
        2;

    }

  }


  // ===================================================
  // DECIMAL PRECISION
  // ===================================================

  let decimals = 2;


  if (
    current < 100
  ) {

    decimals = 4;

  }


  if (
    current < 10
  ) {

    decimals = 5;

  }


  entry =
    Number(
      entry.toFixed(
        decimals
      )
    );


  if (
    stopLoss !== null
  ) {

    stopLoss =
      Number(
        stopLoss.toFixed(
          decimals
        )
      );

  }


  if (
    takeProfit !== null
  ) {

    takeProfit =
      Number(
        takeProfit.toFixed(
          decimals
        )
      );

  }


  // ===================================================
  // CONFIDENCE
  // ===================================================

  let confidence =
    "LOW";


  if (
    action !== "WAIT" &&
    score >= 65
  ) {

    confidence =
      "MEDIUM";

  }


  if (
    action !== "WAIT" &&
    score >= 75
  ) {

    confidence =
      "HIGH";

  }


  // ===================================================
  // SETUP TYPE
  // ===================================================

  const setupType =
    determineSetupType(
      action,
      trend5m,
      structure1,
      liquidity,
      fvg
    );


  // ===================================================
  // WATCH FOR
  // ===================================================

  const watchFor = [];


  if (
    !structure1.bos
  ) {

    watchFor.push(
      "Directional 1M BOS"
    );

  }


  if (
    liquidity.direction ===
    "NONE"
  ) {

    watchFor.push(
      "Liquidity sweep"
    );

  }


  if (
    fvg.direction ===
    "NONE"
  ) {

    watchFor.push(
      "FVG formation"
    );

  }


  if (
    momentum ===
    "NEUTRAL"
  ) {

    watchFor.push(
      "Momentum confirmation"
    );

  }


  if (
    trend5m ===
    "RANGE"
  ) {

    watchFor.push(
      "5M directional breakout"
    );

  }


  if (
    watchFor.length === 0
  ) {

    watchFor.push(
      "Maintain current structure"
    );

  }


  // ===================================================
  // FINAL RESULT
  // ===================================================

  return {

    success:
      true,

    symbol:
      symbol,

    dataSource:
      "Deriv public market data",

    mode:
      "paper analysis only",


    history: {

      ticks:
        ticks.length,

      batches:
        HISTORY_BATCHES

    },


    candles: {

      tenSecond:
        candles10.length,

      oneMinute:
        candles1.length,

      fiveMinute:
        candles5.length

    },


    analysis: {

      trend5m:
        trend5m,

      structure1m:
        structure1.trend,

      bos:
        structure1.bos,

      bosDirection:
        structure1.bosDirection,

      liquidity:
        liquidity.direction,

      liquidityAge:
        liquidity.age,

      fvg:
        fvg.direction,

      fvgAge:
        fvg.age,

      momentum:
        momentum,

      ema:
        ema10Fast >
        ema10Slow
          ? "Bullish"
          : "Bearish",

      rsi:
        Number(
          rsi.toFixed(2)
        ),

      rsiContext:
        rsiContext,

      atr:
        Number(
          atr.toFixed(
            decimals
          )
        )

    },


    conclusion: {

      action:
        action,

      setupType:
        setupType,

      confidence:
        confidence,

      score:
        score,

      buyScore:
        buyScore,

      sellScore:
        sellScore,

      buyConfirmations:
        buyConfirmations,

      sellConfirmations:
        sellConfirmations,

      entry:
        action === "WAIT"
          ? null
          : entry,

      stopLoss:
        stopLoss,

      takeProfit:
        takeProfit,

      rr:
        rr,

      reason:
        reason,

      watchFor:
        watchFor

    }

  };

}


// =====================================================
// HOME
// =====================================================

app.get(
  "/",
  function(req, res) {

    res.json({

      status:
        "online",

      service:
        "Keamz FX Deriv Signal Engine V2",

      mode:
        "paper analysis only",

      defaultSymbol:
        DEFAULT_SYMBOL,

      historyBatches:
        HISTORY_BATCHES

    });

  }
);


// =====================================================
// SIGNAL ENDPOINT
// =====================================================

app.get(
  "/signal",
  async function(req, res) {

    try {

      const symbol =
        (
          req.query.symbol ||
          DEFAULT_SYMBOL
        ).trim();


      if (!symbol) {

        throw new Error(
          "Symbol is required"
        );

      }


      console.log(
        "================================="
      );

      console.log(
        "KEAMZ FX V2 SIGNAL REQUEST:",
        symbol
      );

      console.log(
        "================================="
      );


      const result =
        await analyse(
          symbol
        );


      res.json(
        result
      );


    } catch (error) {

      console.error(
        "SIGNAL ERROR:",
        error.message
      );


      res.status(500).json({

        success:
          false,

        error:
          error.message

      });

    }

  }
);


// =====================================================
// START SERVER
// =====================================================

app.listen(
  PORT,
  "0.0.0.0",
  function() {

    console.log(
      "================================="
    );

    console.log(
      " KEAMZ FX SIGNAL ENGINE V2"
    );

    console.log(
      "================================="
    );

    console.log(
      "Server running on port " +
      PORT
    );

    console.log(
      "Default symbol: " +
      DEFAULT_SYMBOL
    );

    console.log(
      "History batches: " +
      HISTORY_BATCHES
    );

    console.log(
      "Paper analysis only."
    );

  }
);
