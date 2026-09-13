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

      const request = {

        ticks_history: symbol,

        end: endTime,

        count: 1000,

        style: "ticks"

      };

      ws.send(
        JSON.stringify(request)
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
      batches +
      "..."
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


    console.log(
      "Batch received:",
      batch.length,
      "ticks"
    );


    const oldestTick =
      batch[0];


    if (
      !oldestTick ||
      !oldestTick.epoch
    ) {

      break;

    }


    endTime =
      oldestTick.epoch - 1;


    if (batch.length < 1000) {

      break;

    }

  }


  const unique = {};


  allTicks.forEach(function(tick) {

    const key =
      String(tick.epoch) +
      "_" +
      String(tick.price);

    unique[key] = tick;

  });


  const result =
    Object.keys(unique)
      .map(function(key) {

        return unique[key];

      })
      .sort(function(a, b) {

        return a.epoch - b.epoch;

      });


  console.log(
    "TOTAL TICKS COLLECTED:",
    result.length
  );


  return result;

}


// =====================================================
// CANDLE BUILDER
// =====================================================

function buildCandles(ticks, seconds) {

  const groups = {};


  ticks.forEach(function(tick) {

    const bucket =
      Math.floor(
        tick.epoch / seconds
      ) * seconds;


    if (!groups[bucket]) {

      groups[bucket] = {

        epoch: bucket,

        open: tick.price,

        high: tick.price,

        low: tick.price,

        close: tick.price

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

  });


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

  if (values.length === 0) {

    return 0;

  }


  if (values.length < period) {

    return values[
      values.length - 1
    ];

  }


  const k =
    2 / (period + 1);


  let ema =
    values[
      values.length - period
    ];


  for (
    let i =
      values.length - period + 1;
    i < values.length;
    i++
  ) {

    ema =
      values[i] * k +
      ema * (1 - k);

  }


  return ema;

}


// =====================================================
// RSI
// =====================================================

function RSI(values, period) {

  if (
    values.length <
    period + 1
  ) {

    return 50;

  }


  let gains = 0;

  let losses = 0;


  for (
    let i =
      values.length - period;
    i < values.length;
    i++
  ) {

    const diff =
      values[i] -
      values[i - 1];


    if (diff > 0) {

      gains += diff;

    } else {

      losses -= diff;

    }

  }


  if (losses === 0) {

    return 100;

  }


  const rs =
    gains / losses;


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


  const trs = [];


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


    trs.push(tr);

  }


  const recent =
    trs.slice(-period);


  return (
    recent.reduce(
      function(a, b) {

        return a + b;

      },
      0
    ) / recent.length
  );

}


// =====================================================
// MARKET STRUCTURE
// =====================================================

function getStructure(candles) {

  if (candles.length < 10) {

    return {

      trend: "RANGE",

      bos: false,

      lastHigh: null,

      lastLow: null

    };

  }


  const highs = [];

  const lows = [];


  for (
    let i = 2;
    i < candles.length - 2;
    i++
  ) {

    if (

      candles[i].high >
      candles[i - 1].high &&

      candles[i].high >
      candles[i - 2].high &&

      candles[i].high >
      candles[i + 1].high &&

      candles[i].high >
      candles[i + 2].high

    ) {

      highs.push(
        candles[i].high
      );

    }


    if (

      candles[i].low <
      candles[i - 1].low &&

      candles[i].low <
      candles[i - 2].low &&

      candles[i].low <
      candles[i + 1].low &&

      candles[i].low <
      candles[i + 2].low

    ) {

      lows.push(
        candles[i].low
      );

    }

  }


  if (
    highs.length < 2 ||
    lows.length < 2
  ) {

    return {

      trend: "RANGE",

      bos: false,

      lastHigh: null,

      lastLow: null

    };

  }


  const lastHigh =
    highs[highs.length - 1];

  const previousHigh =
    highs[highs.length - 2];

  const lastLow =
    lows[lows.length - 1];

  const previousLow =
    lows[lows.length - 2];


  const current =
    candles[
      candles.length - 1
    ].close;


  let trend = "RANGE";


  if (
    lastHigh > previousHigh &&
    lastLow > previousLow
  ) {

    trend = "BULLISH";

  }


  if (
    lastHigh < previousHigh &&
    lastLow < previousLow
  ) {

    trend = "BEARISH";

  }


  let bos = false;


  if (
    current > lastHigh
  ) {

    bos = true;

  }


  if (
    current < lastLow
  ) {

    bos = true;

  }


  return {

    trend: trend,

    bos: bos,

    lastHigh: lastHigh,

    lastLow: lastLow

  };

}


// =====================================================
// LIQUIDITY SWEEP
// =====================================================

function liquiditySweep(candles) {

  if (candles.length < 8) {

    return "NONE";

  }


  const current =
    candles[
      candles.length - 1
    ];


  const previous =
    candles.slice(-8, -1);


  let highest =
    -Infinity;

  let lowest =
    Infinity;


  previous.forEach(
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


  if (
    current.low < lowest &&
    current.close > lowest
  ) {

    return "SELL-SIDE SWEEP";

  }


  if (
    current.high > highest &&
    current.close < highest
  ) {

    return "BUY-SIDE SWEEP";

  }


  return "NONE";

}


// =====================================================
// FVG
// =====================================================

function detectFVG(candles) {

  if (candles.length < 3) {

    return "NONE";

  }


  const a =
    candles[
      candles.length - 3
    ];


  const c =
    candles[
      candles.length - 1
    ];


  if (
    c.low > a.high
  ) {

    return "BULLISH FVG";

  }


  if (
    c.high < a.low
  ) {

    return "BEARISH FVG";

  }


  return "NONE";

}


// =====================================================
// CANDLE MOMENTUM
// =====================================================

function candleMomentum(candles) {

  if (candles.length < 3) {

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


  if (
    current.close >
    current.open &&
    current.close >
    previous.close
  ) {

    return "BULLISH";

  }


  if (
    current.close <
    current.open &&
    current.close <
    previous.close
  ) {

    return "BEARISH";

  }


  return "NEUTRAL";

}


// =====================================================
// SIGNAL ANALYSIS
// =====================================================

async function analyse(symbol) {

  const ticks =
    await getDerivTicks(
      symbol,
      HISTORY_BATCHES
    );


  if (ticks.length < 100) {

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


  console.log(
    "10S CANDLES:",
    candles10.length
  );


  console.log(
    "1M CANDLES:",
    candles1.length
  );


  console.log(
    "5M CANDLES:",
    candles5.length
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
  // 5 MINUTE
  // ===================================================

  const close5 =
    candles5.map(
      function(c) {

        return c.close;

      }
    );


  const ema5Fast =
    EMA(close5, 5);


  const ema5Slow =
    EMA(close5, 15);


  const structure5 =
    getStructure(
      candles5
    );


  let trend5m =
    "RANGE";


  if (
    ema5Fast > ema5Slow &&
    structure5.trend ===
      "BULLISH"
  ) {

    trend5m =
      "BULLISH";

  }


  if (
    ema5Fast < ema5Slow &&
    structure5.trend ===
      "BEARISH"
  ) {

    trend5m =
      "BEARISH";

  }


  // ===================================================
  // 1 MINUTE
  // ===================================================

  const close1 =
    candles1.map(
      function(c) {

        return c.close;

      }
    );


  const ema1Fast =
    EMA(close1, 5);


  const ema1Slow =
    EMA(close1, 15);


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
    EMA(close10, 5);


  const ema10Slow =
    EMA(close10, 15);


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
  // IMPROVED CONFLUENCE SCORING
  // ===================================================

  /*
   * Maximum base score = 100
   *
   * 5M trend       20
   * 1M structure   20
   * BOS            15
   * Liquidity      15
   * FVG            10
   * EMA             5
   * RSI             5
   * Momentum        10
   *
   * Total          100
   */

  let buyScore = 0;

  let sellScore = 0;


  // ===================================================
  // 5M TREND
  // ===================================================

  if (
    trend5m === "BULLISH"
  ) {

    buyScore += 20;

  }


  if (
    trend5m === "BEARISH"
  ) {

    sellScore += 20;

  }


  // ===================================================
  // 1M STRUCTURE
  // ===================================================

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


  // ===================================================
  // BOS
  // ===================================================

  if (
    structure1.bos
  ) {

    if (
      structure1.trend ===
      "BULLISH"
    ) {

      buyScore += 15;

    }


    if (
      structure1.trend ===
      "BEARISH"
    ) {

      sellScore += 15;

    }

  }


  // ===================================================
  // LIQUIDITY
  // ===================================================

  if (
    liquidity ===
    "SELL-SIDE SWEEP"
  ) {

    buyScore += 15;

  }


  if (
    liquidity ===
    "BUY-SIDE SWEEP"
  ) {

    sellScore += 15;

  }


  // ===================================================
  // FVG
  // ===================================================

  if (
    fvg ===
    "BULLISH FVG"
  ) {

    buyScore += 10;

  }


  if (
    fvg ===
    "BEARISH FVG"
  ) {

    sellScore += 10;

  }


  // ===================================================
  // EMA
  // ===================================================

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


  // ===================================================
  // RSI
  // ===================================================

  if (
    rsi >= 52 &&
    rsi <= 68
  ) {

    buyScore += 5;

  }


  if (
    rsi >= 32 &&
    rsi <= 48
  ) {

    sellScore += 5;

  }


  // Oversold reversal context.
  // RSI alone does NOT create a BUY.

  if (
    rsi <= 30 &&
    momentum ===
      "BULLISH"
  ) {

    buyScore += 5;

  }


  // Overbought reversal context.
  // RSI alone does NOT create a SELL.

  if (
    rsi >= 70 &&
    momentum ===
      "BEARISH"
  ) {

    sellScore += 5;

  }


  // ===================================================
  // MOMENTUM
  // ===================================================

  if (
    momentum ===
    "BULLISH"
  ) {

    buyScore += 10;

  }


  if (
    momentum ===
    "BEARISH"
  ) {

    sellScore += 10;

  }


  // ===================================================
  // NORMALIZE SCORES
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
  // CONFIRMATION COUNT
  // ===================================================

  let buyConfirmations = 0;

  let sellConfirmations = 0;


  // BUY confirmations

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
    structure1.bos &&
    structure1.trend ===
    "BULLISH"
  ) {

    buyConfirmations++;

  }


  if (
    liquidity ===
    "SELL-SIDE SWEEP"
  ) {

    buyConfirmations++;

  }


  if (
    fvg ===
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


  // SELL confirmations

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
    structure1.bos &&
    structure1.trend ===
    "BEARISH"
  ) {

    sellConfirmations++;

  }


  if (
    liquidity ===
    "BUY-SIDE SWEEP"
  ) {

    sellConfirmations++;

  }


  if (
    fvg ===
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


  // ===================================================
  // ACTION
  // ===================================================

  let action =
    "WAIT";


  let reason =
    "Conditions are not sufficiently aligned.";


  // ===================================================
  // BUY
  // ===================================================

  if (

    buyScore >= 65 &&

    buyConfirmations >= 4 &&

    structure1.trend ===
      "BULLISH" &&

    buyScore >=
      sellScore + 10 &&

    trend5m !==
      "BEARISH"

  ) {

    action =
      "BUY";


    reason =
      "Bullish 5M/1M structure has sufficient confirmation from the current momentum, trend and market-structure signals.";

  }


  // ===================================================
  // SELL
  // ===================================================

  if (

    sellScore >= 65 &&

    sellConfirmations >= 4 &&

    structure1.trend ===
      "BEARISH" &&

    sellScore >=
      buyScore + 10 &&

    trend5m !==
      "BULLISH"

  ) {

    action =
      "SELL";


    reason =
      "Bearish 5M/1M structure has sufficient confirmation from the current momentum, trend and market-structure signals.";

  }


  // ===================================================
  // STRONG BULLISH TRIGGER
  // ===================================================

  if (

    action ===
      "WAIT" &&

    structure1.trend ===
      "BULLISH" &&

    structure1.bos &&

    liquidity ===
      "SELL-SIDE SWEEP" &&

    buyScore >= 60 &&

    buyScore >=
      sellScore + 10

  ) {

    action =
      "BUY";


    reason =
      "Bullish 1M BOS occurred after a sell-side liquidity sweep, with sufficient supporting confluence.";

  }


  // ===================================================
  // STRONG BEARISH TRIGGER
  // ===================================================

  if (

    action ===
      "WAIT" &&

    structure1.trend ===
      "BEARISH" &&

    structure1.bos &&

    liquidity ===
      "BUY-SIDE SWEEP" &&

    sellScore >= 60 &&

    sellScore >=
      buyScore + 10

  ) {

    action =
      "SELL";


    reason =
      "Bearish 1M BOS occurred after a buy-side liquidity sweep, with sufficient supporting confluence.";

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
      "Bullish and bearish conditions are too closely balanced.";

  }


  // ===================================================
  // ENTRY / SL / TP
  // ===================================================

  let entry =
    Number(
      current.toFixed(2)
    );


  let stopLoss = null;

  let takeProfit = null;

  let rr = 0;


  // ===================================================
  // BUY LEVELS
  // ===================================================

  if (
    action === "BUY" &&
    atr > 0
  ) {

    const recentLow =
      Math.min.apply(
        null,
        candles10
          .slice(-10)
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


    if (risk > 0) {

      takeProfit =
        entry +
        risk * 2;

      rr = 2;

    }

  }


  // ===================================================
  // SELL LEVELS
  // ===================================================

  if (
    action === "SELL" &&
    atr > 0
  ) {

    const recentHigh =
      Math.max.apply(
        null,
        candles10
          .slice(-10)
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


    if (risk > 0) {

      takeProfit =
        entry -
        risk * 2;

      rr = 2;

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
    score >= 60
  ) {

    confidence =
      "MEDIUM";

  }


  if (
    score >= 75
  ) {

    confidence =
      "HIGH";

  }


  if (
    action === "WAIT"
  ) {

    confidence =
      "LOW";

  }


  // ===================================================
  // WATCH FOR
  // ===================================================

  let watchFor = [];


  if (
    trend5m ===
    "BULLISH"
  ) {

    watchFor.push(
      "Bullish continuation"
    );

  }


  if (
    trend5m ===
    "BEARISH"
  ) {

    watchFor.push(
      "Bearish continuation"
    );

  }


  if (
    !structure1.bos
  ) {

    watchFor.push(
      "1M BOS confirmation"
    );

  }


  if (
    liquidity ===
    "NONE"
  ) {

    watchFor.push(
      "Liquidity sweep"
    );

  }


  if (
    fvg ===
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

    success: true,

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

      liquidity:
        liquidity,

      fvg:
        fvg,

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

      confidence:
        confidence,

      score:
        score,

      buyScore:
        buyScore,

      sellScore:
        sellScore,

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
        "Keamz FX Deriv Signal Engine",

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
// SIGNAL
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


      console.log("");

      console.log(
        "================================="
      );

      console.log(
        "SIGNAL REQUEST:",
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

    }

    catch (error) {

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
      " KEAMZ FX DERIV SIGNAL ENGINE"
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
