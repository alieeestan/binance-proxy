const express = require("express");
const crypto = require("crypto");
const WebSocket = require("ws");

const app = express();
app.use(express.json());

const PROXY_SECRET = process.env.PROXY_SECRET || "change-me";
const BINANCE_BASE = process.env.BINANCE_BASE || "https://fapi.binance.com";
const BINANCE_WS = process.env.BINANCE_WS || "wss://fstream.binance.com";
const API_KEY = process.env.BINANCE_API_KEY || "";
const API_SECRET = process.env.BINANCE_API_SECRET || "";
const META_PHONE_ID = process.env.META_PHONE_NUMBER_ID || "";
const META_TOKEN = process.env.META_ACCESS_TOKEN || "";
const META_API_VERSION = process.env.META_API_VERSION || "v22.0";
const NOTIFY_NUMBERS = ["923421741295", "923003596645"];

// ─── WhatsApp sender ────────────────────────────────────────
async function sendWhatsApp(message) {
  if (!META_PHONE_ID || !META_TOKEN) return;
  const url = `https://graph.facebook.com/${META_API_VERSION}/${META_PHONE_ID}/messages`;
  for (const phone of NOTIFY_NUMBERS) {
    try {
      const r = await fetch(url, {
        method: "POST",
        headers: { "Authorization": `Bearer ${META_TOKEN}`, "Content-Type": "application/json" },
        body: JSON.stringify({ messaging_product: "whatsapp", to: phone, type: "text", text: { body: message } }),
      });
      console.log(`whatsapp to ${phone}: ${r.status}`);
    } catch (e) {
      console.error(`whatsapp to ${phone} failed:`, e.message);
    }
  }
}

// ─── Binance signed request helper ──────────────────────────
// keyOverride/secretOverride allow using the worker's keys instead of proxy env vars
async function signedFetch(endpoint, params, method = "POST", keyOverride = null, secretOverride = null) {
  const key = keyOverride || API_KEY;
  const secret = secretOverride || API_SECRET;
  const qs = Object.entries(params).map(([k, v]) => `${k}=${encodeURIComponent(v)}`).join("&");
  const signature = crypto.createHmac("sha256", secret).update(qs).digest("hex");
  const url = `${BINANCE_BASE}${endpoint}?${qs}&signature=${signature}`;
  return fetch(url, { method, headers: { "X-MBX-APIKEY": key } });
}

// ─── Ghost Trail: Position Tracker ──────────────────────────
const openPositions = {};
// Key: "BTCUSDT_LONG" or "BTCUSDT_SHORT"
// Value: { symbol, side, positionSide, closeSide, entryPrice, qty, slPrice, tp1Price, tp2Price, tp3Price, slAlgoId, currentSL, tp1Reached, tp2Reached, tp3Reached, moving }

function posKey(symbol, positionSide) {
  return `${symbol}_${positionSide}`;
}

async function cancelAlgoOrder(symbol, algoId, apiKey, apiSecret) {
  try {
    const r = await signedFetch("/fapi/v1/algoOrder", {
      symbol, algoId, timestamp: Date.now(), recvWindow: 5000,
    }, "DELETE", apiKey, apiSecret);
    const body = await r.json();
    console.log(`cancel algo ${algoId}: ${r.status} ${body.msg || "ok"}`);
    return r.status === 200;
  } catch (e) {
    console.error(`cancel algo ${algoId} error:`, e.message);
    return false;
  }
}

async function placeNewSL(symbol, closeSide, positionSide, triggerPrice, qty, apiKey, apiSecret) {
  try {
    const r = await signedFetch("/fapi/v1/algoOrder", {
      symbol, side: closeSide, type: "STOP_MARKET",
      positionSide, algoType: "CONDITIONAL",
      triggerPrice: triggerPrice.toString(),
      quantity: qty, priceProtect: "TRUE",
      timestamp: Date.now(), recvWindow: 5000,
    }, "POST", apiKey, apiSecret);
    const body = await r.json();
    console.log(`new SL at ${triggerPrice}: ${r.status} algoId=${body.algoId || body.msg}`);
    return body.algoId || null;
  } catch (e) {
    console.error(`place SL error:`, e.message);
    return null;
  }
}

async function closePositionMarket(symbol, closeSide, positionSide, qty, apiKey, apiSecret) {
  try {
    const r = await signedFetch("/fapi/v1/order", {
      symbol, side: closeSide, type: "MARKET",
      positionSide, quantity: qty,
      timestamp: Date.now(), recvWindow: 5000,
    }, "POST", apiKey, apiSecret);
    const body = await r.json();
    console.log(`close position: ${r.status} orderId=${body.orderId || body.msg}`);
    return body.orderId || null;
  } catch (e) {
    console.error(`close position error:`, e.message);
    return null;
  }
}

async function moveSL(pos, newSLPrice, reason) {
  if (pos.moving) return null; // prevent double moves
  pos.moving = true;

  const key = posKey(pos.symbol, pos.positionSide);
  const ak = pos.apiKey;
  const as = pos.apiSecret;
  console.log(`ghost trail: moving SL for ${key} to ${newSLPrice} (${reason})`);
  console.log(`  using keys from: ${ak ? "worker (✅)" : "proxy env"}`);

  // Cancel current SL
  if (pos.slAlgoId) {
    const cancelOk = await cancelAlgoOrder(pos.symbol, pos.slAlgoId, ak, as);
    console.log(`  cancel old SL ${pos.slAlgoId}: ${cancelOk ? "OK" : "FAILED"}`);
  }

  // Also cancel any other open algo orders for this symbol (safety)
  try {
    const openAlgos = await signedFetch("/fapi/v1/algoOrder", {
      symbol: pos.symbol, timestamp: Date.now(), recvWindow: 5000,
    }, "GET", ak, as);
    const algoData = await openAlgos.json();
    console.log(`  open algos: ${algoData?.rows?.length || 0} found`);
    if (Array.isArray(algoData?.rows)) {
      for (const algo of algoData.rows) {
        if (algo.algoId !== pos.slAlgoId) {
          await cancelAlgoOrder(pos.symbol, algo.algoId, ak, as);
        }
      }
    }
  } catch (e) { console.error("cleanup algos:", e.message); }

  // Place new SL
  const newAlgoId = await placeNewSL(pos.symbol, pos.closeSide, pos.positionSide, newSLPrice, pos.qty, ak, as);
  console.log(`  new SL result: ${newAlgoId ? "algoId=" + newAlgoId : "FAILED"}`);

  if (newAlgoId) {
    pos.slAlgoId = newAlgoId;
    pos.currentSL = newSLPrice;
  } else {
    console.error(`  CRITICAL: Failed to place new SL at ${newSLPrice} for ${key}`);
  }

  pos.moving = false;
  return newAlgoId;
}

// ─── Ghost Trail: Price Monitor (REST polling every 2s) ─────
let priceInterval = null;

function startPriceMonitor() {
  console.log("Price monitor started (polling every 2s)");

  // Log first poll to confirm it works
  let pollCount = 0;

  priceInterval = setInterval(async () => {
    // Only poll if we have positions to track
    const trackedKeys = Object.keys(openPositions);
    if (trackedKeys.length === 0) return;

    pollCount++;

    // Get unique symbols being tracked
    const symbols = [...new Set(trackedKeys.map(k => openPositions[k].symbol))];

    for (const symbol of symbols) {
      try {
        const r = await fetch(`${BINANCE_BASE}/fapi/v1/ticker/price?symbol=${symbol}`);
        const data = await r.json();
        const markPrice = parseFloat(data.price);

        // Log every 30th poll (~1 min) to confirm polling works
        if (pollCount % 30 === 1) {
          console.log(`price poll #${pollCount}: ${symbol}=$${markPrice} | tracking ${trackedKeys.length} positions`);
        }

        if (!markPrice) {
          console.error(`price poll: no price for ${symbol}`, data);
          continue;
        }

        // Check both LONG and SHORT positions for this symbol
        for (const posSide of ["LONG", "SHORT"]) {
          const key = posKey(symbol, posSide);
          const pos = openPositions[key];
          if (!pos || pos.moving) continue;

          const isLong = posSide === "LONG";
          const tp1 = parseFloat(pos.tp1Price || 0);
          const tp2 = parseFloat(pos.tp2Price || 0);
          const tp3 = parseFloat(pos.tp3Price || 0);

          // --- TP3: Close position at market ---
          if (tp3 > 0 && !pos.tp3Reached) {
            const tp3Hit = isLong ? markPrice >= tp3 : markPrice <= tp3;
            if (tp3Hit) {
              pos.tp3Reached = true;
              console.log(`ghost trail: TP3 hit for ${key} at ${markPrice}`);

              if (pos.slAlgoId) await cancelAlgoOrder(pos.symbol, pos.slAlgoId, pos.apiKey, pos.apiSecret);
              await closePositionMarket(pos.symbol, pos.closeSide, pos.positionSide, pos.qty, pos.apiKey, pos.apiSecret);

              const profit = isLong
                ? (markPrice - pos.entryPrice) * parseFloat(pos.qty)
                : (pos.entryPrice - markPrice) * parseFloat(pos.qty);

              let msg = `🎯 *TP3 Hit — Full Profit!*\n\n`;
              msg += `*Symbol:* ${symbol}\n`;
              msg += `*Side:* ${posSide}\n`;
              msg += `*Entry:* $${pos.entryPrice}\n`;
              msg += `*TP3 Price:* $${markPrice.toFixed(2)}\n`;
              msg += `*Est. Profit:* +$${profit.toFixed(2)}\n`;
              msg += `\n*Time:* ${new Date().toUTCString()}`;
              await sendWhatsApp(msg);

              delete openPositions[key];
              continue;
            }
          }

          // --- TP2: Move SL to TP1 level (lock 1R) ---
          if (tp2 > 0 && !pos.tp2Reached) {
            const tp2Hit = isLong ? markPrice >= tp2 : markPrice <= tp2;
            if (tp2Hit) {
              pos.tp2Reached = true;
              console.log(`ghost trail: TP2 hit for ${key} at ${markPrice}`);

              const newSL = tp1;
              const newAlgoId = await moveSL(pos, newSL, "TP2 hit → SL to 1:1");

              const locked = isLong
                ? (newSL - pos.entryPrice) * parseFloat(pos.qty)
                : (pos.entryPrice - newSL) * parseFloat(pos.qty);

              let msg = `📈 *SL Moved to 1:1*\n\n`;
              msg += `*Symbol:* ${symbol}\n`;
              msg += `*Side:* ${posSide}\n`;
              msg += `*TP2 (1:2) reached:* $${markPrice.toFixed(2)}\n`;
              msg += `*New SL:* $${newSL} (1:1 level)\n`;
              msg += `*Locked profit:* +$${locked.toFixed(2)}\n`;
              if (!newAlgoId) msg += `\n⚠️ Failed to place new SL!`;
              msg += `\n*Time:* ${new Date().toUTCString()}`;
              await sendWhatsApp(msg);
              continue;
            }
          }

          // --- TP1: Move SL to breakeven ---
          if (tp1 > 0 && !pos.tp1Reached) {
            const tp1Hit = isLong ? markPrice >= tp1 : markPrice <= tp1;
            if (tp1Hit) {
              pos.tp1Reached = true;
              console.log(`ghost trail: TP1 hit for ${key} at ${markPrice}`);

              const newSL = pos.entryPrice;
              const newAlgoId = await moveSL(pos, newSL, "TP1 hit → SL to BE");

              let msg = `📈 *SL Moved to Breakeven*\n\n`;
              msg += `*Symbol:* ${symbol}\n`;
              msg += `*Side:* ${posSide}\n`;
              msg += `*TP1 (1:1) reached:* $${markPrice.toFixed(2)}\n`;
              msg += `*New SL:* $${newSL} (breakeven)\n`;
              msg += `*Risk:* $0 (fees only)\n`;
              if (!newAlgoId) msg += `\n⚠️ Failed to place new SL!`;
              msg += `\n*Time:* ${new Date().toUTCString()}`;
              await sendWhatsApp(msg);
              continue;
            }
          }
        }
      } catch (e) {
        console.error(`price poll error for ${symbol}:`, e.message);
      }
    }
  }, 2000); // Poll every 2 seconds
}

// ─── User Data Stream (WebSocket) ───────────────────────────
let listenKey = null;
let userWs = null;

async function createListenKey() {
  try {
    const r = await fetch(`${BINANCE_BASE}/fapi/v1/listenKey`, {
      method: "POST",
      headers: { "X-MBX-APIKEY": API_KEY },
    });
    const data = await r.json();
    listenKey = data.listenKey;
    console.log("listenKey created:", listenKey?.slice(0, 20) + "...");
    return listenKey;
  } catch (e) {
    console.error("failed to create listenKey:", e.message);
    return null;
  }
}

async function keepAliveListenKey() {
  if (!listenKey) return;
  try {
    await fetch(`${BINANCE_BASE}/fapi/v1/listenKey`, {
      method: "PUT",
      headers: { "X-MBX-APIKEY": API_KEY },
    });
    console.log("listenKey kept alive");
  } catch (e) {
    console.error("keepAlive failed:", e.message);
  }
}

function connectUserStream() {
  if (!listenKey) return;

  userWs = new WebSocket(`${BINANCE_WS}/ws/${listenKey}`);

  userWs.on("open", () => console.log("User stream connected"));

  userWs.on("message", async (raw) => {
    try {
      const event = JSON.parse(raw.toString());

      if (event.e === "ORDER_TRADE_UPDATE") {
        const o = event.o;
        const isFilled = o.X === "FILLED";
        if (!isFilled) return;

        const type = o.ot || o.o;
        const isSL = type === "STOP_MARKET" || type === "STOP";
        const isEntry = type === "MARKET" || type === "LIMIT";
        const symbol = o.s;
        const pnl = parseFloat(o.rp || 0);
        const side = o.S;

        // Skip entry fills
        if (isEntry) return;

        // If SL filled, remove from ghost trail tracking
        if (isSL) {
          // Find which position this SL belongs to
          for (const [key, pos] of Object.entries(openPositions)) {
            if (pos.symbol === symbol) {
              const trailStatus = pos.tp2Reached ? "after TP2 → SL at 1:1"
                : pos.tp1Reached ? "after TP1 → SL at BE"
                : "Direct SL hit";

              let msg = `🛑 *STOP LOSS HIT*\n\n`;
              msg += `*Symbol:* ${symbol}\n`;
              msg += `*Side:* ${pos.positionSide}\n`;
              msg += `*Trail Status:* ${trailStatus}\n`;
              msg += `*Qty:* ${o.z}\n`;
              msg += `*Fill Price:* $${parseFloat(o.ap).toFixed(2)}\n`;
              msg += `*Trigger Price:* $${parseFloat(o.sp).toFixed(2)}\n`;
              msg += `*Realized PnL:* ${pnl >= 0 ? "+" : ""}$${pnl.toFixed(4)}\n`;
              msg += `*Order ID:* ${o.i}\n`;
              msg += `\n*Time:* ${new Date(event.T).toUTCString()}`;

              console.log(`SL hit for ${key}: ${trailStatus} pnl=${pnl}`);
              await sendWhatsApp(msg);

              // Cancel any remaining algo orders
              try {
                const openAlgos = await signedFetch("/fapi/v1/algoOrder", {
                  symbol, timestamp: Date.now(), recvWindow: 5000,
                }, "GET", pos.apiKey, pos.apiSecret);
                const algoData = await openAlgos.json();
                if (Array.isArray(algoData?.rows)) {
                  for (const algo of algoData.rows) {
                    await cancelAlgoOrder(symbol, algo.algoId, pos.apiKey, pos.apiSecret);
                  }
                }
              } catch (e) { console.error("cleanup after SL:", e.message); }

              delete openPositions[key];
              break;
            }
          }

          // If not in ghost trail tracking, send generic SL notification
          if (!Object.values(openPositions).some(p => p.symbol === symbol)) {
            let msg = `🛑 *STOP LOSS HIT*\n\n`;
            msg += `*Symbol:* ${symbol}\n`;
            msg += `*Side:* ${side}\n`;
            msg += `*Type:* ${type}\n`;
            msg += `*Qty Filled:* ${o.z}\n`;
            msg += `*Fill Price:* $${parseFloat(o.ap).toFixed(2)}\n`;
            msg += `*Trigger Price:* $${parseFloat(o.sp).toFixed(2)}\n`;
            msg += `*Realized PnL:* ${pnl >= 0 ? "+" : ""}$${pnl.toFixed(4)}\n`;
            msg += `\n*Time:* ${new Date(event.T).toUTCString()}`;
            await sendWhatsApp(msg);
          }
        }
      }
    } catch (e) {
      console.error("user stream parse error:", e.message);
    }
  });

  userWs.on("close", async () => {
    console.log("User stream closed, recreating listenKey and reconnecting in 5s...");
    setTimeout(async () => {
      await createListenKey();
      connectUserStream();
    }, 5000);
  });

  userWs.on("error", (e) => {
    console.error("User stream error:", e.message);
    userWs.close();
  });
}

async function startStreams() {
  if (!API_KEY || !API_SECRET) {
    console.log("no API keys — skipping streams");
    return;
  }
  await createListenKey();
  connectUserStream();
  startPriceMonitor();

  // Keep alive every 30 minutes
  setInterval(keepAliveListenKey, 30 * 60 * 1000);
}

// ─── Phase 3: Recovery on startup ───────────────────────────
async function recoverPositions() {
  if (!API_KEY || !API_SECRET) return;
  try {
    const r = await signedFetch("/fapi/v2/positionRisk", {
      symbol: "BTCUSDT", timestamp: Date.now(), recvWindow: 5000,
    }, "GET");
    const positions = await r.json();
    if (!Array.isArray(positions)) return;

    for (const pos of positions) {
      const amt = parseFloat(pos.positionAmt || 0);
      if (amt === 0) continue;

      const positionSide = pos.positionSide;
      const key = posKey(pos.symbol, positionSide);
      const entryPrice = parseFloat(pos.entryPrice);

      // If we don't have this position tracked, add it with SL at breakeven as safe default
      if (!openPositions[key]) {
        console.log(`recovered position: ${key} qty=${amt} entry=${entryPrice}`);
        openPositions[key] = {
          symbol: pos.symbol,
          side: positionSide === "LONG" ? "BUY" : "SELL",
          positionSide,
          closeSide: positionSide === "LONG" ? "SELL" : "BUY",
          entryPrice,
          qty: Math.abs(amt).toString(),
          slPrice: entryPrice.toString(), // safe default: BE
          tp1Price: "0", tp2Price: "0", tp3Price: "0",
          slAlgoId: null,
          currentSL: entryPrice,
          tp1Reached: true, tp2Reached: false, tp3Reached: false, // assume TP1 reached since we set SL at BE
          moving: false,
        };
      }
    }
  } catch (e) {
    console.error("position recovery error:", e.message);
  }
}

// ─── Express routes ─────────────────────────────────────────

// Health check
app.get("/", (req, res) => res.send("ok"));

// Get outbound IP
app.get("/my-ip", async (req, res) => {
  try {
    const r = await fetch("https://api.ipify.org?format=json");
    const data = await r.json();
    res.json({ outbound_ip: data.ip });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Ghost Trail: Register position for trailing SL
app.post("/position", (req, res) => {
  try {
    if (req.headers["x-proxy-secret"] !== PROXY_SECRET) {
      return res.status(401).json({ error: "bad proxy secret" });
    }

    const { symbol, side, positionSide, closeSide, entryPrice, qty, slPrice, tp1Price, tp2Price, tp3Price, slAlgoId, apiKey, apiSecret } = req.body;

    if (!symbol || !positionSide || !entryPrice) {
      return res.status(400).json({ error: "missing fields" });
    }

    const key = posKey(symbol, positionSide);

    openPositions[key] = {
      symbol, side, positionSide, closeSide,
      entryPrice: parseFloat(entryPrice),
      qty, slPrice, tp1Price, tp2Price, tp3Price,
      slAlgoId,
      apiKey: apiKey || API_KEY,     // use worker's keys, fallback to proxy env
      apiSecret: apiSecret || API_SECRET,
      currentSL: parseFloat(slPrice),
      tp1Reached: false, tp2Reached: false, tp3Reached: false,
      moving: false,
    };

    console.log(`ghost trail: registered ${key} entry=${entryPrice} sl=${slPrice} tp1=${tp1Price} tp2=${tp2Price} tp3=${tp3Price}`);
    res.json({ ok: true, key, tracked: Object.keys(openPositions).length });
  } catch (e) {
    console.error("position register error:", e.message);
    res.status(500).json({ error: e.message });
  }
});

// View tracked positions
app.get("/positions", (req, res) => {
  res.json(openPositions);
});

// Remove position from Ghost Trail tracking
app.delete("/position/:key", (req, res) => {
  if (req.headers["x-proxy-secret"] !== PROXY_SECRET) {
    return res.status(401).json({ error: "bad proxy secret" });
  }
  const key = req.params.key;
  if (openPositions[key]) {
    delete openPositions[key];
    console.log(`ghost trail: removed ${key} from tracking`);
    res.json({ ok: true, removed: key });
  } else {
    res.json({ ok: true, message: "not tracked" });
  }
});

// Diagnostic: test if proxy can place/cancel orders with its own keys
app.get("/test-keys", async (req, res) => {
  try {
    const r = await signedFetch("/fapi/v2/balance", { timestamp: Date.now(), recvWindow: 5000 }, "GET");
    const body = await r.json();
    if (Array.isArray(body)) {
      const usdt = body.find(b => b.asset === "USDT");
      res.json({ ok: true, balance: usdt?.availableBalance, keySet: !!API_KEY, secretSet: !!API_SECRET, keyPrefix: API_KEY?.slice(0, 8) });
    } else {
      res.json({ ok: false, error: body.msg || body.code, keySet: !!API_KEY, secretSet: !!API_SECRET, keyPrefix: API_KEY?.slice(0, 8) });
    }
  } catch (e) {
    res.json({ ok: false, error: e.message, keySet: !!API_KEY, secretSet: !!API_SECRET });
  }
});

// Diagnostic: test price fetch + check TP levels
app.get("/ghost-status", async (req, res) => {
  const keys = Object.keys(openPositions);
  if (keys.length === 0) return res.json({ positions: 0, message: "no positions tracked" });

  const results = {};
  for (const key of keys) {
    const pos = openPositions[key];
    try {
      const r = await fetch(`${BINANCE_BASE}/fapi/v1/ticker/price?symbol=${pos.symbol}`);
      const data = await r.json();
      const price = parseFloat(data.price);
      const isLong = pos.positionSide === "LONG";
      const tp1 = parseFloat(pos.tp1Price || 0);
      const tp2 = parseFloat(pos.tp2Price || 0);
      const tp3 = parseFloat(pos.tp3Price || 0);

      results[key] = {
        price,
        entry: pos.entryPrice,
        currentSL: pos.currentSL,
        tp1: tp1, tp1Hit: isLong ? price >= tp1 : price <= tp1, tp1Reached: pos.tp1Reached,
        tp2: tp2, tp2Hit: isLong ? price >= tp2 : price <= tp2, tp2Reached: pos.tp2Reached,
        tp3: tp3, tp3Hit: isLong ? price >= tp3 : price <= tp3, tp3Reached: pos.tp3Reached,
        moving: pos.moving,
      };
    } catch (e) {
      results[key] = { error: e.message };
    }
  }
  res.json(results);
});

// Proxy endpoint
app.post("/proxy", async (req, res) => {
  try {
    if (req.headers["x-proxy-secret"] !== PROXY_SECRET) {
      return res.status(401).json({ error: "bad proxy secret" });
    }

    const { endpoint, params, apiKey, apiSecret, method } = req.body;

    if (!endpoint || !params || !apiKey || !apiSecret) {
      return res.status(400).json({ error: "missing fields" });
    }

    const qs = Object.entries(params)
      .map(([k, v]) => `${k}=${encodeURIComponent(v)}`)
      .join("&");
    const signature = crypto
      .createHmac("sha256", apiSecret)
      .update(qs)
      .digest("hex");

    const url = `${BINANCE_BASE}${endpoint}?${qs}&signature=${signature}`;

    const result = await fetch(url, {
      method: method || "POST",
      headers: { "X-MBX-APIKEY": apiKey },
    });

    const ct = result.headers.get("content-type") || "";
    if (ct.includes("json")) {
      const body = await result.json();
      res.status(result.status).json(body);
    } else {
      const text = await result.text();
      res.status(result.status).json({ error: `Binance returned non-JSON (${result.status})`, body: text.slice(0, 200) });
    }
  } catch (e) {
    console.error("proxy error:", e.message);
    res.status(500).json({ error: e.message });
  }
});

// ─── Start ──────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
app.listen(PORT, async () => {
  console.log(`proxy running on port ${PORT}`);
  await recoverPositions();
  await startStreams();
});
// deploy 1777910815
