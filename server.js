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
function signedFetch(endpoint, params, method = "POST") {
  const qs = Object.entries(params).map(([k, v]) => `${k}=${encodeURIComponent(v)}`).join("&");
  const signature = crypto.createHmac("sha256", API_SECRET).update(qs).digest("hex");
  const url = `${BINANCE_BASE}${endpoint}?${qs}&signature=${signature}`;
  return fetch(url, { method, headers: { "X-MBX-APIKEY": API_KEY } });
}

// ─── User Data Stream (WebSocket) ───────────────────────────
let listenKey = null;
let ws = null;

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

function connectWebSocket() {
  if (!listenKey) return;

  ws = new WebSocket(`${BINANCE_WS}/ws/${listenKey}`);

  ws.on("open", () => console.log("Binance WebSocket connected"));

  ws.on("message", async (raw) => {
    try {
      const event = JSON.parse(raw.toString());

      // ORDER_TRADE_UPDATE — fires when any order fills
      if (event.e === "ORDER_TRADE_UPDATE") {
        const o = event.o;
        const isFilled = o.X === "FILLED";
        const isPartial = o.X === "PARTIALLY_FILLED";

        if (!isFilled && !isPartial) return;

        const type = o.ot || o.o; // original order type
        const isSL = type === "STOP_MARKET" || type === "STOP";
        const isTP = type === "TAKE_PROFIT_MARKET" || type === "TAKE_PROFIT";
        const isEntry = type === "MARKET" || type === "LIMIT";

        // Skip entry fills (those are notified by the Cloudflare Worker)
        if (isEntry) return;

        const emoji = isSL ? "🛑" : "✅";
        const label = isSL ? "STOP LOSS HIT" : "TAKE PROFIT HIT";
        const side = o.S; // BUY or SELL
        const pnl = parseFloat(o.rp || 0);
        const symbol = o.s;

        // --- AUTO-CANCEL: When SL hits, check if position is fully closed → cancel remaining TPs ---
        // --- When TP hits, check if position is fully closed → cancel remaining SL ---
        if (isFilled) {
          try {
            // Check remaining position
            const posR = await signedFetch("/fapi/v2/positionRisk", { symbol, timestamp: Date.now(), recvWindow: 5000 }, "GET");
            const positions = await posR.json();

            let positionClosed = true;
            if (Array.isArray(positions)) {
              for (const pos of positions) {
                if (pos.symbol === symbol && parseFloat(pos.positionAmt || 0) !== 0) {
                  positionClosed = false;
                  break;
                }
              }
            }

            if (positionClosed) {
              // Cancel all remaining open orders for this symbol
              const cancelR = await signedFetch("/fapi/v1/allOpenOrders", { symbol, timestamp: Date.now(), recvWindow: 5000 }, "DELETE");
              console.log(`position closed — cancelled remaining orders: ${cancelR.status}`);

              // Also cancel algo/conditional orders
              try {
                const openAlgos = await signedFetch("/fapi/v1/algoOrder/openOrders", { symbol, timestamp: Date.now(), recvWindow: 5000 }, "GET");
                const algoData = await openAlgos.json();
                if (Array.isArray(algoData?.rows)) {
                  for (const algo of algoData.rows) {
                    const delR = await signedFetch("/fapi/v1/algoOrder", { symbol, algoId: algo.algoId, timestamp: Date.now(), recvWindow: 5000 }, "DELETE");
                    console.log(`cancelled algo ${algo.algoId}: ${delR.status}`);
                  }
                }
              } catch (e) {
                console.error("algo cancel error:", e.message);
              }
            }
          } catch (e) {
            console.error("auto-cancel error:", e.message);
          }
        }

        let msg = `${emoji} *${label}*\n\n`;
        msg += `*Symbol:* ${symbol}\n`;
        msg += `*Side:* ${side}\n`;
        msg += `*Type:* ${type}\n`;
        msg += `*Qty Filled:* ${o.z}\n`;
        msg += `*Fill Price:* $${parseFloat(o.ap).toFixed(2)}\n`;
        msg += `*Trigger Price:* $${parseFloat(o.sp).toFixed(2)}\n`;
        msg += `*Realized PnL:* ${pnl >= 0 ? "+" : ""}$${pnl.toFixed(4)}\n`;
        msg += `*Order ID:* ${o.i}\n`;
        msg += `*Status:* ${o.X}\n`;
        msg += `\n*Time:* ${new Date(event.T).toUTCString()}`;

        console.log(`${label}: ${symbol} ${side} qty=${o.z} pnl=${pnl}`);
        await sendWhatsApp(msg);
      }
    } catch (e) {
      console.error("ws message parse error:", e.message);
    }
  });

  ws.on("close", async () => {
    console.log("WebSocket closed, recreating listenKey and reconnecting in 5s...");
    setTimeout(async () => {
      await createListenKey(); // Get fresh listenKey — old one may have expired
      connectWebSocket();
    }, 5000);
  });

  ws.on("error", (e) => {
    console.error("WebSocket error:", e.message);
    ws.close();
  });
}

async function startUserStream() {
  if (!API_KEY || !API_SECRET) {
    console.log("no API keys — skipping user data stream");
    return;
  }
  await createListenKey();
  connectWebSocket();

  // Keep alive every 30 minutes
  setInterval(keepAliveListenKey, 30 * 60 * 1000);
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

    const body = await result.json();
    res.status(result.status).json(body);
  } catch (e) {
    console.error("proxy error:", e.message);
    res.status(500).json({ error: e.message });
  }
});

// ─── Start ──────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`proxy running on port ${PORT}`);
  startUserStream();
});
// redeploy 1777636018
// ws-connect 1777636957
