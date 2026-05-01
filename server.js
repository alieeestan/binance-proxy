const express = require("express");
const crypto = require("crypto");
const https = require("https");

const app = express();
app.use(express.json());

const PROXY_SECRET = process.env.PROXY_SECRET || "change-me";
const BINANCE_BASE = process.env.BINANCE_BASE || "https://fapi.binance.com";

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

// Proxy endpoint: receives signed params from Cloudflare Worker, forwards to Binance
app.post("/proxy", async (req, res) => {
  try {
    // Verify proxy secret
    if (req.headers["x-proxy-secret"] !== PROXY_SECRET) {
      return res.status(401).json({ error: "bad proxy secret" });
    }

    const { endpoint, params, apiKey, apiSecret, method } = req.body;

    if (!endpoint || !params || !apiKey || !apiSecret) {
      return res.status(400).json({ error: "missing fields" });
    }

    // Sign the request
    const qs = Object.entries(params)
      .map(([k, v]) => `${k}=${encodeURIComponent(v)}`)
      .join("&");
    const signature = crypto
      .createHmac("sha256", apiSecret)
      .update(qs)
      .digest("hex");

    const url = `${BINANCE_BASE}${endpoint}?${qs}&signature=${signature}`;

    // Forward to Binance
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

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`proxy running on port ${PORT}`));
