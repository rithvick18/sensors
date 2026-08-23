/**
 * Sensor Dashboard — app.js
 * Real-time motion sensor dashboard with 2-way control
 */

(() => {
  "use strict";

  /* ── Configuration ── */
  const HISTORY_LENGTH = 200;
  const ACCEL_SCALE = 20;   // m/s²
  const GYRO_SCALE = 10;    // rad/s
  const MAX_FEED_LINES = 30;

  /* ── DOM refs ── */
  const $ = (id) => document.getElementById(id);

  const phoneUrlInput    = $("phone-url");
  const remoteStreamBtn  = $("remote-stream-btn");
  const remoteBtnLabel   = $("remote-btn-label");
  const remoteBtnIcon    = $("remote-btn-icon");
  const phoneSyncPill    = $("phone-sync-pill");
  const syncText         = $("sync-text");
  const statusDot        = $("status-dot");
  const statusText       = $("status-text");
  const connectionStatus = $("connection-status");
  const statsBar         = $("stats-bar");
  const dashboard        = $("dashboard");
  const orientSection    = $("orientation-section");

  // Stats
  const statSamplesVal   = $("stat-samples-value");
  const statRateVal      = $("stat-rate-value");
  const statLatencyVal   = $("stat-latency-value");
  const statPacketVal    = $("stat-packet-value");

  // Accel
  const accelMagEl  = $("accel-magnitude");
  const accelXBar   = $("accel-x-bar");
  const accelYBar   = $("accel-y-bar");
  const accelZBar   = $("accel-z-bar");
  const accelXVal   = $("accel-x-value");
  const accelYVal   = $("accel-y-value");
  const accelZVal   = $("accel-z-value");
  const accelCanvas = $("accel-chart");

  // Gyro
  const gyroMagEl   = $("gyro-magnitude");
  const gyroXBar    = $("gyro-x-bar");
  const gyroYBar    = $("gyro-y-bar");
  const gyroZBar    = $("gyro-z-bar");
  const gyroXVal    = $("gyro-x-value");
  const gyroYVal    = $("gyro-y-value");
  const gyroZVal    = $("gyro-z-value");
  const gyroCanvas  = $("gyro-chart");

  // Feed
  const dataFeed    = $("data-feed");

  /* ── State ── */
  let reader = null;
  let abortCtrl = null;
  let connected = false;
  let isStreamingActive = false;
  let totalSamples = 0;
  let startTime = null;
  let rateCounter = 0;
  let lastRateCheck = 0;
  let currentRate = 0;
  let rateInterval = null;
  let elapsedInterval = null;
  let chartAnimFrame = null;

  // Current values
  let ax = 0, ay = 0, az = 0;
  let gx = 0, gy = 0, gz = 0;
  let lastSeq = 0;

  // History for charts
  const accelXHistory = [];
  const accelYHistory = [];
  const accelZHistory = [];
  const gyroXHistory  = [];
  const gyroYHistory  = [];
  const gyroZHistory  = [];

  // Feed lines
  const feedLines = [];

  /* ── Canvas Setup ── */
  let accelCtx = null;
  let gyroCtx = null;

  function setupCanvas(canvas) {
    if (!canvas) return null;
    const dpr = window.devicePixelRatio || 1;
    const rect = canvas.getBoundingClientRect();
    if (rect.width === 0 || rect.height === 0) return null;
    canvas.width = rect.width * dpr;
    canvas.height = rect.height * dpr;
    const ctx = canvas.getContext("2d");
    ctx.scale(dpr, dpr);
    return ctx;
  }

  function initCanvases() {
    accelCtx = setupCanvas(accelCanvas);
    gyroCtx  = setupCanvas(gyroCanvas);
    renderInitialGrids();
  }

  const CHART_COLORS = {
    accel: {
      x: { line: "rgba(21, 101, 192, 1)",  fill: "rgba(21, 101, 192, 0.08)" },
      y: { line: "rgba(2, 136, 209, 1)",   fill: "rgba(2, 136, 209, 0.08)" },
      z: { line: "rgba(245, 124, 0, 1)",   fill: "rgba(245, 124, 0, 0.08)" },
    },
    gyro: {
      x: { line: "rgba(103, 58, 183, 1)",  fill: "rgba(103, 58, 183, 0.08)" },
      y: { line: "rgba(216, 27, 96, 1)",   fill: "rgba(216, 27, 96, 0.08)" },
      z: { line: "rgba(0, 150, 136, 1)",   fill: "rgba(0, 150, 136, 0.08)" },
    },
  };

  function renderInitialGrids() {
    if (accelCtx && accelCanvas) {
      drawChart(accelCtx, accelCanvas, [[], [], []], CHART_COLORS.accel, ACCEL_SCALE);
    }
    if (gyroCtx && gyroCanvas) {
      drawChart(gyroCtx, gyroCanvas, [[], [], []], CHART_COLORS.gyro, GYRO_SCALE);
    }
  }

  function drawChart(ctx, canvas, histories, colors, scale) {
    if (!ctx || !canvas) return;
    const w = canvas.getBoundingClientRect().width;
    const h = canvas.getBoundingClientRect().height;
    ctx.clearRect(0, 0, w, h);

    // Grid lines
    ctx.strokeStyle = "rgba(0, 0, 0, 0.05)";
    ctx.lineWidth = 1;
    const gridLines = 4;
    for (let i = 1; i < gridLines; i++) {
      const y = (h / gridLines) * i;
      ctx.beginPath();
      ctx.moveTo(0, y);
      ctx.lineTo(w, y);
      ctx.stroke();
    }

    // Zero baseline
    ctx.strokeStyle = "rgba(0, 0, 0, 0.12)";
    ctx.setLineDash([4, 4]);
    ctx.beginPath();
    ctx.moveTo(0, h / 2);
    ctx.lineTo(w, h / 2);
    ctx.stroke();
    ctx.setLineDash([]);

    // Draw each axis
    const axes = ["x", "y", "z"];
    for (let a = 0; a < 3; a++) {
      const data = histories[a];
      const color = colors[axes[a]];
      if (!data || data.length < 2) continue;

      const step = w / (HISTORY_LENGTH - 1);
      const offset = HISTORY_LENGTH - data.length;

      // Build path
      ctx.beginPath();
      for (let i = 0; i < data.length; i++) {
        const x = (offset + i) * step;
        const normalized = (data[i] + scale) / (2 * scale);
        const y = h - (normalized * h);
        if (i === 0) ctx.moveTo(x, y);
        else ctx.lineTo(x, y);
      }

      // Stroke
      ctx.strokeStyle = color.line;
      ctx.lineWidth = 1.8;
      ctx.lineJoin = "round";
      ctx.stroke();

      // Fill
      const lastX = (offset + data.length - 1) * step;
      const firstX = offset * step;
      ctx.lineTo(lastX, h);
      ctx.lineTo(firstX, h);
      ctx.closePath();
      ctx.fillStyle = color.fill;
      ctx.fill();
    }

    // Legend
    const legendY = 12;
    let legendX = w - 10;
    for (let a = 2; a >= 0; a--) {
      const label = axes[a].toUpperCase();
      ctx.font = "700 10px 'Inter', sans-serif";
      const textW = ctx.measureText(label).width;
      legendX -= textW;
      ctx.fillStyle = colors[axes[a]].line;
      ctx.fillText(label, legendX, legendY);
      legendX -= 12;
      ctx.beginPath();
      ctx.arc(legendX + 4, legendY - 3, 3, 0, Math.PI * 2);
      ctx.fill();
      legendX -= 10;
    }
  }

  function renderCharts() {
    if (!accelCtx || !gyroCtx) {
      initCanvases();
    }

    drawChart(
      accelCtx, accelCanvas,
      [accelXHistory, accelYHistory, accelZHistory],
      CHART_COLORS.accel,
      ACCEL_SCALE
    );

    drawChart(
      gyroCtx, gyroCanvas,
      [gyroXHistory, gyroYHistory, gyroZHistory],
      CHART_COLORS.gyro,
      GYRO_SCALE
    );

    chartAnimFrame = requestAnimationFrame(renderCharts);
  }

  /* ── UI Update ── */
  function updateUI() {
    // Accelerometer
    const accelMag = Math.sqrt(ax * ax + ay * ay + az * az);
    if (accelMagEl) accelMagEl.textContent = accelMag.toFixed(3);
    if (accelXVal) accelXVal.textContent = `${ax.toFixed(3)} m/s²`;
    if (accelYVal) accelYVal.textContent = `${ay.toFixed(3)} m/s²`;
    if (accelZVal) accelZVal.textContent = `${az.toFixed(3)} m/s²`;
    
    if (accelXBar) {
      accelXBar.style.width = `${Math.min(Math.abs(ax) / ACCEL_SCALE * 100, 100)}%`;
      accelXBar.classList.toggle('negative', ax < 0);
    }
    if (accelYBar) {
      accelYBar.style.width = `${Math.min(Math.abs(ay) / ACCEL_SCALE * 100, 100)}%`;
      accelYBar.classList.toggle('negative', ay < 0);
    }
    if (accelZBar) {
      accelZBar.style.width = `${Math.min(Math.abs(az) / ACCEL_SCALE * 100, 100)}%`;
      accelZBar.classList.toggle('negative', az < 0);
    }

    // Gyroscope
    const gyroMag = Math.sqrt(gx * gx + gy * gy + gz * gz);
    if (gyroMagEl) gyroMagEl.textContent = gyroMag.toFixed(3);
    if (gyroXVal) gyroXVal.textContent = `${gx.toFixed(3)} rad/s`;
    if (gyroYVal) gyroYVal.textContent = `${gy.toFixed(3)} rad/s`;
    if (gyroZVal) gyroZVal.textContent = `${gz.toFixed(3)} rad/s`;
    
    if (gyroXBar) {
      gyroXBar.style.width = `${Math.min(Math.abs(gx) / GYRO_SCALE * 100, 100)}%`;
      gyroXBar.classList.toggle('negative', gx < 0);
    }
    if (gyroYBar) {
      gyroYBar.style.width = `${Math.min(Math.abs(gy) / GYRO_SCALE * 100, 100)}%`;
      gyroYBar.classList.toggle('negative', gy < 0);
    }
    if (gyroZBar) {
      gyroZBar.style.width = `${Math.min(Math.abs(gz) / GYRO_SCALE * 100, 100)}%`;
      gyroZBar.classList.toggle('negative', gz < 0);
    }

    // Stats
    if (statSamplesVal) statSamplesVal.textContent = totalSamples.toLocaleString();
    if (statRateVal) statRateVal.textContent = `${currentRate} Hz`;
    if (statPacketVal) statPacketVal.textContent = lastSeq || "—";
  }

  function pushHistory(arr, val) {
    arr.push(val);
    if (arr.length > HISTORY_LENGTH) arr.shift();
  }

  function processSample(data) {
    const accel = data.accel;
    const gyro = data.gyro;
    if (!accel || !gyro) return;

    ax = Number(accel.x) || 0;
    ay = Number(accel.y) || 0;
    az = Number(accel.z) || 0;
    gx = Number(gyro.x) || 0;
    gy = Number(gyro.y) || 0;
    gz = Number(gyro.z) || 0;
    lastSeq = data.sequence || 0;
    totalSamples++;
    rateCounter++;

    if (!startTime) startTime = Date.now();

    // Push histories
    pushHistory(accelXHistory, ax);
    pushHistory(accelYHistory, ay);
    pushHistory(accelZHistory, az);
    pushHistory(gyroXHistory, gx);
    pushHistory(gyroYHistory, gy);
    pushHistory(gyroZHistory, gz);

    // Feed
    const line = JSON.stringify(data);
    feedLines.push(line);
    if (feedLines.length > MAX_FEED_LINES) feedLines.shift();
  }

  /* ── Throttled DOM updates ── */
  let uiThrottle = null;
  function scheduleUIUpdate() {
    if (uiThrottle) return;
    uiThrottle = requestAnimationFrame(() => {
      updateUI();
      updateFeed();
      uiThrottle = null;
    });
  }

  function updateFeed() {
    if (!dataFeed) return;
    const container = dataFeed.parentElement;
    const shouldScroll = container ? (container.scrollTop + container.clientHeight >= container.scrollHeight - 20) : false;

    dataFeed.innerHTML = feedLines.map(line => {
      const colored = line
        .replace(/"([^"]+)":/g, '<span class="feed-key">"$1"</span>:')
        .replace(/:(\s*)([-\d.]+)/g, ':$1<span class="feed-number">$2</span>');
      return `<span class="feed-line">${colored}</span>`;
    }).join("\n");

    if (shouldScroll && container) {
      container.scrollTop = container.scrollHeight;
    }
  }

  /* ── Connection States ── */
  function setConnectionState(state) {
    if (statusDot) statusDot.className = "status-dot";
    if (connectionStatus) connectionStatus.className = "connection-status";

    if (state === "connected") {
      connected = true;
      if (statusDot) statusDot.classList.add("active");
      if (connectionStatus) connectionStatus.classList.add("connected");
      if (statusText) statusText.textContent = "Connected";
    } else if (state === "connecting") {
      connected = false;
      if (statusDot) statusDot.classList.add("connecting");
      if (connectionStatus) connectionStatus.classList.add("connecting");
      if (statusText) statusText.textContent = "Connecting…";
    } else {
      connected = false;
      if (statusText) statusText.textContent = "Disconnected";
    }
  }

  function normalizeBaseUrl(input) {
    let url = (input || "").trim();
    if (!url) return "";
    if (!/^https?:\/\//i.test(url)) {
      url = `http://${url}`;
    }
    return url.replace(/\/(stream|sample|latest|health)?\/?$/i, "").replace(/\/+$/, "");
  }

  /* ── Connect to Stream ── */
  async function connect() {
    const rawInput = phoneUrlInput ? phoneUrlInput.value.trim() : "";
    const baseUrl = normalizeBaseUrl(rawInput) || window.location.origin;

    if (phoneUrlInput) {
      phoneUrlInput.value = baseUrl;
      localStorage.setItem("sensor_dashboard_url", baseUrl);
    }

    const streamUrl = `${baseUrl}/stream`;
    setConnectionState("connecting");

    abortCtrl = new AbortController();

    try {
      const response = await fetch(streamUrl, {
        signal: abortCtrl.signal,
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      setConnectionState("connected");
      initCanvases();

      if (!chartAnimFrame) {
        chartAnimFrame = requestAnimationFrame(renderCharts);
      }

      // Start rate counter
      lastRateCheck = performance.now();
      if (!rateInterval) {
        rateInterval = setInterval(() => {
          const now = performance.now();
          const dt = (now - lastRateCheck) / 1000;
          currentRate = Math.round(rateCounter / dt);
          rateCounter = 0;
          lastRateCheck = now;
        }, 1000);
      }

      // Start elapsed timer
      if (!elapsedInterval) {
        elapsedInterval = setInterval(() => {
          if (!startTime) return;
          const elapsed = Math.floor((Date.now() - startTime) / 1000);
          const m = String(Math.floor(elapsed / 60)).padStart(2, "0");
          const s = String(elapsed % 60).padStart(2, "0");
          if (statLatencyVal) statLatencyVal.textContent = `${m}:${s}`;
        }, 500);
      }

      // Read NDJSON stream
      reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split("\n");
        buffer = lines.pop() || "";

        for (const line of lines) {
          const trimmed = line.trim();
          if (!trimmed) continue;
          try {
            const data = JSON.parse(trimmed);
            if (data.accel && data.gyro) {
              processSample(data);
              scheduleUIUpdate();
            }
          } catch (_) {}
        }
      }

    } catch (err) {
      if (err.name !== "AbortError") {
        console.warn("Stream connection:", err.message);
      }
    } finally {
      disconnect(true);
      // Auto-reconnect stream cleanly
      if (window.location.hostname === "localhost" || window.location.hostname === "127.0.0.1") {
        setTimeout(() => {
          if (!connected && !abortCtrl) {
            connect();
          }
        }, 1000);
      }
    }
  }

  function disconnect(fromError = false) {
    if (reader) {
      try { reader.cancel(); } catch (_) {}
      reader = null;
    }
    if (abortCtrl) {
      abortCtrl.abort();
      abortCtrl = null;
    }
    if (!fromError) {
      setConnectionState("disconnected");
    }
  }

  /* ── 2-Way Remote Control ── */
  function updateStreamingUi(streaming) {
    isStreamingActive = streaming;
    if (remoteStreamBtn) {
      if (streaming) {
        remoteStreamBtn.classList.add("connected");
        if (remoteBtnLabel) remoteBtnLabel.textContent = "Stop Streaming";
        if (remoteBtnIcon) {
          remoteBtnIcon.innerHTML = `
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
              <rect x="6" y="6" width="12" height="12" rx="2" ry="2"/>
            </svg>`;
        }
      } else {
        remoteStreamBtn.classList.remove("connected");
        if (remoteBtnLabel) remoteBtnLabel.textContent = "Start Streaming";
        if (remoteBtnIcon) {
          remoteBtnIcon.innerHTML = `
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
              <polygon points="5 3 19 12 5 21 5 3"/>
            </svg>`;
        }
      }
    }
  }

  async function toggleRemoteStream() {
    const baseUrl = (phoneUrlInput ? normalizeBaseUrl(phoneUrlInput.value) : "") || window.location.origin;
    const targetAction = isStreamingActive ? "STOP" : "START";
    try {
      if (remoteBtnLabel) remoteBtnLabel.textContent = isStreamingActive ? "Stopping..." : "Starting...";
      if (!connected && !abortCtrl) {
        connect();
      }
      const res = await fetch(`${baseUrl}/api/control`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action: targetAction })
      });
      const data = await res.json();
      if (data && data.ok) {
        updateStreamingUi(data.is_streaming);
      }
    } catch (err) {
      console.error("Remote command error:", err);
    }
  }

  if (remoteStreamBtn) {
    remoteStreamBtn.addEventListener("click", toggleRemoteStream);
  }

  // Periodic Status Poller for 2-Way Sync
  setInterval(async () => {
    try {
      const baseUrl = (phoneUrlInput ? normalizeBaseUrl(phoneUrlInput.value) : "") || window.location.origin;
      const res = await fetch(`${baseUrl}/api/status`);
      if (res.ok) {
        const data = await res.json();
        if (phoneSyncPill && syncText) {
          if (data.phone_connected) {
            phoneSyncPill.classList.add("online");
            syncText.textContent = data.is_streaming ? "Phone: Streaming" : "Phone: Sync Ready";
          } else {
            phoneSyncPill.classList.remove("online");
            syncText.textContent = "Phone: Waiting...";
          }
        }
        if (typeof data.is_streaming === "boolean" && data.is_streaming !== isStreamingActive) {
          updateStreamingUi(data.is_streaming);
        }
      }
    } catch (_) {}
  }, 1200);

  if (phoneUrlInput) {
    phoneUrlInput.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        toggleRemoteStream();
      }
    });

    phoneUrlInput.addEventListener("input", () => {
      localStorage.setItem("sensor_dashboard_url", phoneUrlInput.value);
    });
  }

  // Handle window resize
  let resizeTimeout;
  window.addEventListener("resize", () => {
    clearTimeout(resizeTimeout);
    resizeTimeout = setTimeout(() => {
      initCanvases();
    }, 200);
  });

  /* ── Initialization ── */
  window.addEventListener("DOMContentLoaded", () => {
    initCanvases();
  });
  initCanvases();

  // Auto-connect to stream
  if (phoneUrlInput) {
    phoneUrlInput.value = window.location.origin;
  }
  setTimeout(() => {
    if (!connected && !abortCtrl) {
      connect();
    }
  }, 200);

})();
