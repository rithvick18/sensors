/**
 * Sensor Dashboard — app.js
 * Connects to the Flutter phone HTTP server's /stream endpoint (NDJSON)
 * and renders accelerometer + gyroscope data in real-time.
 */

(() => {
  "use strict";

  /* ── Configuration ── */
  const HISTORY_LENGTH = 200;
  const ACCEL_SCALE = 20;   // m/s²
  const GYRO_SCALE = 10;    // rad/s
  const MAX_FEED_LINES = 30;
  const CHART_FPS = 30;

  /* ── DOM refs ── */
  const $ = (id) => document.getElementById(id);

  const phoneUrlInput   = $("phone-url");
  const connectBtn      = $("connect-btn");
  const connectBtnLabel = $("connect-btn-label");
  const statusDot       = $("status-dot");
  const statusText      = $("status-text");
  const connectionStatus = $("connection-status");
  const statsBar        = $("stats-bar");
  const dashboard       = $("dashboard");
  const orientSection   = $("orientation-section");

  // Stats
  const statSamplesVal  = $("stat-samples-value");
  const statRateVal     = $("stat-rate-value");
  const statLatencyVal  = $("stat-latency-value");
  const statPacketVal   = $("stat-packet-value");

  // Accel
  const accelMagEl = $("accel-magnitude");
  const accelXBar  = $("accel-x-bar");
  const accelYBar  = $("accel-y-bar");
  const accelZBar  = $("accel-z-bar");
  const accelXVal  = $("accel-x-value");
  const accelYVal  = $("accel-y-value");
  const accelZVal  = $("accel-z-value");
  const accelCanvas = $("accel-chart");

  // Gyro
  const gyroMagEl  = $("gyro-magnitude");
  const gyroXBar   = $("gyro-x-bar");
  const gyroYBar   = $("gyro-y-bar");
  const gyroZBar   = $("gyro-z-bar");
  const gyroXVal   = $("gyro-x-value");
  const gyroYVal   = $("gyro-y-value");
  const gyroZVal   = $("gyro-z-value");
  const gyroCanvas = $("gyro-chart");

  // Feed
  const dataFeed = $("data-feed");

  /* ── State ── */
  let reader = null;
  let abortCtrl = null;
  let connected = false;
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

  // History for charts (per-axis for richer visualization)
  const accelXHistory = [];
  const accelYHistory = [];
  const accelZHistory = [];
  const gyroXHistory  = [];
  const gyroYHistory  = [];
  const gyroZHistory  = [];

  // Feed lines
  const feedLines = [];

  /* ── Canvas Setup ── */
  function setupCanvas(canvas) {
    const dpr = window.devicePixelRatio || 1;
    const rect = canvas.getBoundingClientRect();
    canvas.width = rect.width * dpr;
    canvas.height = rect.height * dpr;
    const ctx = canvas.getContext("2d");
    ctx.scale(dpr, dpr);
    return ctx;
  }

  let accelCtx, gyroCtx;

  function initCanvases() {
    accelCtx = setupCanvas(accelCanvas);
    gyroCtx  = setupCanvas(gyroCanvas);
  }

  /* ── Chart Drawing ── */
  const CHART_COLORS = {
    accel: {
      x: { line: "rgba(59, 130, 246, 1)",   fill: "rgba(59, 130, 246, 0.08)" },
      y: { line: "rgba(34, 197, 94, 1)",     fill: "rgba(34, 197, 94, 0.08)" },
      z: { line: "rgba(249, 115, 22, 1)",    fill: "rgba(249, 115, 22, 0.08)" },
    },
    gyro: {
      x: { line: "rgba(168, 85, 247, 1)",    fill: "rgba(168, 85, 247, 0.08)" },
      y: { line: "rgba(236, 72, 153, 1)",    fill: "rgba(236, 72, 153, 0.08)" },
      z: { line: "rgba(20, 184, 166, 1)",    fill: "rgba(20, 184, 166, 0.08)" },
    },
  };

  function drawChart(ctx, canvas, histories, colors, scale) {
    const w = canvas.getBoundingClientRect().width;
    const h = canvas.getBoundingClientRect().height;
    ctx.clearRect(0, 0, w, h);

    // Grid lines
    ctx.strokeStyle = "rgba(255, 255, 255, 0.04)";
    ctx.lineWidth = 1;
    const gridLines = 4;
    for (let i = 1; i < gridLines; i++) {
      const y = (h / gridLines) * i;
      ctx.beginPath();
      ctx.moveTo(0, y);
      ctx.lineTo(w, y);
      ctx.stroke();
    }

    // Zero line
    ctx.strokeStyle = "rgba(255, 255, 255, 0.1)";
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
      if (data.length < 2) continue;

      const step = w / (HISTORY_LENGTH - 1);
      const offset = HISTORY_LENGTH - data.length;

      // Build path
      ctx.beginPath();
      for (let i = 0; i < data.length; i++) {
        const x = (offset + i) * step;
        // Map value from [-scale, +scale] to [h, 0]
        const normalized = (data[i] + scale) / (2 * scale);
        const y = h - (normalized * h);
        if (i === 0) ctx.moveTo(x, y);
        else ctx.lineTo(x, y);
      }

      // Stroke
      ctx.strokeStyle = color.line;
      ctx.lineWidth = 1.5;
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
      ctx.font = "600 10px 'Inter', sans-serif";
      const textW = ctx.measureText(label).width;
      legendX -= textW;
      ctx.fillStyle = colors[axes[a]].line;
      ctx.fillText(label, legendX, legendY);
      legendX -= 14;
      ctx.beginPath();
      ctx.arc(legendX + 4, legendY - 3, 3, 0, Math.PI * 2);
      ctx.fill();
      legendX -= 12;
    }
  }

  function renderCharts() {
    if (!connected) return;

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
    accelMagEl.textContent = accelMag.toFixed(3);
    accelXVal.textContent = ax.toFixed(3);
    accelYVal.textContent = ay.toFixed(3);
    accelZVal.textContent = az.toFixed(3);
    accelXBar.style.width = `${Math.min(Math.abs(ax) / ACCEL_SCALE * 100, 100)}%`;
    accelYBar.style.width = `${Math.min(Math.abs(ay) / ACCEL_SCALE * 100, 100)}%`;
    accelZBar.style.width = `${Math.min(Math.abs(az) / ACCEL_SCALE * 100, 100)}%`;

    // Gyroscope
    const gyroMag = Math.sqrt(gx * gx + gy * gy + gz * gz);
    gyroMagEl.textContent = gyroMag.toFixed(3);
    gyroXVal.textContent = gx.toFixed(3);
    gyroYVal.textContent = gy.toFixed(3);
    gyroZVal.textContent = gz.toFixed(3);
    gyroXBar.style.width = `${Math.min(Math.abs(gx) / GYRO_SCALE * 100, 100)}%`;
    gyroYBar.style.width = `${Math.min(Math.abs(gy) / GYRO_SCALE * 100, 100)}%`;
    gyroZBar.style.width = `${Math.min(Math.abs(gz) / GYRO_SCALE * 100, 100)}%`;

    // Stats
    statSamplesVal.textContent = totalSamples.toLocaleString();
    statRateVal.textContent = `${currentRate} Hz`;
    statPacketVal.textContent = lastSeq || "—";
  }

  function pushHistory(arr, val) {
    arr.push(val);
    if (arr.length > HISTORY_LENGTH) arr.shift();
  }

  function processSample(data) {
    const accel = data.accel;
    const gyro = data.gyro;
    if (!accel || !gyro) return;

    ax = accel.x; ay = accel.y; az = accel.z;
    gx = gyro.x;  gy = gyro.y;  gz = gyro.z;
    lastSeq = data.sequence || 0;
    totalSamples++;
    rateCounter++;

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
    const container = dataFeed.parentElement;
    const shouldScroll = container.scrollTop + container.clientHeight >= container.scrollHeight - 20;

    dataFeed.innerHTML = feedLines.map(line => {
      const colored = line
        .replace(/"([^"]+)":/g, '<span class="feed-key">"$1"</span>:')
        .replace(/:(\s*)([-\d.]+)/g, ':$1<span class="feed-number">$2</span>');
      return `<span class="feed-line">${colored}</span>`;
    }).join("\n");

    if (shouldScroll) {
      container.scrollTop = container.scrollHeight;
    }
  }

  /* ── Connection ── */
  function setConnectionState(state) {
    statusDot.className = "status-dot";
    connectionStatus.className = "connection-status";

    if (state === "connected") {
      connected = true;
      statusDot.classList.add("active");
      connectionStatus.classList.add("connected");
      statusText.textContent = "Connected";
      connectBtnLabel.textContent = "Disconnect";
      connectBtn.classList.add("connected");
      statsBar.classList.add("visible");
      dashboard.classList.add("visible");
      orientSection.classList.add("visible");
    } else if (state === "connecting") {
      connected = false;
      statusDot.classList.add("connecting");
      connectionStatus.classList.add("connecting");
      statusText.textContent = "Connecting…";
      connectBtnLabel.textContent = "Cancel";
      connectBtn.classList.add("connected");
    } else {
      connected = false;
      statusText.textContent = "Disconnected";
      connectBtnLabel.textContent = "Connect";
      connectBtn.classList.remove("connected");
    }
  }

  async function connect() {
    const baseUrl = phoneUrlInput.value.trim().replace(/\/+$/, "");
    if (!baseUrl) {
      phoneUrlInput.focus();
      return;
    }

    const streamUrl = `${baseUrl}/stream`;
    setConnectionState("connecting");

    // Reset state
    totalSamples = 0;
    rateCounter = 0;
    currentRate = 0;
    lastRateCheck = performance.now();
    startTime = Date.now();
    accelXHistory.length = 0;
    accelYHistory.length = 0;
    accelZHistory.length = 0;
    gyroXHistory.length = 0;
    gyroYHistory.length = 0;
    gyroZHistory.length = 0;
    feedLines.length = 0;

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
      chartAnimFrame = requestAnimationFrame(renderCharts);

      // Start rate counter
      lastRateCheck = performance.now();
      rateInterval = setInterval(() => {
        const now = performance.now();
        const dt = (now - lastRateCheck) / 1000;
        currentRate = Math.round(rateCounter / dt);
        rateCounter = 0;
        lastRateCheck = now;
      }, 1000);

      // Start elapsed timer
      elapsedInterval = setInterval(() => {
        if (!startTime) return;
        const elapsed = Math.floor((Date.now() - startTime) / 1000);
        const m = String(Math.floor(elapsed / 60)).padStart(2, "0");
        const s = String(elapsed % 60).padStart(2, "0");
        statLatencyVal.textContent = `${m}:${s}`;
      }, 500);

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
            processSample(data);
            scheduleUIUpdate();
          } catch (e) {
            // skip malformed
          }
        }
      }

    } catch (err) {
      if (err.name !== "AbortError") {
        console.error("Connection error:", err);
        alert(`Connection failed: ${err.message}\n\nMake sure the HTTP server is running on your phone and both devices are on the same network.`);
      }
    } finally {
      disconnect(true);
    }
  }

  function disconnect(fromError = false) {
    if (reader) {
      try { reader.cancel(); } catch (e) {}
      reader = null;
    }
    if (abortCtrl) {
      abortCtrl.abort();
      abortCtrl = null;
    }
    if (rateInterval) {
      clearInterval(rateInterval);
      rateInterval = null;
    }
    if (elapsedInterval) {
      clearInterval(elapsedInterval);
      elapsedInterval = null;
    }
    if (chartAnimFrame) {
      cancelAnimationFrame(chartAnimFrame);
      chartAnimFrame = null;
    }

    if (!fromError || connected) {
      setConnectionState("disconnected");
    }
  }

  /* ── Event Listeners ── */
  connectBtn.addEventListener("click", () => {
    if (connected || abortCtrl) {
      disconnect();
    } else {
      connect();
    }
  });

  phoneUrlInput.addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
      e.preventDefault();
      if (!connected && !abortCtrl) connect();
    }
  });

  // Handle window resize
  let resizeTimeout;
  window.addEventListener("resize", () => {
    clearTimeout(resizeTimeout);
    resizeTimeout = setTimeout(() => {
      if (connected) initCanvases();
    }, 200);
  });

  /* ── Initialization ── */
  // Try to load saved URL
  const saved = localStorage.getItem("sensor_dashboard_url");
  if (saved) phoneUrlInput.value = saved;

  // Save URL on change
  phoneUrlInput.addEventListener("input", () => {
    localStorage.setItem("sensor_dashboard_url", phoneUrlInput.value);
  });

})();
