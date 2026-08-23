# Sensors - Real-Time IMU Streamer & LSL Bridge

A comprehensive system for streaming smartphone Accelerometer and Gyroscope (IMU) data in real time over UDP and HTTP, republishing samples as Lab Streaming Layer (LSL) streams for MNE-Python workflows, and monitoring/controlling streams through an interactive Web Dashboard.

---

## 🌟 Key Features

- **📱 Mobile App (Flutter)**:
  - Live real-time visualization of 3-axis Accelerometer ($m/s^2$) and Gyroscope ($rad/s$).
  - High-frequency streaming (up to 100 Hz).
  - Dual streaming modes: **UDP Datagrams** and **Local HTTP Server** (`/sample` and `/stream`).
  - Automatic local IP/interface detection and QR code generation for quick pairing.
  - Remote control listening support (start/stop streaming from the web dashboard).
- **🖥️ Unified Server Hub (`server.py`)**:
  - Multi-threaded Python server handling UDP sensor ingestion and HTTP web dashboard hosting concurrently.
  - Built-in **LSL Stream Outlet** (`PhoneSensors`) for direct integration with MNE-Python, LabRecorder, OpenViBE, and BCILAB.
  - Interactive **Web Dashboard** served on `http://localhost:3000` with live Chart.js time-series plots, FFT frequency spectrum analysis, 3D device orientation visualizer, signal statistics, and CSV recording.
  - 2-way remote start & stop streaming control between phone and web dashboard.
- **⚡ Lightweight Receiver CLI (`receiver.py`)**:
  - Headless CLI tool to consume UDP packets or HTTP streams and publish an LSL stream.
- **📓 Analysis Notebook**:
  - `Mobile Accl with LSL in VS Code.ipynb` for consuming LSL streams in real time using MNE-Python.

---

## 📁 Repository Structure

```text
├── lib/                             # Flutter application source code
│   └── main.dart                    # Main UI, sensor listeners, UDP/HTTP streamers
├── web-dashboard/                   # Web Dashboard frontend assets
│   ├── index.html                   # Dashboard UI layout & panels
│   ├── app.js                       # Real-time charts, 3D visualizer, SSE stream handling
│   └── style.css                    # Dashboard styling and theme
├── server.py                        # Unified Server: UDP ingestion + LSL outlet + Web server
├── receiver.py                      # Dedicated CLI bridge (UDP/HTTP -> LSL)
├── requirements.txt                 # Python dependencies
└── Mobile Accl with LSL in VS Code.ipynb  # Interactive MNE-Python LSL analysis notebook
```

---

## 🚀 Quick Start

### 1. Python Environment Setup

Create and activate a virtual environment on your computer:

```sh
python3 -m venv .venv
source .venv/bin/activate    # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Start the Unified Server Hub (Recommended)

Run the unified hub to ingest phone data, publish the LSL stream, and launch the Web Dashboard:

```sh
python server.py
```

Options:
- `--udp-port <port>`: UDP port to listen for phone packets (default: `12345`).
- `--web-port <port>`: HTTP port for the web dashboard (default: `3000`).
- `--lsl-name <name>`: Name of the published LSL stream (default: `PhoneSensors`).
- `--no-lsl`: Disable LSL publishing and only run the web dashboard & UDP listener.

Once started:
- Open your browser to **`http://localhost:3000`** (or `http://YOUR_COMPUTER_IP:3000`).
- The terminal will display your computer's local IP address.

---

### 3. Run the Flutter Mobile App

Install and launch the Flutter app on iOS or Android:

```sh
flutter pub get
flutter run
```

#### Connecting to the Server:
1. Ensure your smartphone and computer are connected to the **same Wi-Fi network**.
2. Enter your computer's IP address and UDP port (`12345` by default) in the app.
3. Tap **Start UDP** to begin streaming IMU samples.
4. You will see real-time data appear on both the Flutter app and the Web Dashboard (`http://localhost:3000`).

---

## 📡 Streaming Protocols & Data Formats

### Channel Specification

LSL Stream & UDP payload contains 6 channels in the following order:

| Channel Index | Name | Description | Unit |
| :--- | :--- | :--- | :--- |
| `0` | `accel_x` | Acceleration along X-axis | $m/s^2$ |
| `1` | `accel_y` | Acceleration along Y-axis | $m/s^2$ |
| `2` | `accel_z` | Acceleration along Z-axis | $m/s^2$ |
| `3` | `gyro_x` | Angular velocity around X-axis | $rad/s$ |
| `4` | `gyro_y` | Angular velocity around Y-axis | $rad/s$ |
| `5` | `gyro_z` | Angular velocity around Z-axis | $rad/s$ |

### 1. UDP JSON Payload Format

Each UDP datagram sent by the phone contains:

```json
{
  "timestamp": 1718000000.123,
  "accel": [0.02, 9.81, 0.15],
  "gyro": [0.001, -0.002, 0.000]
}
```

### 2. Standalone HTTP Server Mode

When running in HTTP mode from the app (**Start HTTP**):
- `http://PHONE_IP:8080/sample`: Returns a single JSON snapshot of latest sensor values.
- `http://PHONE_IP:8080/stream`: Returns a continuous stream of newline-delimited JSON (NDJSON) at ~100 Hz.

Consume in Python using `requests`:

```python
import requests

with requests.get("http://PHONE_IP:8080/stream", stream=True) as r:
    r.raise_for_status()
    for line in r.iter_lines(decode_unicode=True):
        if line:
            print(line)
```

---

## 🛠️ Alternative: Headless Receiver CLI (`receiver.py`)

If you do not need the Web Dashboard and only require an LSL stream bridge:

```sh
# UDP mode (default)
python receiver.py --host 0.0.0.0 --port 12345 --name PhoneSensors --sfreq 100

# HTTP stream mode
python receiver.py --transport http --url http://PHONE_IP:8080/stream
```

---

## 🧠 MNE-Python & LSL Integration

You can easily capture and process the live LSL stream in Python:

```python
from pylsl import StreamInlet, resolve_byprop

# Resolve the PhoneSensors LSL stream
print("Looking for an LSL stream with name 'PhoneSensors'...")
streams = resolve_byprop("name", "PhoneSensors", timeout=5)
if not streams:
    raise RuntimeError("Stream not found!")

inlet = StreamInlet(streams[0])

while True:
    sample, timestamp = inlet.pull_sample()
    print(f"Timestamp: {timestamp:.3f} | Sample: {sample}")
```

For interactive analysis, see [`Mobile Accl with LSL in VS Code.ipynb`](file:///Users/rithvick/Desktop/sensors/Mobile%20Accl%20with%20LSL%20in%20VS%20Code.ipynb).

---

## 🔧 Troubleshooting

- **No packets received / Web dashboard shows disconnected**:
  - Confirm both devices are connected to the same Wi-Fi subnet.
  - Disable VPNs or hotspot client isolation if applicable.
  - Check your computer's firewall settings to permit incoming UDP traffic on port `12345` and TCP traffic on port `3000` (macOS may prompt you to allow incoming connections).
- **LSL stream not discovered**:
  - Ensure `pylsl` or `mne-lsl` is installed (`pip install -r requirements.txt`).
  - Check that liblsl shared library can be resolved by your environment.
