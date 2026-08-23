#!/usr/bin/env python3
"""
Unified Sensor Hub:
- Receives UDP packets from iPhone/Android on port 12345
- Publishes LSL Stream for MNE-Python
- Serves the Web Dashboard on http://localhost:3000
- Provides 2-way Remote Start & Stop control between Phone and Web
- Multi-threaded (ThreadingHTTPServer) for concurrent streaming & control
"""

from __future__ import annotations

import argparse
import json
import os
import queue
import socket
import sys
import threading
import time
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from typing import Any

try:
    from pylsl import StreamInfo, StreamOutlet
    HAS_LSL = True
except ImportError:
    try:
        from mne_lsl.lsl import StreamInfo, StreamOutlet
        HAS_LSL = True
    except ImportError:
        HAS_LSL = False

CHANNEL_NAMES = ("accel_x", "accel_y", "accel_z", "gyro_x", "gyro_y", "gyro_z")

dashboard_clients: list[queue.Queue[str]] = []
phone_control_clients: list[queue.Queue[str]] = []
clients_lock = threading.Lock()
control_lock = threading.Lock()

latest_sample: dict[str, Any] = {}
stats = {
    "packets_received": 0,
    "start_time": time.time(),
    "last_packet_time": 0.0,
    "last_phone_addr": None,
}


def get_local_ip() -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


def is_streaming_active() -> bool:
    return (time.time() - stats["last_packet_time"]) < 1.5


def broadcast_phone_command(action: str) -> None:
    cmd_json = json.dumps({"action": action, "timestamp": time.time()})
    with control_lock:
        for q in phone_control_clients:
            try:
                q.put_nowait(cmd_json)
            except queue.Full:
                pass

    # Also send UDP packet to phone on port 12346 if address is known
    phone_addr = stats["last_phone_addr"]
    if phone_addr:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.sendto(cmd_json.encode("utf-8"), (phone_addr[0], 12346))
            s.close()
        except Exception:
            pass


def decode_sample(data: dict[str, Any]) -> list[float]:
    accel = data["accel"]
    gyro = data["gyro"]
    return [
        float(accel["x"]),
        float(accel["y"]),
        float(accel["z"]),
        float(gyro["x"]),
        float(gyro["y"]),
        float(gyro["z"]),
    ]


class DashboardHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        dashboard_dir = os.path.join(os.path.dirname(__file__), "web-dashboard")
        super().__init__(*args, directory=dashboard_dir, **kwargs)

    def end_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        super().end_headers()

    def do_OPTIONS(self) -> None:
        self.send_response(204)
        self.end_headers()

    def do_GET(self) -> None:
        clean_path = self.path.split("?")[0].rstrip("/")

        # Web Dashboard Sensor Stream (NDJSON)
        if clean_path in ("/stream", "/live"):
            self.send_response(200)
            self.send_header("Content-Type", "application/x-ndjson; charset=utf-8")
            self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
            self.send_header("Connection", "keep-alive")
            self.end_headers()
            self.wfile.flush()

            # Immediate handshake to resolve client fetch promise instantly
            self.wfile.write(b"{\"action\":\"CONNECTED\"}\n")
            self.wfile.flush()

            q: queue.Queue[str] = queue.Queue(maxsize=100)
            with clients_lock:
                dashboard_clients.append(q)

            try:
                while True:
                    try:
                        line = q.get(timeout=1.0)
                        self.wfile.write((line + "\n").encode("utf-8"))
                        self.wfile.flush()
                    except queue.Empty:
                        self.wfile.write(b"\n")
                        self.wfile.flush()
            except (ConnectionResetError, BrokenPipeError, ConnectionAbortedError):
                pass
            finally:
                with clients_lock:
                    if q in dashboard_clients:
                        dashboard_clients.remove(q)
            return

        # Phone Remote Control Command Stream (NDJSON)
        if clean_path == "/api/control/stream":
            self.send_response(200)
            self.send_header("Content-Type", "application/x-ndjson; charset=utf-8")
            self.send_header("Cache-Control", "no-cache, no-store")
            self.send_header("Connection", "keep-alive")
            self.end_headers()

            q: queue.Queue[str] = queue.Queue(maxsize=20)
            init_msg = json.dumps({"action": "STATUS", "is_streaming": is_streaming_active()})
            self.wfile.write((init_msg + "\n").encode("utf-8"))
            self.wfile.flush()

            with control_lock:
                phone_control_clients.append(q)

            print(f"📱 Phone connected to Remote Control channel ({len(phone_control_clients)} active)")

            try:
                while True:
                    try:
                        cmd = q.get(timeout=5.0)
                        self.wfile.write((cmd + "\n").encode("utf-8"))
                        self.wfile.flush()
                    except queue.Empty:
                        heartbeat = json.dumps({"action": "HEARTBEAT", "time": time.time()})
                        self.wfile.write((heartbeat + "\n").encode("utf-8"))
                        self.wfile.flush()
            except (ConnectionResetError, BrokenPipeError, ConnectionAbortedError):
                pass
            finally:
                with control_lock:
                    if q in phone_control_clients:
                        phone_control_clients.remove(q)
                print(f"📱 Phone disconnected from Remote Control channel ({len(phone_control_clients)} active)")
            return

        if clean_path in ("/sample", "/latest"):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(latest_sample).encode("utf-8"))
            return

        if clean_path in ("/status", "/api/status"):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            is_active = is_streaming_active()
            status_data = {
                "local_ip": get_local_ip(),
                "udp_port": 12345,
                "packets_received": stats["packets_received"],
                "active_dashboard_clients": len(dashboard_clients),
                "phone_connected": len(phone_control_clients) > 0 or is_active,
                "is_streaming": is_active,
                "lsl_enabled": HAS_LSL,
            }
            self.wfile.write(json.dumps(status_data).encode("utf-8"))
            return

        super().do_GET()

    def do_POST(self) -> None:
        clean_path = self.path.split("?")[0].rstrip("/")

        if clean_path in ("/control", "/api/control"):
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length).decode("utf-8") if content_length > 0 else "{}"
            try:
                payload = json.loads(body)
            except Exception:
                payload = {}

            action = payload.get("action", "").upper()
            if action == "TOGGLE":
                action = "STOP" if is_streaming_active() else "START"

            if action in ("START", "STOP"):
                print(f"🎛️  Remote Command Received: {action}")
                broadcast_phone_command(action)
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "ok": True,
                    "action": action,
                    "is_streaming": action == "START",
                }).encode("utf-8"))
            else:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Invalid action. Use START, STOP, or TOGGLE."}).encode("utf-8"))
            return

        self.send_response(404)
        self.end_headers()

    def log_message(self, format: str, *args: Any) -> None:
        if any(p in args[0] for p in ("GET /stream", "GET /live", "GET /api/control/stream", "GET /api/status")):
            return
        super().log_message(format, *args)


def run_udp_listener(port: int, lsl_outlet: StreamOutlet | None) -> None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", port))
    
    last_print = time.time()
    packet_count_since_print = 0

    while True:
        try:
            raw_data, addr = sock.recvfrom(4096)
            stats["last_phone_addr"] = addr
            text = raw_data.decode("utf-8", errors="ignore").strip()
            if not text:
                continue

            parsed = json.loads(text)
            latest_sample.clear()
            latest_sample.update(parsed)

            stats["packets_received"] += 1
            stats["last_packet_time"] = time.time()
            packet_count_since_print += 1

            if lsl_outlet is not None:
                try:
                    sample_vals = decode_sample(parsed)
                    lsl_outlet.push_sample(sample_vals)
                except Exception:
                    pass

            with clients_lock:
                for q in dashboard_clients:
                    try:
                        q.put_nowait(text)
                    except queue.Full:
                        pass

            now = time.time()
            if now - last_print >= 2.0:
                hz = packet_count_since_print / (now - last_print)
                packet_count_since_print = 0
                last_print = now
                if hz > 0:
                    print(f"📡 Receiving live data: {hz:.0f} Hz ({stats['packets_received']} total packets) -> Dashboard + LSL active")

        except Exception:
            time.sleep(0.01)


def main() -> None:
    parser = argparse.ArgumentParser(description="Sensor Streamer Unified Hub")
    parser.add_argument("--udp-port", type=int, default=12345, help="UDP port for phone packets")
    parser.add_argument("--web-port", type=int, default=3000, help="Web dashboard HTTP port")
    parser.add_argument("--no-lsl", action="store_true", help="Disable LSL publishing")
    args = parser.parse_args()

    local_ip = get_local_ip()

    lsl_outlet = None
    if HAS_LSL and not args.no_lsl:
        info = StreamInfo("PhoneSensors", "Sensors", len(CHANNEL_NAMES), 100.0, "float32", "phone_sensor_stream")
        lsl_outlet = StreamOutlet(info)
        print("✅ LSL Stream 'PhoneSensors' initialized")
    else:
        print("ℹ️  LSL Stream disabled (pylsl not active)")

    udp_thread = threading.Thread(
        target=run_udp_listener,
        args=(args.udp_port, lsl_outlet),
        daemon=True,
    )
    udp_thread.start()

    server = ThreadingHTTPServer(("0.0.0.0", args.web_port), DashboardHandler)
    server.daemon_threads = True
    print(f"""
╔══════════════════════════════════════════════════════════════════════╗
║             🚀 SENSOR STREAMER HUB — 2-WAY CONTROL READY             ║
╠══════════════════════════════════════════════════════════════════════╣
║                                                                      ║
║  1. Open Web Dashboard in browser:                                   ║
║     👉 http://localhost:{args.web_port:<5}                                    ║
║                                                                      ║
║  2. Control from anywhere:                                           ║
║     • Click 'Start Streaming' on Web Dashboard                       ║
║       OR tap 'Start' on your iPhone!                                 ║
║                                                                      ║
╚══════════════════════════════════════════════════════════════════════╝
""")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping hub...")
        server.server_close()


if __name__ == "__main__":
    main()
