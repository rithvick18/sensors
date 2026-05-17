#!/usr/bin/env python3
"""Bridge phone sensor UDP packets into an LSL stream for MNE-Python."""

from __future__ import annotations

import argparse
import json
import socket
import time
from collections.abc import Iterable
from typing import Any, Protocol

try:
    from mne_lsl.lsl import StreamInfo, StreamOutlet
    USING_MNE_LSL = True
except ImportError:
    from pylsl import StreamInfo, StreamOutlet  # type: ignore[no-redef]
    USING_MNE_LSL = False

CHANNEL_NAMES = (
    "accel_x",
    "accel_y",
    "accel_z",
    "gyro_x",
    "gyro_y",
    "gyro_z",
)


class Outlet(Protocol):
    def push_sample(self, sample: list[float]) -> None: ...


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Receive Flutter sensor packets over UDP or HTTP and publish LSL."
    )
    parser.add_argument(
        "--transport",
        choices=("udp", "http"),
        default="udp",
        help="Input transport to consume.",
    )
    parser.add_argument("--host", default="0.0.0.0", help="UDP host to bind.")
    parser.add_argument("--port", type=int, default=12345, help="UDP port to bind.")
    parser.add_argument(
        "--url",
        default="http://127.0.0.1:8080/stream",
        help="HTTP /stream or /sample URL when --transport http is used.",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=0.01,
        help="Seconds between HTTP /sample polls if --url is not a stream endpoint.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=10.0,
        help="HTTP connect/read timeout in seconds.",
    )
    parser.add_argument("--name", default="PhoneSensors", help="LSL stream name.")
    parser.add_argument("--type", default="Sensors", help="LSL stream type.")
    parser.add_argument(
        "--sfreq",
        type=float,
        default=100.0,
        help="Nominal LSL sampling frequency in Hz.",
    )
    parser.add_argument(
        "--source-id",
        default="phone_sensor_stream",
        help="Stable LSL source id.",
    )
    parser.add_argument(
        "--print-every",
        type=float,
        default=2.0,
        help="Seconds between throughput status lines. Use 0 to disable.",
    )
    return parser.parse_args()


def make_outlet(args: argparse.Namespace) -> Outlet:
    if USING_MNE_LSL:
        info = StreamInfo(
            name=args.name,
            stype=args.type,
            n_channels=len(CHANNEL_NAMES),
            sfreq=args.sfreq,
            dtype="float32",
            source_id=args.source_id,
        )
        info.set_channel_names(list(CHANNEL_NAMES))
        info.set_channel_types("misc")
        info.set_channel_units("none")
    else:
        info = StreamInfo(
            args.name,
            args.type,
            len(CHANNEL_NAMES),
            args.sfreq,
            "float32",
            args.source_id,
        )
        channels = info.desc().append_child("channels")
        for name in CHANNEL_NAMES:
            channel = channels.append_child("channel")
            channel.append_child_value("label", name)
            channel.append_child_value("type", "misc")
            channel.append_child_value("unit", "none")
    return StreamOutlet(info)


def decode_sample(payload: bytes | str) -> list[float]:
    if isinstance(payload, bytes):
        payload = payload.decode("utf-8")
    packet: dict[str, Any] = json.loads(payload)
    accel = packet["accel"]
    gyro = packet["gyro"]
    return [
        float(accel["x"]),
        float(accel["y"]),
        float(accel["z"]),
        float(gyro["x"]),
        float(gyro["y"]),
        float(gyro["z"]),
    ]


def iter_udp_payloads(args: argparse.Namespace) -> Iterable[bytes]:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((args.host, args.port))

    print(f"Listening on udp://{args.host}:{args.port}")
    try:
        while True:
            payload, _address = sock.recvfrom(2048)
            yield payload
    finally:
        sock.close()


def iter_http_payloads(args: argparse.Namespace) -> Iterable[str]:
    try:
        import requests
    except ImportError as error:
        raise SystemExit(
            "HTTP transport needs requests. Install it with: pip install requests"
        ) from error

    print(f"Connecting to {args.url}")
    if args.url.rstrip("/").endswith("/stream"):
        with requests.get(args.url, stream=True, timeout=args.timeout) as response:
            response.raise_for_status()
            for line in response.iter_lines(decode_unicode=True):
                if line:
                    yield line
        return

    session = requests.Session()
    while True:
        response = session.get(args.url, timeout=args.timeout)
        response.raise_for_status()
        yield response.text
        time.sleep(max(args.poll_interval, 0.001))


def publish_payloads(
    payloads: Iterable[bytes | str],
    outlet: Outlet,
    print_every: float,
) -> None:
    count = 0
    dropped = 0
    started_at = time.monotonic()
    last_print = started_at

    try:
        for payload in payloads:
            try:
                outlet.push_sample(decode_sample(payload))
                count += 1
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
                dropped += 1
                print(f"Skipped malformed packet: {error}")

            now = time.monotonic()
            if print_every > 0 and now - last_print >= print_every:
                elapsed = max(now - started_at, 0.001)
                print(
                    f"{count} samples streamed "
                    f"({count / elapsed:.1f} Hz avg, dropped {dropped})"
                )
                last_print = now
    except KeyboardInterrupt:
        print("\nStopped.")


def main() -> None:
    args = parse_args()
    outlet = make_outlet(args)

    print(f"Publishing LSL stream '{args.name}' at {args.sfreq:g} Hz")
    print("Channel order:", ", ".join(CHANNEL_NAMES))

    payloads = (
        iter_http_payloads(args)
        if args.transport == "http"
        else iter_udp_payloads(args)
    )
    publish_payloads(payloads, outlet, args.print_every)


if __name__ == "__main__":
    main()
