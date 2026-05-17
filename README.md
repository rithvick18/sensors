# Sensors - LSL Streamer

Flutter app for viewing phone accelerometer and gyroscope values, with UDP streaming to a Python bridge that republishes the samples as an LSL stream for MNE-Python workflows.

## Run the Flutter app

```sh
flutter pub get
flutter run
```

The app displays live accelerometer and gyroscope values as soon as sensor events are available. Enter your computer's LAN IP address and UDP port, then press Start to send samples.

## Local HTTP access

Press **Start HTTP** in the app to expose the latest sensor data on your local network. The app shows two URLs:

```text
http://PHONE_OR_COMPUTER_IP:8080/sample
http://PHONE_OR_COMPUTER_IP:8080/stream
```

`/sample` returns one JSON sample per request. `/stream` returns newline-delimited JSON at about 100 Hz, which works well with Python `requests`:

```python
import requests

with requests.get("http://PHONE_OR_COMPUTER_IP:8080/stream", stream=True) as r:
    r.raise_for_status()
    for line in r.iter_lines(decode_unicode=True):
        if line:
            print(line)
```

## Run the MNE-LSL bridge

Create a Python environment on the computer receiving the phone data:

```sh
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python receiver.py --port 12345
```

Use the same port in the Flutter app. The receiver publishes an LSL stream named `PhoneSensors` with this channel order:

```text
accel_x, accel_y, accel_z, gyro_x, gyro_y, gyro_z
```

Accelerometer values are in `m/s^2`; gyroscope values are in `rad/s`.

## Useful receiver options

```sh
python receiver.py --host 0.0.0.0 --port 12345 --name PhoneSensors --sfreq 100
python receiver.py --transport http --url http://PHONE_OR_COMPUTER_IP:8080/stream
```

If packets do not arrive, check that both devices are on the same network and that your firewall allows UDP on the selected port or TCP access to the HTTP port. On macOS, allow incoming connections when the system prompts you.
