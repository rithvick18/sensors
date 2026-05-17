import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math' as math;

import 'package:flutter/material.dart';
import 'package:sensors_plus/sensors_plus.dart';

void main() {
  runApp(const MyApp());
}

class MyApp extends StatelessWidget {
  const MyApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      debugShowCheckedModeBanner: false,
      title: 'Sensor LSL Streamer',
      theme: ThemeData(
        useMaterial3: true,
        colorScheme: ColorScheme.fromSeed(seedColor: const Color(0xFF1565C0)),
        scaffoldBackgroundColor: const Color(0xFFF5F7FA),
      ),
      home: const SensorStreamerPage(),
    );
  }
}

class SensorStreamerPage extends StatefulWidget {
  const SensorStreamerPage({super.key});

  @override
  State<SensorStreamerPage> createState() => _SensorStreamerPageState();
}

class _SensorStreamerPageState extends State<SensorStreamerPage> {
  static const _sensorSamplingPeriod = Duration(milliseconds: 10);
  static const _udpSamplingPeriod = Duration(milliseconds: 10);
  static const _httpSamplingPeriod = Duration(milliseconds: 10);
  static const _displayRefreshPeriod = Duration(milliseconds: 100);
  static const _historyLength = 80;

  final TextEditingController _ipController = TextEditingController(
    text: '192.168.1.100',
  );
  final TextEditingController _portController = TextEditingController(
    text: '12345',
  );
  final TextEditingController _httpPortController = TextEditingController(
    text: '8080',
  );

  RawDatagramSocket? _socket;
  HttpServer? _httpServer;
  StreamSubscription<AccelerometerEvent>? _accelSub;
  StreamSubscription<GyroscopeEvent>? _gyroSub;
  StreamSubscription<HttpRequest>? _httpRequestSub;
  Timer? _sendTimer;
  Timer? _displayTimer;
  Timer? _rateTimer;

  bool _isStreaming = false;
  bool _isHttpServing = false;
  double _accelX = 0;
  double _accelY = 0;
  double _accelZ = 0;
  double _gyroX = 0;
  double _gyroY = 0;
  double _gyroZ = 0;
  int _samplesSent = 0;
  int _httpSamplesServed = 0;
  int _httpClients = 0;
  int _samplesAtLastRateCheck = 0;
  double _sendRateHz = 0;
  DateTime? _streamStartedAt;
  DateTime? _lastPacketAt;
  DateTime? _lastHttpRequestAt;
  DateTime? _lastSensorAt;
  String? _sensorError;
  String? _httpUrl;

  final List<double> _accelHistory = <double>[];
  final List<double> _gyroHistory = <double>[];

  @override
  void initState() {
    super.initState();
    _startSensorListeners();
    _displayTimer = Timer.periodic(_displayRefreshPeriod, (_) {
      if (!mounted) return;
      setState(() {
        _addHistorySample(_accelHistory, _accelMagnitude);
        _addHistorySample(_gyroHistory, _gyroMagnitude);
      });
    });
    _rateTimer = Timer.periodic(const Duration(seconds: 1), (_) {
      if (!mounted) return;
      setState(() {
        _sendRateHz = (_samplesSent - _samplesAtLastRateCheck).toDouble();
        _samplesAtLastRateCheck = _samplesSent;
      });
    });
  }

  @override
  void dispose() {
    _stopStreaming(updateUi: false);
    _stopHttpServer(updateUi: false);
    _displayTimer?.cancel();
    _rateTimer?.cancel();
    _accelSub?.cancel();
    _gyroSub?.cancel();
    _ipController.dispose();
    _portController.dispose();
    _httpPortController.dispose();
    super.dispose();
  }

  double get _accelMagnitude =>
      math.sqrt(_accelX * _accelX + _accelY * _accelY + _accelZ * _accelZ);

  double get _gyroMagnitude =>
      math.sqrt(_gyroX * _gyroX + _gyroY * _gyroY + _gyroZ * _gyroZ);

  Duration get _elapsed {
    final startedAt = _streamStartedAt;
    if (startedAt == null) return Duration.zero;
    return DateTime.now().difference(startedAt);
  }

  void _startSensorListeners() {
    _accelSub = accelerometerEventStream(samplingPeriod: _sensorSamplingPeriod)
        .listen(
          (AccelerometerEvent event) {
            _accelX = event.x;
            _accelY = event.y;
            _accelZ = event.z;
            _lastSensorAt = DateTime.now();
            _sensorError = null;
          },
          onError: (Object error) {
            _setSensorError('Accelerometer unavailable');
          },
        );

    _gyroSub = gyroscopeEventStream(samplingPeriod: _sensorSamplingPeriod)
        .listen(
          (GyroscopeEvent event) {
            _gyroX = event.x;
            _gyroY = event.y;
            _gyroZ = event.z;
            _lastSensorAt = DateTime.now();
            _sensorError = null;
          },
          onError: (Object error) {
            _setSensorError('Gyroscope unavailable');
          },
        );
  }

  Future<void> _startStreaming() async {
    if (_isStreaming) return;

    final ip = _ipController.text.trim();
    final port = int.tryParse(_portController.text.trim());
    final address = InternetAddress.tryParse(ip);

    if (address == null) {
      _showError('Enter a valid PC IP address.');
      return;
    }
    if (port == null || port < 1 || port > 65535) {
      _showError('Enter a UDP port from 1 to 65535.');
      return;
    }

    try {
      _socket = await RawDatagramSocket.bind(InternetAddress.anyIPv4, 0);
      _samplesSent = 0;
      _samplesAtLastRateCheck = 0;
      _sendRateHz = 0;
      _streamStartedAt = DateTime.now();
      _lastPacketAt = null;

      _sendTimer = Timer.periodic(
        _udpSamplingPeriod,
        (_) => _sendPacket(address, port),
      );
      _sendPacket(address, port);

      if (!mounted) return;
      setState(() => _isStreaming = true);
    } catch (error) {
      _showError('Failed to start streaming: $error');
      _stopStreaming();
    }
  }

  void _sendPacket(InternetAddress address, int port) {
    final socket = _socket;
    if (socket == null) return;

    final data = jsonEncode(_buildSensorPacket(sequence: _samplesSent + 1));

    final bytes = utf8.encode(data);
    final sent = socket.send(bytes, address, port);
    if (sent > 0) {
      _samplesSent += 1;
      _lastPacketAt = DateTime.now();
    }
  }

  Future<void> _startHttpServer() async {
    if (_isHttpServing) return;

    final port = int.tryParse(_httpPortController.text.trim());
    if (port == null || port < 1 || port > 65535) {
      _showError('Enter an HTTP port from 1 to 65535.');
      return;
    }

    try {
      final server = await HttpServer.bind(
        InternetAddress.anyIPv4,
        port,
        shared: true,
      );
      _httpServer = server;
      _httpRequestSub = server.listen(
        _handleHttpRequest,
        onError: (Object error) => _showError('HTTP server error: $error'),
      );

      final lanIp = await _resolveLanIpv4();
      if (!mounted) return;
      setState(() {
        _isHttpServing = true;
        _httpSamplesServed = 0;
        _httpClients = 0;
        _lastHttpRequestAt = null;
        _httpUrl = 'http://${lanIp ?? server.address.address}:$port';
      });
    } catch (error) {
      _showError('Failed to start HTTP server: $error');
      await _stopHttpServer();
    }
  }

  Future<void> _stopHttpServer({bool updateUi = true}) async {
    await _httpRequestSub?.cancel();
    _httpRequestSub = null;
    await _httpServer?.close(force: true);
    _httpServer = null;

    void updateState() {
      _isHttpServing = false;
      _httpClients = 0;
      _httpUrl = null;
    }

    if (updateUi && mounted) {
      setState(updateState);
    } else {
      updateState();
    }
  }

  Future<void> _handleHttpRequest(HttpRequest request) async {
    _addCorsHeaders(request.response);

    if (request.method == 'OPTIONS') {
      request.response.statusCode = HttpStatus.noContent;
      await request.response.close();
      return;
    }

    if (request.method != 'GET') {
      await _writeJson(request.response, {
        'error': 'Only GET is supported.',
      }, statusCode: HttpStatus.methodNotAllowed);
      return;
    }

    final path = request.uri.path;
    if (path == '/' || path == '/health') {
      await _writeJson(request.response, {
        'status': 'ok',
        'sample_hz': 100,
        'endpoints': ['/sample', '/stream', '/health'],
        'active_stream_clients': _httpClients,
        'samples_served': _httpSamplesServed,
        'last_sensor_at': _lastSensorAt?.toIso8601String(),
      });
      return;
    }

    if (path == '/sample' || path == '/latest') {
      final packet = _nextHttpPacket();
      await _writeJson(request.response, packet);
      return;
    }

    if (path == '/stream') {
      await _streamHttpSamples(request.response);
      return;
    }

    await _writeJson(request.response, {
      'error': 'Unknown endpoint.',
    }, statusCode: HttpStatus.notFound);
  }

  Future<void> _streamHttpSamples(HttpResponse response) async {
    response.statusCode = HttpStatus.ok;
    response.headers.contentType = ContentType(
      'application',
      'x-ndjson',
      charset: 'utf-8',
    );
    response.bufferOutput = false;

    if (mounted) {
      setState(() {
        _httpClients += 1;
        _lastHttpRequestAt = DateTime.now();
      });
    }

    final stream = Stream<List<int>>.periodic(_httpSamplingPeriod, (_) {
      return utf8.encode('${jsonEncode(_nextHttpPacket())}\n');
    });

    try {
      await response.addStream(stream);
    } catch (_) {
      // Ignored, client disconnected
    } finally {
      if (mounted) {
        setState(() => _httpClients = math.max(0, _httpClients - 1));
      }
    }
  }

  Map<String, Object?> _nextHttpPacket() {
    _httpSamplesServed += 1;
    _lastHttpRequestAt = DateTime.now();
    return _buildSensorPacket(sequence: _httpSamplesServed);
  }

  Map<String, Object?> _buildSensorPacket({required int sequence}) {
    return {
      'timestamp':
          DateTime.now().microsecondsSinceEpoch /
          Duration.microsecondsPerSecond,
      'sequence': sequence,
      'accel': {'x': _accelX, 'y': _accelY, 'z': _accelZ},
      'gyro': {'x': _gyroX, 'y': _gyroY, 'z': _gyroZ},
    };
  }

  Future<void> _writeJson(
    HttpResponse response,
    Map<String, Object?> payload, {
    int statusCode = HttpStatus.ok,
  }) async {
    response.statusCode = statusCode;
    response.headers.contentType = ContentType.json;
    response.write(jsonEncode(payload));
    await response.close();
  }

  void _addCorsHeaders(HttpResponse response) {
    response.headers
      ..set(HttpHeaders.accessControlAllowOriginHeader, '*')
      ..set(HttpHeaders.accessControlAllowMethodsHeader, 'GET, OPTIONS')
      ..set(HttpHeaders.accessControlAllowHeadersHeader, 'Content-Type')
      ..set(HttpHeaders.cacheControlHeader, 'no-store');
  }

  Future<String?> _resolveLanIpv4() async {
    try {
      final interfaces = await NetworkInterface.list(
        includeLinkLocal: false,
        type: InternetAddressType.IPv4,
      );
      for (final interface in interfaces) {
        for (final address in interface.addresses) {
          if (!address.isLoopback) return address.address;
        }
      }
    } catch (_) {
      return null;
    }
    return null;
  }

  void _stopStreaming({bool updateUi = true}) {
    _sendTimer?.cancel();
    _sendTimer = null;
    _socket?.close();
    _socket = null;

    void updateState() {
      _isStreaming = false;
      _sendRateHz = 0;
      _streamStartedAt = null;
    }

    if (updateUi && mounted) {
      setState(updateState);
    } else {
      updateState();
    }
  }

  void _setSensorError(String message) {
    if (!mounted) {
      _sensorError = message;
      return;
    }
    setState(() => _sensorError = message);
  }

  void _showError(String message) {
    if (!mounted) return;
    ScaffoldMessenger.of(context).showSnackBar(
      SnackBar(content: Text(message), backgroundColor: Colors.red.shade700),
    );
  }

  void _addHistorySample(List<double> history, double value) {
    history.add(value);
    if (history.length > _historyLength) {
      history.removeRange(0, history.length - _historyLength);
    }
  }

  @override
  Widget build(BuildContext context) {
    final colorScheme = Theme.of(context).colorScheme;

    return Scaffold(
      appBar: AppBar(
        title: const Text('Sensor LSL Streamer'),
        actions: [
          Padding(
            padding: const EdgeInsets.only(right: 16),
            child: _StatusPill(isStreaming: _isStreaming),
          ),
        ],
      ),
      body: SafeArea(
        child: SingleChildScrollView(
          padding: const EdgeInsets.all(16),
          child: Center(
            child: ConstrainedBox(
              constraints: const BoxConstraints(maxWidth: 880),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.stretch,
                children: [
                  _StreamSummary(
                    isStreaming: _isStreaming,
                    samplesSent: _samplesSent,
                    sendRateHz: _sendRateHz,
                    elapsed: _elapsed,
                    lastPacketAt: _lastPacketAt,
                    lastSensorAt: _lastSensorAt,
                    sensorError: _sensorError,
                  ),
                  const SizedBox(height: 12),
                  _EndpointCard(
                    ipController: _ipController,
                    portController: _portController,
                    isStreaming: _isStreaming,
                    onToggleStreaming: _isStreaming
                        ? _stopStreaming
                        : () => _startStreaming(),
                  ),
                  const SizedBox(height: 12),
                  _HttpServerCard(
                    portController: _httpPortController,
                    isServing: _isHttpServing,
                    url: _httpUrl,
                    samplesServed: _httpSamplesServed,
                    activeClients: _httpClients,
                    lastRequestAt: _lastHttpRequestAt,
                    onToggleServing: _isHttpServing
                        ? () => _stopHttpServer()
                        : () => _startHttpServer(),
                  ),
                  const SizedBox(height: 12),
                  LayoutBuilder(
                    builder: (context, constraints) {
                      final cards = [
                        _SensorCard(
                          title: 'Accelerometer',
                          icon: Icons.speed,
                          unit: 'm/s^2',
                          x: _accelX,
                          y: _accelY,
                          z: _accelZ,
                          magnitude: _accelMagnitude,
                          history: _accelHistory,
                          color: colorScheme.primary,
                          scale: 20,
                        ),
                        _SensorCard(
                          title: 'Gyroscope',
                          icon: Icons.screen_rotation_alt,
                          unit: 'rad/s',
                          x: _gyroX,
                          y: _gyroY,
                          z: _gyroZ,
                          magnitude: _gyroMagnitude,
                          history: _gyroHistory,
                          color: colorScheme.tertiary,
                          scale: 10,
                        ),
                      ];

                      if (constraints.maxWidth >= 720) {
                        return Row(
                          crossAxisAlignment: CrossAxisAlignment.start,
                          children: [
                            Expanded(child: cards[0]),
                            const SizedBox(width: 12),
                            Expanded(child: cards[1]),
                          ],
                        );
                      }
                      return Column(
                        children: [
                          cards[0],
                          const SizedBox(height: 12),
                          cards[1],
                        ],
                      );
                    },
                  ),
                ],
              ),
            ),
          ),
        ),
      ),
    );
  }
}

class _StreamSummary extends StatelessWidget {
  const _StreamSummary({
    required this.isStreaming,
    required this.samplesSent,
    required this.sendRateHz,
    required this.elapsed,
    required this.lastPacketAt,
    required this.lastSensorAt,
    required this.sensorError,
  });

  final bool isStreaming;
  final int samplesSent;
  final double sendRateHz;
  final Duration elapsed;
  final DateTime? lastPacketAt;
  final DateTime? lastSensorAt;
  final String? sensorError;

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    final colorScheme = theme.colorScheme;
    final statusText =
        sensorError ??
        (lastSensorAt == null ? 'Waiting for sensor samples' : 'Sensors live');

    return Card(
      elevation: 0,
      color: Colors.white,
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Row(
              children: [
                Icon(
                  sensorError == null ? Icons.sensors : Icons.warning_amber,
                  color: sensorError == null
                      ? colorScheme.primary
                      : colorScheme.error,
                ),
                const SizedBox(width: 10),
                Expanded(
                  child: Text(
                    statusText,
                    style: theme.textTheme.titleMedium?.copyWith(
                      fontWeight: FontWeight.w700,
                    ),
                  ),
                ),
              ],
            ),
            const SizedBox(height: 16),
            Wrap(
              spacing: 10,
              runSpacing: 10,
              children: [
                _MetricChip(
                  icon: Icons.send,
                  label: 'Sent',
                  value: samplesSent.toString(),
                ),
                _MetricChip(
                  icon: Icons.speed,
                  label: 'Rate',
                  value: '${sendRateHz.toStringAsFixed(0)} Hz',
                ),
                _MetricChip(
                  icon: Icons.timer,
                  label: 'Time',
                  value: isStreaming ? _formatDuration(elapsed) : '00:00',
                ),
                _MetricChip(
                  icon: Icons.schedule,
                  label: 'Last',
                  value: lastPacketAt == null
                      ? '--'
                      : _formatClock(lastPacketAt!),
                ),
              ],
            ),
          ],
        ),
      ),
    );
  }
}

class _EndpointCard extends StatelessWidget {
  const _EndpointCard({
    required this.ipController,
    required this.portController,
    required this.isStreaming,
    required this.onToggleStreaming,
  });

  final TextEditingController ipController;
  final TextEditingController portController;
  final bool isStreaming;
  final VoidCallback onToggleStreaming;

  @override
  Widget build(BuildContext context) {
    return Card(
      elevation: 0,
      color: Colors.white,
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: LayoutBuilder(
          builder: (context, constraints) {
            final fields = [
              Expanded(
                flex: 3,
                child: TextField(
                  controller: ipController,
                  decoration: const InputDecoration(
                    labelText: 'PC IP Address',
                    prefixIcon: Icon(Icons.computer),
                    border: OutlineInputBorder(),
                  ),
                  keyboardType: TextInputType.text,
                  enabled: !isStreaming,
                ),
              ),
              Expanded(
                flex: 2,
                child: TextField(
                  controller: portController,
                  decoration: const InputDecoration(
                    labelText: 'UDP Port',
                    prefixIcon: Icon(Icons.settings_ethernet),
                    border: OutlineInputBorder(),
                  ),
                  keyboardType: TextInputType.number,
                  enabled: !isStreaming,
                ),
              ),
            ];

            final button = FilledButton.icon(
              onPressed: onToggleStreaming,
              icon: Icon(isStreaming ? Icons.stop : Icons.play_arrow),
              label: Text(isStreaming ? 'Stop' : 'Start'),
              style: FilledButton.styleFrom(
                minimumSize: const Size(132, 56),
                shape: RoundedRectangleBorder(
                  borderRadius: BorderRadius.circular(8),
                ),
              ),
            );

            if (constraints.maxWidth >= 680) {
              return Row(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  fields[0],
                  const SizedBox(width: 12),
                  fields[1],
                  const SizedBox(width: 12),
                  button,
                ],
              );
            }

            return Column(
              crossAxisAlignment: CrossAxisAlignment.stretch,
              children: [
                TextField(
                  controller: ipController,
                  decoration: const InputDecoration(
                    labelText: 'PC IP Address',
                    prefixIcon: Icon(Icons.computer),
                    border: OutlineInputBorder(),
                  ),
                  keyboardType: TextInputType.text,
                  enabled: !isStreaming,
                ),
                const SizedBox(height: 12),
                TextField(
                  controller: portController,
                  decoration: const InputDecoration(
                    labelText: 'UDP Port',
                    prefixIcon: Icon(Icons.settings_ethernet),
                    border: OutlineInputBorder(),
                  ),
                  keyboardType: TextInputType.number,
                  enabled: !isStreaming,
                ),
                const SizedBox(height: 12),
                button,
              ],
            );
          },
        ),
      ),
    );
  }
}

class _HttpServerCard extends StatelessWidget {
  const _HttpServerCard({
    required this.portController,
    required this.isServing,
    required this.url,
    required this.samplesServed,
    required this.activeClients,
    required this.lastRequestAt,
    required this.onToggleServing,
  });

  final TextEditingController portController;
  final bool isServing;
  final String? url;
  final int samplesServed;
  final int activeClients;
  final DateTime? lastRequestAt;
  final VoidCallback onToggleServing;

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    final colorScheme = theme.colorScheme;
    final displayUrl = url ?? 'http://0.0.0.0:${portController.text.trim()}';

    return Card(
      elevation: 0,
      color: Colors.white,
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: [
            Row(
              children: [
                Icon(Icons.http, color: colorScheme.primary),
                const SizedBox(width: 10),
                Expanded(
                  child: Text(
                    'Local HTTP Access',
                    style: theme.textTheme.titleMedium?.copyWith(
                      fontWeight: FontWeight.w700,
                    ),
                  ),
                ),
                _StatusPill(isStreaming: isServing),
              ],
            ),
            const SizedBox(height: 14),
            LayoutBuilder(
              builder: (context, constraints) {
                final portField = TextField(
                  controller: portController,
                  decoration: const InputDecoration(
                    labelText: 'HTTP Port',
                    prefixIcon: Icon(Icons.settings_ethernet),
                    border: OutlineInputBorder(),
                  ),
                  keyboardType: TextInputType.number,
                  enabled: !isServing,
                );

                final button = FilledButton.icon(
                  onPressed: onToggleServing,
                  icon: Icon(isServing ? Icons.stop : Icons.play_arrow),
                  label: Text(isServing ? 'Stop HTTP' : 'Start HTTP'),
                  style: FilledButton.styleFrom(
                    minimumSize: const Size(148, 56),
                    shape: RoundedRectangleBorder(
                      borderRadius: BorderRadius.circular(8),
                    ),
                  ),
                );

                if (constraints.maxWidth >= 620) {
                  return Row(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      SizedBox(width: 220, child: portField),
                      const SizedBox(width: 12),
                      Expanded(child: _EndpointUrl(url: displayUrl)),
                      const SizedBox(width: 12),
                      button,
                    ],
                  );
                }

                return Column(
                  crossAxisAlignment: CrossAxisAlignment.stretch,
                  children: [
                    portField,
                    const SizedBox(height: 12),
                    _EndpointUrl(url: displayUrl),
                    const SizedBox(height: 12),
                    button,
                  ],
                );
              },
            ),
            const SizedBox(height: 14),
            Wrap(
              spacing: 10,
              runSpacing: 10,
              children: [
                _MetricChip(
                  icon: Icons.article,
                  label: 'Served',
                  value: samplesServed.toString(),
                ),
                _MetricChip(
                  icon: Icons.hub,
                  label: 'Clients',
                  value: activeClients.toString(),
                ),
                _MetricChip(
                  icon: Icons.schedule,
                  label: 'Last',
                  value: lastRequestAt == null
                      ? '--'
                      : _formatClock(lastRequestAt!),
                ),
              ],
            ),
          ],
        ),
      ),
    );
  }
}

class _EndpointUrl extends StatelessWidget {
  const _EndpointUrl({required this.url});

  final String url;

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    final colorScheme = theme.colorScheme;

    return Container(
      constraints: const BoxConstraints(minHeight: 56),
      padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 8),
      decoration: BoxDecoration(
        color: colorScheme.surfaceContainerHighest,
        borderRadius: BorderRadius.circular(8),
        border: Border.all(color: colorScheme.outlineVariant),
      ),
      alignment: Alignment.centerLeft,
      child: SelectableText(
        '$url/sample\n$url/stream',
        style: theme.textTheme.bodyMedium?.copyWith(
          fontFeatures: const [FontFeature.tabularFigures()],
          height: 1.25,
        ),
      ),
    );
  }
}

class _SensorCard extends StatelessWidget {
  const _SensorCard({
    required this.title,
    required this.icon,
    required this.unit,
    required this.x,
    required this.y,
    required this.z,
    required this.magnitude,
    required this.history,
    required this.color,
    required this.scale,
  });

  final String title;
  final IconData icon;
  final String unit;
  final double x;
  final double y;
  final double z;
  final double magnitude;
  final List<double> history;
  final Color color;
  final double scale;

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);

    return Card(
      elevation: 0,
      color: Colors.white,
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: [
            Row(
              children: [
                Icon(icon, color: color),
                const SizedBox(width: 10),
                Expanded(
                  child: Text(
                    title,
                    style: theme.textTheme.titleMedium?.copyWith(
                      fontWeight: FontWeight.w700,
                    ),
                  ),
                ),
                Text(
                  magnitude.toStringAsFixed(3),
                  style: theme.textTheme.titleMedium?.copyWith(
                    color: color,
                    fontWeight: FontWeight.w800,
                  ),
                ),
              ],
            ),
            const SizedBox(height: 14),
            _AxisRow(
              label: 'X',
              value: x,
              unit: unit,
              color: color,
              scale: scale,
            ),
            _AxisRow(
              label: 'Y',
              value: y,
              unit: unit,
              color: color,
              scale: scale,
            ),
            _AxisRow(
              label: 'Z',
              value: z,
              unit: unit,
              color: color,
              scale: scale,
            ),
            const SizedBox(height: 14),
            SizedBox(
              height: 56,
              child: CustomPaint(
                painter: _SparklinePainter(values: history, color: color),
                child: const SizedBox.expand(),
              ),
            ),
          ],
        ),
      ),
    );
  }
}

class _AxisRow extends StatelessWidget {
  const _AxisRow({
    required this.label,
    required this.value,
    required this.unit,
    required this.color,
    required this.scale,
  });

  final String label;
  final double value;
  final String unit;
  final Color color;
  final double scale;

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    final progress = (value.abs() / scale).clamp(0.0, 1.0).toDouble();

    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 6),
      child: Row(
        children: [
          SizedBox(
            width: 24,
            child: Text(
              label,
              style: theme.textTheme.labelLarge?.copyWith(
                color: color,
                fontWeight: FontWeight.w800,
              ),
            ),
          ),
          Expanded(
            child: ClipRRect(
              borderRadius: BorderRadius.circular(6),
              child: LinearProgressIndicator(
                value: progress,
                minHeight: 10,
                backgroundColor: color.withValues(alpha: 0.12),
                color: value >= 0 ? color : Theme.of(context).colorScheme.error,
              ),
            ),
          ),
          const SizedBox(width: 12),
          SizedBox(
            width: 112,
            child: Text(
              '${value.toStringAsFixed(3)} $unit',
              textAlign: TextAlign.right,
              style: theme.textTheme.bodyMedium?.copyWith(
                fontFeatures: const [FontFeature.tabularFigures()],
              ),
            ),
          ),
        ],
      ),
    );
  }
}

class _MetricChip extends StatelessWidget {
  const _MetricChip({
    required this.icon,
    required this.label,
    required this.value,
  });

  final IconData icon;
  final String label;
  final String value;

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    final colorScheme = theme.colorScheme;

    return Container(
      height: 42,
      padding: const EdgeInsets.symmetric(horizontal: 12),
      decoration: BoxDecoration(
        color: colorScheme.surfaceContainerHighest,
        borderRadius: BorderRadius.circular(8),
      ),
      child: Row(
        mainAxisSize: MainAxisSize.min,
        children: [
          Icon(icon, size: 18, color: colorScheme.primary),
          const SizedBox(width: 8),
          Text(
            '$label: ',
            style: theme.textTheme.labelLarge?.copyWith(
              color: colorScheme.onSurfaceVariant,
            ),
          ),
          Text(
            value,
            style: theme.textTheme.labelLarge?.copyWith(
              fontWeight: FontWeight.w800,
              fontFeatures: const [FontFeature.tabularFigures()],
            ),
          ),
        ],
      ),
    );
  }
}

class _StatusPill extends StatelessWidget {
  const _StatusPill({required this.isStreaming});

  final bool isStreaming;

  @override
  Widget build(BuildContext context) {
    final colorScheme = Theme.of(context).colorScheme;
    final color = isStreaming ? Colors.green.shade700 : colorScheme.outline;

    return Container(
      height: 32,
      padding: const EdgeInsets.symmetric(horizontal: 10),
      decoration: BoxDecoration(
        border: Border.all(color: color.withValues(alpha: 0.45)),
        borderRadius: BorderRadius.circular(8),
      ),
      child: Row(
        mainAxisSize: MainAxisSize.min,
        children: [
          Icon(
            isStreaming ? Icons.circle : Icons.circle_outlined,
            size: 12,
            color: color,
          ),
          const SizedBox(width: 8),
          Text(isStreaming ? 'Streaming' : 'Idle'),
        ],
      ),
    );
  }
}

class _SparklinePainter extends CustomPainter {
  const _SparklinePainter({required this.values, required this.color});

  final List<double> values;
  final Color color;

  @override
  void paint(Canvas canvas, Size size) {
    final gridPaint = Paint()
      ..color = Colors.black.withValues(alpha: 0.06)
      ..strokeWidth = 1;
    final baseline = size.height - 1;
    canvas.drawLine(
      Offset(0, baseline),
      Offset(size.width, baseline),
      gridPaint,
    );

    if (values.length < 2) return;

    final minValue = values.reduce(math.min);
    final maxValue = values.reduce(math.max);
    final range = math.max(maxValue - minValue, 0.001);
    final step = size.width / (values.length - 1);
    final path = Path();

    for (var i = 0; i < values.length; i++) {
      final x = step * i;
      final y = size.height - ((values[i] - minValue) / range * size.height);
      if (i == 0) {
        path.moveTo(x, y);
      } else {
        path.lineTo(x, y);
      }
    }

    final fillPath = Path.from(path)
      ..lineTo(size.width, size.height)
      ..lineTo(0, size.height)
      ..close();

    canvas.drawPath(fillPath, Paint()..color = color.withValues(alpha: 0.10));
    canvas.drawPath(
      path,
      Paint()
        ..color = color
        ..strokeWidth = 2
        ..style = PaintingStyle.stroke
        ..strokeCap = StrokeCap.round,
    );
  }

  @override
  bool shouldRepaint(covariant _SparklinePainter oldDelegate) => true;
}

String _formatDuration(Duration duration) {
  final minutes = duration.inMinutes.remainder(60).toString().padLeft(2, '0');
  final seconds = duration.inSeconds.remainder(60).toString().padLeft(2, '0');
  return '$minutes:$seconds';
}

String _formatClock(DateTime time) {
  final hour = time.hour.toString().padLeft(2, '0');
  final minute = time.minute.toString().padLeft(2, '0');
  final second = time.second.toString().padLeft(2, '0');
  return '$hour:$minute:$second';
}
