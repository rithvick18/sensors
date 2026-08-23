import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math' as math;

import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:qr_flutter/qr_flutter.dart';
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

class DetectedIp {
  final String ip;
  final String interfaceName;
  final String label;
  final bool isWifi;
  final bool isCgnat;

  const DetectedIp({
    required this.ip,
    required this.interfaceName,
    required this.label,
    required this.isWifi,
    required this.isCgnat,
  });
}

class _SensorStreamerPageState extends State<SensorStreamerPage> {
  static const _sensorSamplingPeriod = Duration(milliseconds: 10);
  static const _udpSamplingPeriod = Duration(milliseconds: 10);
  static const _httpSamplingPeriod = Duration(milliseconds: 10);
  static const _displayRefreshPeriod = Duration(milliseconds: 100);
  static const _historyLength = 80;

  final TextEditingController _ipController = TextEditingController(
    text: '192.168.1.69',
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
  List<DetectedIp> _detectedIps = <DetectedIp>[];
  String? _selectedIp;
  bool _isRemoteControlConnected = false;

  HttpClient? _controlClient;
  StreamSubscription<String>? _controlSub;
  RawDatagramSocket? _commandSocket;
  Timer? _controlReconnectTimer;

  final List<double> _accelHistory = <double>[];
  final List<double> _gyroHistory = <double>[];

  @override
  void initState() {
    super.initState();
    _startSensorListeners();
    _startControlListener();
    _startUdpCommandListener();
    _ipController.addListener(_onIpChanged);
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

  void _onIpChanged() {
    _startControlListener();
  }

  @override
  void dispose() {
    _ipController.removeListener(_onIpChanged);
    _controlReconnectTimer?.cancel();
    _controlSub?.cancel();
    _controlClient?.close(force: true);
    _commandSocket?.close();
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

  void _startControlListener() {
    _controlSub?.cancel();
    _controlClient?.close(force: true);

    final ip = _ipController.text.trim();
    if (ip.isEmpty) return;

    final client = HttpClient()..connectionTimeout = const Duration(seconds: 4);
    _controlClient = client;

    client
        .getUrl(Uri.parse('http://$ip:3000/api/control/stream'))
        .then((request) => request.close())
        .then((response) {
      if (!mounted) return;
      setState(() => _isRemoteControlConnected = true);

      _controlSub = response
          .transform(utf8.decoder)
          .transform(const LineSplitter())
          .listen(
        (line) {
          final trimmed = line.trim();
          if (trimmed.isEmpty) return;
          try {
            final data = jsonDecode(trimmed) as Map<String, dynamic>;
            final action = (data['action'] as String?)?.toUpperCase();
            if (action == 'START') {
              if (!_isStreaming) {
                _startStreaming();
              }
            } else if (action == 'STOP') {
              if (_isStreaming) {
                _stopStreaming();
              }
            }
          } catch (_) {}
        },
        onError: (_) {
          if (mounted) setState(() => _isRemoteControlConnected = false);
          _scheduleControlReconnect();
        },
        onDone: () {
          if (mounted) setState(() => _isRemoteControlConnected = false);
          _scheduleControlReconnect();
        },
        cancelOnError: true,
      );
    }).catchError((_) {
      if (mounted) setState(() => _isRemoteControlConnected = false);
      _scheduleControlReconnect();
    });
  }

  void _scheduleControlReconnect() {
    _controlReconnectTimer?.cancel();
    if (!mounted) return;
    _controlReconnectTimer = Timer(const Duration(seconds: 3), () {
      if (mounted) _startControlListener();
    });
  }

  Future<void> _startUdpCommandListener() async {
    try {
      _commandSocket =
          await RawDatagramSocket.bind(InternetAddress.anyIPv4, 12346);
      _commandSocket?.listen((event) {
        if (event == RawSocketEvent.read) {
          final datagram = _commandSocket?.receive();
          if (datagram != null) {
            final message = utf8.decode(datagram.data).trim();
            try {
              final data = jsonDecode(message) as Map<String, dynamic>;
              final action = (data['action'] as String?)?.toUpperCase();
              if (action == 'START' && !_isStreaming) {
                _startStreaming();
              } else if (action == 'STOP' && _isStreaming) {
                _stopStreaming();
              }
            } catch (_) {}
          }
        }
      });
    } catch (_) {}
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

      final detected = await _resolveAllIpv4();
      final chosenIp = detected.isNotEmpty ? detected.first.ip : server.address.address;

      if (!mounted) return;
      setState(() {
        _isHttpServing = true;
        _httpSamplesServed = 0;
        _httpClients = 0;
        _lastHttpRequestAt = null;
        _detectedIps = detected;
        _selectedIp = chosenIp;
        _httpUrl = 'http://$chosenIp:$port';
      });
    } catch (error) {
      _showError('Failed to start HTTP server: $error');
      await _stopHttpServer();
    }
  }

  void _selectIp(String ip) {
    if (!mounted) return;
    final port = _httpPortController.text.trim();
    setState(() {
      _selectedIp = ip;
      _httpUrl = 'http://$ip:$port';
    });
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
      _detectedIps = <DetectedIp>[];
      _selectedIp = null;
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

  Future<List<DetectedIp>> _resolveAllIpv4() async {
    final list = <DetectedIp>[];
    try {
      final interfaces = await NetworkInterface.list(
        includeLinkLocal: false,
        type: InternetAddressType.IPv4,
      );
      for (final iface in interfaces) {
        final name = iface.name.toLowerCase();
        for (final address in iface.addresses) {
          if (address.isLoopback) continue;
          final ip = address.address;
          final isWifi = name.startsWith('en0') ||
              name.startsWith('wlan') ||
              name.contains('wifi') ||
              name == 'en';
          final isCgnat = _isCarrierCgnat(ip);
          String label;
          if (isWifi) {
            label = 'Wi-Fi ($name)';
          } else if (name.startsWith('pdp_ip')) {
            label = 'Cellular ($name)';
          } else if (name.startsWith('utun') || name.contains('tun')) {
            label = 'VPN ($name)';
          } else {
            label = iface.name;
          }
          list.add(
            DetectedIp(
              ip: ip,
              interfaceName: iface.name,
              label: label,
              isWifi: isWifi,
              isCgnat: isCgnat,
            ),
          );
        }
      }
    } catch (_) {}

    list.sort((a, b) {
      int score(DetectedIp item) {
        final isPriv = _isPrivateLan(item.ip);
        if (item.isWifi && isPriv && !item.isCgnat) return 100;
        if (item.isWifi && !item.isCgnat) return 80;
        if (isPriv && !item.isCgnat) return 60;
        if (item.label.contains('VPN')) return 40;
        if (item.isCgnat || item.label.contains('Cellular')) return 10;
        return 20;
      }

      return score(b).compareTo(score(a));
    });

    return list;
  }

  bool _isCarrierCgnat(String ip) {
    final parts = ip.split('.').map(int.tryParse).toList();
    if (parts.length == 4 &&
        parts[0] == 100 &&
        parts[1] != null &&
        parts[1]! >= 64 &&
        parts[1]! <= 127) {
      return true;
    }
    return false;
  }

  bool _isPrivateLan(String ip) {
    final parts = ip.split('.').map(int.tryParse).toList();
    if (parts.length != 4 || parts.any((p) => p == null)) return false;
    final a = parts[0]!, b = parts[1]!;
    if (a == 192 && b == 168) return true;
    if (a == 10 && !_isCarrierCgnat(ip)) return true;
    if (a == 172 && b >= 16 && b <= 31) return true;
    return false;
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
                    isRemoteControlConnected: _isRemoteControlConnected,
                    onToggleStreaming: _isStreaming
                        ? _stopStreaming
                        : () => _startStreaming(),
                  ),
                  const SizedBox(height: 12),
                  _HttpServerCard(
                    portController: _httpPortController,
                    isServing: _isHttpServing,
                    url: _httpUrl,
                    detectedIps: _detectedIps,
                    selectedIp: _selectedIp,
                    onSelectIp: _selectIp,
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
    required this.isRemoteControlConnected,
    required this.onToggleStreaming,
  });

  final TextEditingController ipController;
  final TextEditingController portController;
  final bool isStreaming;
  final bool isRemoteControlConnected;
  final VoidCallback onToggleStreaming;

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    final colorScheme = theme.colorScheme;

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
                Icon(Icons.hub, color: colorScheme.primary),
                const SizedBox(width: 8),
                Text(
                  'UDP Endpoint / Streaming',
                  style: theme.textTheme.titleMedium?.copyWith(
                    fontWeight: FontWeight.w700,
                  ),
                ),
                const Spacer(),
                Container(
                  padding:
                      const EdgeInsets.symmetric(horizontal: 8, vertical: 4),
                  decoration: BoxDecoration(
                    color: isRemoteControlConnected
                        ? Colors.green.shade50
                        : colorScheme.surfaceContainerHighest,
                    borderRadius: BorderRadius.circular(6),
                    border: Border.all(
                      color: isRemoteControlConnected
                          ? Colors.green.shade300
                          : colorScheme.outlineVariant,
                    ),
                  ),
                  child: Row(
                    mainAxisSize: MainAxisSize.min,
                    children: [
                      Icon(
                        isRemoteControlConnected
                            ? Icons.wifi_tethering
                            : Icons.wifi_tethering_off,
                        size: 13,
                        color: isRemoteControlConnected
                            ? Colors.green.shade800
                            : colorScheme.onSurfaceVariant,
                      ),
                      const SizedBox(width: 4),
                      Text(
                        isRemoteControlConnected
                            ? 'Web Sync Ready'
                            : 'Web Sync Idle',
                        style: TextStyle(
                          fontSize: 11,
                          fontWeight: FontWeight.w700,
                          color: isRemoteControlConnected
                              ? Colors.green.shade800
                              : colorScheme.onSurfaceVariant,
                        ),
                      ),
                    ],
                  ),
                ),
              ],
            ),
            const SizedBox(height: 14),
            LayoutBuilder(
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
                    backgroundColor:
                        isStreaming ? Colors.red.shade700 : null,
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
          ],
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
    required this.detectedIps,
    required this.selectedIp,
    required this.onSelectIp,
    required this.samplesServed,
    required this.activeClients,
    required this.lastRequestAt,
    required this.onToggleServing,
  });

  final TextEditingController portController;
  final bool isServing;
  final String? url;
  final List<DetectedIp> detectedIps;
  final String? selectedIp;
  final ValueChanged<String> onSelectIp;
  final int samplesServed;
  final int activeClients;
  final DateTime? lastRequestAt;
  final VoidCallback onToggleServing;

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    final colorScheme = theme.colorScheme;
    final displayUrl = url ?? 'http://0.0.0.0:${portController.text.trim()}';
    final isCellular = selectedIp != null &&
        (detectedIps.any((d) => d.ip == selectedIp && (d.isCgnat || !d.isWifi)));

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
                      Expanded(child: _EndpointUrl(url: displayUrl, isServing: isServing)),
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
                    _EndpointUrl(url: displayUrl, isServing: isServing),
                    const SizedBox(height: 12),
                    button,
                  ],
                );
              },
            ),
            if (isServing && detectedIps.length > 1) ...[
              const SizedBox(height: 12),
              Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text(
                    'Detected IP Addresses:',
                    style: theme.textTheme.labelMedium?.copyWith(
                      color: colorScheme.onSurfaceVariant,
                      fontWeight: FontWeight.w700,
                    ),
                  ),
                  const SizedBox(height: 6),
                  Wrap(
                    spacing: 8,
                    runSpacing: 6,
                    children: [
                      for (final d in detectedIps)
                        ChoiceChip(
                          avatar: Icon(
                            d.isWifi ? Icons.wifi : Icons.cell_tower,
                            size: 16,
                          ),
                          label: Text('${d.label}: ${d.ip}'),
                          selected: selectedIp == d.ip,
                          onSelected: (selected) {
                            if (selected) onSelectIp(d.ip);
                          },
                        ),
                    ],
                  ),
                ],
              ),
            ],
            if (isServing && isCellular) ...[
              const SizedBox(height: 12),
              Container(
                padding: const EdgeInsets.all(12),
                decoration: BoxDecoration(
                  color: Colors.amber.shade50,
                  borderRadius: BorderRadius.circular(8),
                  border: Border.all(color: Colors.amber.shade400),
                ),
                child: Row(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    Icon(Icons.info_outline, size: 20, color: Colors.amber.shade900),
                    const SizedBox(width: 10),
                    Expanded(
                      child: Text(
                        'Using cellular IP ($selectedIp). To connect with your computer dashboard, ensure your iPhone is connected to Wi-Fi on the same local network as your PC (e.g. 192.168.1.69).',
                        style: theme.textTheme.bodySmall?.copyWith(
                          color: Colors.amber.shade900,
                          fontWeight: FontWeight.w600,
                        ),
                      ),
                    ),
                  ],
                ),
              ),
            ],
            if (isServing && url != null) ...[
              const SizedBox(height: 16),
              Container(
                padding: const EdgeInsets.all(16),
                decoration: BoxDecoration(
                  color: colorScheme.surfaceContainerHighest.withValues(alpha: 0.45),
                  borderRadius: BorderRadius.circular(12),
                  border: Border.all(color: colorScheme.outlineVariant),
                ),
                child: Column(
                  children: [
                    Row(
                      mainAxisAlignment: MainAxisAlignment.center,
                      children: [
                        Icon(Icons.qr_code_2, size: 20, color: colorScheme.primary),
                        const SizedBox(width: 8),
                        Text(
                          'Scan to Connect Web Dashboard',
                          style: theme.textTheme.titleSmall?.copyWith(
                            fontWeight: FontWeight.w700,
                          ),
                        ),
                      ],
                    ),
                    const SizedBox(height: 12),
                    Container(
                      padding: const EdgeInsets.all(12),
                      decoration: BoxDecoration(
                        color: Colors.white,
                        borderRadius: BorderRadius.circular(12),
                        boxShadow: [
                          BoxShadow(
                            color: Colors.black.withValues(alpha: 0.06),
                            blurRadius: 8,
                            offset: const Offset(0, 2),
                          ),
                        ],
                      ),
                      child: QrImageView(
                        data: url!,
                        version: QrVersions.auto,
                        size: 160,
                        backgroundColor: Colors.white,
                        errorCorrectionLevel: QrErrorCorrectLevel.M,
                      ),
                    ),
                    const SizedBox(height: 8),
                    Text(
                      url!,
                      style: theme.textTheme.bodySmall?.copyWith(
                        color: colorScheme.onSurfaceVariant,
                        fontFeatures: const [FontFeature.tabularFigures()],
                        fontWeight: FontWeight.w600,
                      ),
                    ),
                  ],
                ),
              ),
            ],
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
  const _EndpointUrl({required this.url, required this.isServing});

  final String url;
  final bool isServing;

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
      child: Row(
        children: [
          Expanded(
            child: SelectableText(
              '$url/sample\n$url/stream',
              style: theme.textTheme.bodyMedium?.copyWith(
                fontFeatures: const [FontFeature.tabularFigures()],
                height: 1.25,
              ),
            ),
          ),
          if (isServing) ...[
            const SizedBox(width: 8),
            IconButton(
              tooltip: 'Copy URL',
              icon: const Icon(Icons.copy, size: 20),
              onPressed: () {
                Clipboard.setData(ClipboardData(text: url));
                ScaffoldMessenger.of(context).showSnackBar(
                  SnackBar(
                    content: Text('Copied $url to clipboard'),
                    duration: const Duration(seconds: 2),
                  ),
                );
              },
            ),
          ],
        ],
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
