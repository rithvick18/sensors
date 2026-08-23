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
  static const _displayRefreshPeriod = Duration(milliseconds: 100);
  static const _historyLength = 80;

  final TextEditingController _ipController = TextEditingController(
    text: '192.168.1.69',
  );
  final TextEditingController _portController = TextEditingController(
    text: '12345',
  );

  RawDatagramSocket? _socket;
  StreamSubscription<AccelerometerEvent>? _accelSub;
  StreamSubscription<GyroscopeEvent>? _gyroSub;
  Timer? _sendTimer;
  Timer? _displayTimer;
  Timer? _rateTimer;

  bool _isStreaming = false;
  double _accelX = 0;
  double _accelY = 0;
  double _accelZ = 0;
  double _gyroX = 0;
  double _gyroY = 0;
  double _gyroZ = 0;
  int _samplesSent = 0;
  int _samplesAtLastRateCheck = 0;
  double _sendRateHz = 0;
  DateTime? _streamStartedAt;
  DateTime? _lastPacketAt;
  DateTime? _lastSensorAt;
  String? _sensorError;
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
    _displayTimer?.cancel();
    _rateTimer?.cancel();
    _accelSub?.cancel();
    _gyroSub?.cancel();
    _ipController.dispose();
    _portController.dispose();
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

  void _showInstructionsDialog(BuildContext context) {
    showDialog(
      context: context,
      builder: (context) => const _InstructionsDialog(),
    );
  }

  @override
  Widget build(BuildContext context) {
    final colorScheme = Theme.of(context).colorScheme;

    return Scaffold(
      appBar: AppBar(
        title: const Text('Sensor LSL Streamer'),
        elevation: 0,
        scrolledUnderElevation: 0,
        backgroundColor: Colors.white,
        surfaceTintColor: Colors.transparent,
        actions: [
          IconButton(
            icon: const Icon(Icons.help_outline_rounded),
            tooltip: 'Instructions',
            onPressed: () => _showInstructionsDialog(context),
          ),
          Padding(
            padding: const EdgeInsets.only(right: 16, left: 4),
            child: _StatusPill(isStreaming: _isStreaming),
          ),
        ],
      ),
      body: SafeArea(
        child: SingleChildScrollView(
          physics: const AlwaysScrollableScrollPhysics(
            parent: BouncingScrollPhysics(),
          ),
          padding: const EdgeInsets.all(16),
          child: Align(
            alignment: Alignment.topCenter,
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
                Icon(Icons.hub_outlined, color: colorScheme.primary, size: 22),
                const SizedBox(width: 8),
                Expanded(
                  child: Text(
                    'UDP Streaming',
                    style: theme.textTheme.titleMedium?.copyWith(
                      fontWeight: FontWeight.w700,
                    ),
                  ),
                ),
                const SizedBox(width: 8),
                Container(
                  padding:
                      const EdgeInsets.symmetric(horizontal: 10, vertical: 5),
                  decoration: BoxDecoration(
                    color: isRemoteControlConnected
                        ? Colors.green.shade50
                        : colorScheme.surfaceContainerHighest,
                    borderRadius: BorderRadius.circular(20),
                    border: Border.all(
                      color: isRemoteControlConnected
                          ? Colors.green.shade400
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
                        size: 14,
                        color: isRemoteControlConnected
                            ? Colors.green.shade800
                            : colorScheme.onSurfaceVariant,
                      ),
                      const SizedBox(width: 5),
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

class _InstructionsDialog extends StatelessWidget {
  const _InstructionsDialog();

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    final colorScheme = theme.colorScheme;

    return Dialog(
      shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(16)),
      backgroundColor: Colors.white,
      insetPadding: const EdgeInsets.symmetric(horizontal: 16, vertical: 24),
      child: ConstrainedBox(
        constraints: const BoxConstraints(maxWidth: 640, maxHeight: 720),
        child: Column(
          mainAxisSize: MainAxisSize.min,
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: [
            // Header
            Padding(
              padding: const EdgeInsets.fromLTRB(20, 18, 12, 12),
              child: Row(
                children: [
                  Container(
                    padding: const EdgeInsets.all(8),
                    decoration: BoxDecoration(
                      color: colorScheme.primary.withValues(alpha: 0.1),
                      borderRadius: BorderRadius.circular(8),
                    ),
                    child: Icon(
                      Icons.menu_book_rounded,
                      color: colorScheme.primary,
                      size: 22,
                    ),
                  ),
                  const SizedBox(width: 12),
                  Expanded(
                    child: Column(
                      crossAxisAlignment: CrossAxisAlignment.start,
                      children: [
                        Text(
                          'How to Use This App',
                          style: theme.textTheme.titleMedium?.copyWith(
                            fontWeight: FontWeight.w800,
                          ),
                        ),
                        Text(
                          'Mobile IMU Streamer & Sensor Guide',
                          style: theme.textTheme.bodySmall?.copyWith(
                            color: colorScheme.onSurfaceVariant,
                          ),
                        ),
                      ],
                    ),
                  ),
                  IconButton(
                    icon: const Icon(Icons.close, size: 20),
                    onPressed: () => Navigator.of(context).pop(),
                  ),
                ],
              ),
            ),
            const Divider(height: 1),
            // Content
            Flexible(
              child: SingleChildScrollView(
                padding: const EdgeInsets.all(20),
                child: Column(
                  crossAxisAlignment: CrossAxisAlignment.stretch,
                  children: [
                    _InstructionSection(
                      title: '1. Streaming to Computer (UDP Mode)',
                      icon: Icons.wifi,
                      color: colorScheme.primary,
                      description:
                          'Streams high-speed 3-axis Accelerometer and Gyroscope data (~100 Hz) directly to your PC for LSL recording, Python scripts, or the Web Dashboard.',
                      steps: const [
                        'Ensure your phone and computer are on the same Wi-Fi network.',
                        'Run "python server.py" on your computer to launch the server hub and web dashboard.',
                        'Enter your computer\'s IP Address (e.g. 192.168.1.69) and UDP Port (12345) in the fields above.',
                        'Tap "Start" to begin transmitting data datagrams.',
                        'Open http://localhost:3000 on your PC browser to view real-time charts and 3D orientation.',
                      ],
                    ),
                    const SizedBox(height: 16),
                    _InstructionSection(
                      title: '2. Web Sync & Remote Control',
                      icon: Icons.sync,
                      color: Colors.indigo.shade700,
                      description:
                          'When your PC server hub is running, the phone automatically synchronizes via SSE (Server-Sent Events).',
                      steps: const [
                        'When connected, the green "Web Sync Ready" badge appears at the top.',
                        'You can start and stop streaming directly from the PC Web Dashboard without touching your phone.',
                      ],
                    ),
                    const SizedBox(height: 16),
                    _InstructionSection(
                      title: '3. Sensor Units & Coordinate Frame',
                      icon: Icons.speed,
                      color: Colors.deepPurple.shade700,
                      description:
                          'Live IMU metrics streamed at ~100 Hz:',
                      steps: const [
                        'Accelerometer (m/s²): Measures acceleration including gravity (~9.81 m/s² on Z when flat on a table).',
                        'Gyroscope (rad/s): Measures angular rate of rotation around X, Y, and Z axes.',
                        'Sparklines at the bottom display real-time sensor waveform history.',
                      ],
                    ),
                    const SizedBox(height: 16),
                    _InstructionSection(
                      title: '4. Troubleshooting Tips',
                      icon: Icons.help_outline,
                      color: Colors.amber.shade900,
                      steps: const [
                        'If streaming fails to connect, verify both devices are on the same Wi-Fi subnet and VPNs are disabled.',
                        'If using cellular data, switch to a shared Wi-Fi network or phone hotspot.',
                        'Ensure your PC firewall permits incoming UDP traffic on port 12345 and TCP traffic on port 3000.',
                      ],
                    ),
                  ],
                ),
              ),
            ),
            const Divider(height: 1),
            // Footer
            Padding(
              padding: const EdgeInsets.symmetric(horizontal: 20, vertical: 12),
              child: Row(
                mainAxisAlignment: MainAxisAlignment.end,
                children: [
                  FilledButton(
                    onPressed: () => Navigator.of(context).pop(),
                    child: const Text('Got it'),
                  ),
                ],
              ),
            ),
          ],
        ),
      ),
    );
  }
}

class _InstructionSection extends StatelessWidget {
  const _InstructionSection({
    required this.title,
    required this.icon,
    required this.color,
    required this.steps,
    this.description,
  });

  final String title;
  final IconData icon;
  final Color color;
  final List<String> steps;
  final String? description;

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    final colorScheme = theme.colorScheme;

    return Container(
      padding: const EdgeInsets.all(14),
      decoration: BoxDecoration(
        color: colorScheme.surfaceContainerHighest.withValues(alpha: 0.35),
        borderRadius: BorderRadius.circular(10),
        border: Border.all(color: colorScheme.outlineVariant),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Row(
            children: [
              Icon(icon, color: color, size: 20),
              const SizedBox(width: 8),
              Expanded(
                child: Text(
                  title,
                  style: theme.textTheme.titleSmall?.copyWith(
                    fontWeight: FontWeight.w700,
                    color: color,
                  ),
                ),
              ),
            ],
          ),
          if (description != null) ...[
            const SizedBox(height: 6),
            Text(
              description!,
              style: theme.textTheme.bodySmall?.copyWith(
                color: theme.textTheme.bodySmall?.color?.withValues(alpha: 0.85),
                height: 1.35,
              ),
            ),
          ],
          const SizedBox(height: 8),
          for (var i = 0; i < steps.length; i++)
            Padding(
              padding: const EdgeInsets.only(bottom: 6),
              child: Row(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Container(
                    width: 18,
                    height: 18,
                    margin: const EdgeInsets.only(top: 2, right: 8),
                    decoration: BoxDecoration(
                      color: color.withValues(alpha: 0.12),
                      shape: BoxShape.circle,
                    ),
                    alignment: Alignment.center,
                    child: Text(
                      '${i + 1}',
                      style: TextStyle(
                        fontSize: 10,
                        fontWeight: FontWeight.w800,
                        color: color,
                      ),
                    ),
                  ),
                  Expanded(
                    child: Text(
                      steps[i],
                      style: theme.textTheme.bodySmall?.copyWith(
                        height: 1.35,
                      ),
                    ),
                  ),
                ],
              ),
            ),
        ],
      ),
    );
  }
}
