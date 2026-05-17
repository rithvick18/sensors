import 'package:flutter_test/flutter_test.dart';

import 'package:sensors/main.dart';

void main() {
  testWidgets('shows sensor dashboard and streaming controls', (tester) async {
    await tester.pumpWidget(const MyApp());

    expect(find.text('Sensor LSL Streamer'), findsOneWidget);
    expect(find.text('PC IP Address'), findsOneWidget);
    expect(find.text('UDP Port'), findsOneWidget);
    expect(find.text('Local HTTP Access'), findsOneWidget);
    expect(find.text('HTTP Port'), findsOneWidget);
    expect(find.text('Accelerometer'), findsOneWidget);
    expect(find.text('Gyroscope'), findsOneWidget);
    expect(find.text('Start'), findsOneWidget);
    expect(find.text('Start HTTP'), findsOneWidget);
  });
}
