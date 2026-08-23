import 'package:flutter_test/flutter_test.dart';

import 'package:sensors/main.dart';

void main() {
  testWidgets('shows sensor dashboard and streaming controls', (tester) async {
    await tester.pumpWidget(const MyApp());

    expect(find.text('Sensor LSL Streamer'), findsOneWidget);
    expect(find.byTooltip('Instructions'), findsOneWidget);
    expect(find.text('PC IP Address'), findsOneWidget);
    expect(find.text('UDP Port'), findsOneWidget);
    expect(find.text('Accelerometer'), findsOneWidget);
    expect(find.text('Gyroscope'), findsOneWidget);
    expect(find.text('Start'), findsOneWidget);
  });

  testWidgets('opens instructions dialog when clicking instructions button', (tester) async {
    await tester.pumpWidget(const MyApp());

    final instructionsButton = find.byTooltip('Instructions');
    expect(instructionsButton, findsOneWidget);

    await tester.tap(instructionsButton);
    await tester.pumpAndSettle();

    expect(find.text('How to Use This App'), findsOneWidget);
    expect(find.text('1. Streaming to Computer (UDP Mode)'), findsOneWidget);
    expect(find.text('2. Web Sync & Remote Control'), findsOneWidget);
    expect(find.text('3. Sensor Units & Coordinate Frame'), findsOneWidget);
    expect(find.text('4. Troubleshooting Tips'), findsOneWidget);

    final gotItButton = find.text('Got it');
    expect(gotItButton, findsOneWidget);

    await tester.tap(gotItButton);
    await tester.pumpAndSettle();

    expect(find.text('How to Use This App'), findsNothing);
  });
}


