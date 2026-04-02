import 'dart:async';
import 'dart:io';
import 'package:flutter/foundation.dart';
import 'package:flutter/services.dart';
import 'package:flutter_blue_plus/flutter_blue_plus.dart';

import '../models/di2_device.dart';
import 'di2_parser.dart';
import 'hold_detector.dart';
import 'log_service.dart';
import 'storage_service.dart';

enum BleStatus { off, idle, scanning, connecting, connected, disconnected }

/// Battery-efficient BLE manager for Shimano Di2 (RD-R8150 / EW-WU111).
///
/// Design principles
/// ─────────────────
/// • Scan ONCE to pair, then reconnect by saved device ID — no continuous scan.
/// • All button data arrives via BLE NOTIFY — zero CPU polling cost at rest.
/// • Automatic exponential back-off reconnect (2 s → 60 s max).
/// • "Balanced" connection priority: lower duty cycle = less heat + drain.
class BleService extends ChangeNotifier {
  final StorageService _storage;
  final void Function() onHoldDetected;

  BleService({required StorageService storage, required this.onHoldDetected})
      : _storage = storage;

  // ── Public state ───────────────────────────────────────────────────────────
  BleStatus status = BleStatus.idle;
  BluetoothDevice? connectedDevice;

  // ── Internals ──────────────────────────────────────────────────────────────
  HoldDetector? _holdDetector;
  StreamSubscription<BluetoothConnectionState>? _connStateSub;
  final List<StreamSubscription<List<int>>> _notifySubs = [];
  StreamSubscription<dynamic>? _wahooEventSub;

  static const _wahooEventChannel = EventChannel('com.bell/wahoo_events');

  bool _disposed = false;
  int _reconnectDelayMs = 2000;
  static const int _maxReconnectDelayMs = 60000;

  // ── Shimano BLE service / characteristic UUIDs ────────────────────────────
  // Legacy D-Fly (EW-WU111, SM-EWW01, older junction boxes)
  static final Guid _dFlyServiceUuid =
      Guid('a026ee01-0a7d-4ab3-97fa-f1500f9feb8b');
  // ignore: unused_field
  static final Guid _dFlyButtonCharUuid =
      Guid('a026e002-0a7d-4ab3-97fa-f1500f9feb8b');

  // Shimano 12-speed Di2 (RD-R8150 / RD-R9250) — confirmed from live device log.
  // Service UUID suffix 5348494d414e4f5f424c4500 = ASCII "SHIMANO_BLE\0".
  //
  // 18ef — D-Fly channel status service (button event indications)
  // 18ff — alternative / gear data service
  // 18fe — write-only service (not needed — no auth required)
  static final Guid _shimanoServiceUuid =
      Guid('000018ef-5348-494d-414e-4f5f424c4500');
  // ignore: unused_field
  static final Guid _shimanoAltServiceUuid =
      Guid('000018ff-5348-494d-414e-4f5f424c4500');

  /// D-Fly channel characteristic — sends INDICATIONS with per-channel press state.
  /// Packet: [headerByte, ch1, ch2, ch3, ...]
  ///   Bit 0x10 = short press, 0x20 = long press, 0x40 = double press, 0 = released.
  /// Ch.1 = left A-button (climbA), Ch.4 = right A-button (climbB).
  static final Guid _dFlyChannelCharUuid =
      Guid('00002ac2-5348-494d-414e-4f5f424c4500');

  // Scan service UUIDs for reference / future re-enabling of scan filter.
  // ignore: unused_element
  List<Guid> get _scanServiceUuids => [_dFlyServiceUuid, _shimanoServiceUuid];

  // ── D-Fly channel state (stateful — needed to detect VALUE changes) ────────
  List<int>? _lastDFlyChannels;
  // Channels that have already fired onHoldDetected for the current hold
  // gesture. Cleared when the channel byte returns to 0 (released).
  // Prevents repeated Di2 indications from re-triggering the call every 240 ms.
  final Set<int> _holdFiredChannels = {};

  // ── Public API ─────────────────────────────────────────────────────────────

  Future<void> start() async {
    _rebuildHoldDetector();
    final paired = _storage.pairedDevice;
    Log.i('BLE', 'start() — paired device: ${paired?.name ?? "none"}  climbA=${_storage.climbAEnabled}  climbB=${_storage.climbBEnabled}  holdMs=${_storage.holdDurationMs}');
    if (paired != null) await _connectById(paired.remoteId);
    _listenWahooEvents();
  }

  /// On Android: subscribe to Wahoo BT connect/disconnect events from
  /// MainActivity so we can auto-start the Di2 connection when the rider
  /// mounts their bike and the Wahoo head-unit pairs.
  void _listenWahooEvents() {
    if (!Platform.isAndroid) return;
    _wahooEventSub?.cancel();
    _wahooEventSub = _wahooEventChannel.receiveBroadcastStream().listen(
      (event) {
        Log.i('BLE', 'Wahoo event: $event');
        if (event == 'wahoo_connected') {
          final paired = _storage.pairedDevice;
          if (paired != null && status != BleStatus.connected && status != BleStatus.connecting) {
            Log.i('BLE', 'Wahoo connected → auto-connecting Di2');
            _connectById(paired.remoteId);
          }
        }
      },
      onError: (e) => Log.w('BLE', 'Wahoo event channel error: $e'),
    );
  }

  /// Returns a stream of scan results for the pairing screen.
  /// Filters by Shimano service UUIDs — catches both legacy D-Fly (EW-WU111)
  /// and 12-speed proprietary (RD-R8150 / RD-R9250).
  Stream<ScanResult> scan({int timeoutSec = 15}) {
    _setStatus(BleStatus.scanning);
    Log.i('BLE', 'Starting scan — no filter, showing all BLE devices');
    FlutterBluePlus.startScan(
      timeout: Duration(seconds: timeoutSec),
      androidScanMode: AndroidScanMode.lowPower,
    );
    return FlutterBluePlus.scanResults.expand((list) => list);
  }

  Future<void> stopScan() async {
    await FlutterBluePlus.stopScan();
    if (status == BleStatus.scanning) _setStatus(BleStatus.idle);
  }

  /// Persist [device] as the paired DI2 unit and connect to it.
  Future<void> pairAndConnect(BluetoothDevice device) async {
    await _storage.savePairedDevice(
      Di2Device(remoteId: device.remoteId.str, name: device.platformName),
    );
    await _connectById(device.remoteId.str);
  }

  /// Disconnect and forget the paired device.
  Future<void> unpair() async {
    _disposed = true;
    await _disconnect();
    await _storage.clearPairedDevice();
    _disposed = false;
    _setStatus(BleStatus.idle);
  }

  /// Call after the user changes the hold duration setting.
  void refreshHoldThreshold() => _rebuildHoldDetector();

  // ── Connection internals ───────────────────────────────────────────────────

  Future<void> _connectById(String remoteId) async {
    if (_disposed) return;
    Log.i('BLE', 'Connecting to $remoteId …');
    _setStatus(BleStatus.connecting);
    try {
      final device = BluetoothDevice.fromId(remoteId);
      await _attach(device);
    } catch (e) {
      Log.e('BLE', 'connect error: $e');
      _scheduleReconnect(remoteId);
    }
  }

  Future<void> _attach(BluetoothDevice device) async {
    await device.connect(autoConnect: false, mtu: null);

    connectedDevice = device;
    _reconnectDelayMs = 2000; // reset back-off on success
    Log.i('BLE', 'Connected to ${device.platformName} (${device.remoteId})');
    _setStatus(BleStatus.connected);

    // Balanced priority: ~100 ms BLE connection interval saves battery vs
    // "high" (7.5 ms) while still being fast enough for button detection.
    // Android only — iOS does not expose this API.
    if (Platform.isAndroid) {
      await device.requestConnectionPriority(
        connectionPriorityRequest: ConnectionPriority.balanced,
      );
    }

    _connStateSub?.cancel();
    _connStateSub = device.connectionState.listen((state) {
      if (state == BluetoothConnectionState.disconnected) {
        _handleDisconnect(device.remoteId.str);
      }
    });

    await _subscribeAll(device);
  }

  Future<void> _subscribeAll(BluetoothDevice device) async {
    for (final s in _notifySubs) { s.cancel(); }
    _notifySubs.clear();
    _lastDFlyChannels = null; // reset on each fresh connection
    _holdFiredChannels.clear();

    final services = await device.discoverServices();
    Log.i('BLE', 'Discovered ${services.length} services');

    // First pass: log ALL characteristics so the debug log captures the
    // full picture of services and properties for diagnostics.
    for (final svc in services) {
      final isShimanoSvc = svc.serviceUuid == _dFlyServiceUuid ||
          svc.serviceUuid == _shimanoServiceUuid ||
          svc.serviceUuid == _shimanoAltServiceUuid;
      Log.i('BLE', '  service ${svc.serviceUuid}${isShimanoSvc ? " ← Shimano ✓" : ""}');
      for (final char in svc.characteristics) {
        final p = char.properties;
        final flags = [
          if (p.read)         'R',
          if (p.write)        'W',
          if (p.writeWithoutResponse) 'WnR',
          if (p.notify)       'N',
          if (p.indicate)     'I',
        ].join('|');
        Log.i('BLE', '    char ${char.characteristicUuid}  [$flags]');
      }
    }

    // Second pass: subscribe to all notify AND indicate characteristics.
    // Di2 D-Fly channel char (2ac2) uses INDICATIONS — must include indicate here.
    for (final svc in services) {
      for (final char in svc.characteristics) {
        if (!char.properties.notify && !char.properties.indicate) continue;
        await char.setNotifyValue(true);

        final isDFlyChannelChar = char.characteristicUuid == _dFlyChannelCharUuid;
        // Legacy D-Fly button char (EW-WU111)
        final isLegacyDFlyChar = char.characteristicUuid == _dFlyButtonCharUuid;

        final sub = char.lastValueStream.listen((data) {
          if (data.isEmpty) return;
          Log.raw('BLE', 'notify/indicate ${char.characteristicUuid}', data);

          if (isDFlyChannelChar) {
            // ── 2ac2 D-Fly channel char (RD-R8150 / 12-speed Di2) ─────────
            // Packet: [headerByte, ch1, ch2, ch3, ...]
            // Bit 0x10 = short press, 0x20 = long press, 0x40 = double press.
            // Ch.1 (index 0) = left A-button (climbA), Ch.4 (index 3) = right.
            if (data.length < 2) return;
            final channels = data.sublist(1);

            if (_lastDFlyChannels == null) {
              // First packet: initialize state without triggering any buttons.
              _lastDFlyChannels = List.from(channels);
              Log.i('BLE', 'D-Fly init state: ${channels.map((b) => b.toRadixString(16).padLeft(2, "0")).join(" ")}');
              return;
            }

            for (int i = 0; i < channels.length; i++) {
              final prev = i < _lastDFlyChannels!.length ? _lastDFlyChannels![i] : channels[i];
              final curr = channels[i];
              if (curr == prev) continue;

              final chNum = i + 1; // 1-indexed D-Fly channel number
              final pressType = (curr & 0x20) != 0 ? 'LONG'
                              : (curr & 0x10) != 0 ? 'short'
                              : (curr & 0x40) != 0 ? 'double'
                              : 'released';
              Log.i('BLE', 'D-Fly Ch.$chNum $pressType (0x${curr.toRadixString(16).padLeft(2, "0")})');

              if ((curr & 0x20) != 0) {
                // Long press — fire ONCE per hold gesture (suppress repeats).
                if (!_holdFiredChannels.contains(chNum)) {
                  _holdFiredChannels.add(chNum);
                  final enabled =
                      (chNum == 3 && _storage.climbBEnabled);  // Ch.3 = right A
                  if (enabled) {
                    Log.i('BLE', 'Enabled long press on D-Fly Ch.$chNum → triggering hold callback');
                    onHoldDetected();
                  } else {
                    Log.i('BLE', 'Long press on D-Fly Ch.$chNum — button not enabled, ignoring');
                  }
                } else {
                  Log.i('BLE', 'D-Fly Ch.$chNum hold repeat suppressed');
                }
              } else {
                // Long-press bit cleared (released / short / double) —
                // clear the gate so the next hold gesture can fire again.
                _holdFiredChannels.remove(chNum);
              }
            }
            _lastDFlyChannels = List.from(channels);

          } else if (isLegacyDFlyChar) {
            // ── Legacy D-Fly path (EW-WU111 / older junction boxes) ────────
            final active = Di2Parser.isEnabledActive(
              data,
              climbA: _storage.climbAEnabled,
              climbB: _storage.climbBEnabled,
            );
            Log.i('BLE',
              'D-Fly legacy [${char.characteristicUuid}] '
              'comp=0x${data.length > 3 ? data[3].toRadixString(16).padLeft(2, "0") : "?"}  '
              'isButtonDown=${Di2Parser.isButtonDown(data)}  '
              'enabledActive=$active');
            if (active) {
              Log.i('BLE', 'Enabled button active → feeding HoldDetector');
              _holdDetector?.onPacket();
            }
          } else {
            // ── Other / background char — log only ────────────────────────
            Log.i('BLE', 'bg packet [${char.characteristicUuid}] ${data.length} bytes');
          }
        });
        _notifySubs.add(sub);
      }
    }
    Log.i('BLE', 'Listening on ${_notifySubs.length} notify/indicate characteristics');
  }

  void _handleDisconnect(String remoteId) {
    Log.w('BLE', 'Disconnected from $remoteId');
    connectedDevice = null;
    _lastDFlyChannels = null; // reset D-Fly state so next connect re-initializes
    _holdFiredChannels.clear();
    _setStatus(BleStatus.disconnected);
    for (final s in _notifySubs) { s.cancel(); }
    _notifySubs.clear();
    if (_storage.autoReconnect && !_disposed) _scheduleReconnect(remoteId);
  }

  void _scheduleReconnect(String remoteId) {
    if (_disposed) return;
    Log.i('BLE', 'Retry in ${_reconnectDelayMs}ms …');
    Future.delayed(Duration(milliseconds: _reconnectDelayMs), () {
      if (!_disposed) _connectById(remoteId);
    });
    _reconnectDelayMs =
        (_reconnectDelayMs * 2).clamp(2000, _maxReconnectDelayMs);
  }

  Future<void> _disconnect() async {
    _connStateSub?.cancel();
    for (final s in _notifySubs) { s.cancel(); }
    _notifySubs.clear();
    try {
      await connectedDevice?.disconnect();
    } catch (_) {}
    connectedDevice = null;
  }

  void _rebuildHoldDetector() {
    _holdDetector?.dispose();
    _holdDetector = HoldDetector(
      onHold: onHoldDetected,
      holdThresholdMs: _storage.holdDurationMs,
    );
  }

  void _setStatus(BleStatus s) {
    status = s;
    notifyListeners();
  }

  @override
  void dispose() {
    _disposed = true;
    _holdDetector?.dispose();
    _connStateSub?.cancel();
    _wahooEventSub?.cancel();
    for (final s in _notifySubs) { s.cancel(); }
    super.dispose();
  }
}
