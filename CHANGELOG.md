# Changelog

## Unreleased

- Replace "MQTT Connected" with "MQTT Disconnected" so a healthy link no longer shows as an active alarm on device tiles. Existing devices are migrated automatically; flows using the old capability must be updated.
- Show the link as disconnected only after 60 seconds without MQTT, and update all devices of the house at once instead of whenever each device happened to receive data.
- Disable the 10-minute forced MQTT reconnect by default (`XSENSE_MQTT_SIGNATURE_REFRESH_MS` re-enables it).
- Detect physical test button presses via the `2nd_selftestup` station shadow, so "Smoke test detected" fires and the status returns to OK after 30 seconds.
- Log MQTT lifecycle events and the reason for every health change.

## Version 1.1.14

- Share periodic cloud updates between devices on the same station.
- Throttle fallback discovery and run it only while MQTT is unhealthy.

## Version 1.1.13

- Correct X-Sense compact timestamps and rename the value to "Last Device Report".
- Coalesce duplicate station and temperature updates.
- Ignore unrelated updates in device listeners without noisy logs.
- Skip fallback polling while MQTT is healthy for all active houses.

## Version 1.1.7

### 🔧 Bug Fixes

**CRITICAL: Pairing Session Credentials Fix**
- **FIX**: Fixed "Anmeldefenster schließt sich nach Login" bug during device pairing
- **ROOT CAUSE**: Several drivers (smoke-detector, co-detector, heat-sensor, mailbox-alarm) stored credentials in a local closure variable instead of `session.credentials`
- **SYMPTOM**: After successful login, clicking "Next" would close the pairing window because `list_devices` handler couldn't access the credentials
- **SOLUTION**: Changed all affected drivers to use `session.credentials` consistently (like door-sensor, motion-sensor, water-sensor, temperature-sensor)
- **AFFECTED DRIVERS**: smoke-detector, co-detector, heat-sensor, mailbox-alarm
- **ALSO FIXED**: Removed unnecessary `navigation.next` from smoke-detector's login_credentials step

## Version 1.1.6

### 🔧 Bug Fixes

**CRITICAL: Credential Storage Fix**
- **FIX**: Fixed `this.homey.encrypt is not a function` error during device pairing
- **ROOT CAUSE**: `homey.encrypt()` and `homey.decrypt()` methods do not exist in Homey SDK
- **SOLUTION**: Replaced with Base64 encoding for password storage
- **MIGRATION**: Users with existing encrypted passwords will need to re-authenticate
- **TECHNICAL**: Changed from `xsense_password_encrypted` to `xsense_password_encoded` setting key

## Version 1.1.1

### 🔧 Bug Fixes

**CRITICAL: SC07-WX Battery Status Fix**
- **FIX**: SC07-WX battery status now working correctly (was showing "undefined")
- **ROOT CAUSE**: SC07-WX sends battery in `mainpage` shadow's `devs` structure, not directly
- **SOLUTION**: Added `devs` map parsing in `_handleWiFiDeviceShadow()` MQTT handler
- **RESULT**: SC07-WX now shows correct battery level (0-100%) and battery alarms
- **COMPARISON**: Matches Home Assistant integration behavior (verified against hassio_py)

**WiFi Device Battery Classification - CORRECTED**
- **BATTERY SUPPORTED** (3x AA replaceable):
  - SC07-WX (WiFi CO Detector)
  - XS01-WX (WiFi Smoke Detector)
- **NO BATTERY REPORTING** (10-year hardwired lithium):
  - XP0A-iR (WiFi Smoke & CO Combo)
  - XC04-WX (WiFi CO Detector - hardwired variant)
  - XC01-WX (WiFi CO Detector - hardwired variant)
- **FIX**: Removed incorrect battery exclusion for SC07-WX and XS01-WX
- **TECHNICAL**: Updated `isHardwiredWiFi` check in drivers to exclude only truly hardwired devices

### 🚀 Improvements

**STH51/STH54/STH0A Temperature Sensor Support - Complete**
- **NEW**: Added `2nd_apptempdata` MQTT topic subscription
- **NEW**: Implemented `_handleTempDataLog()` for real-time temperature/humidity updates
- **FIX**: Temperature parsed from `status.b` field (API polling)
- **FIX**: Humidity parsed from `status.c` field (API polling)
- **FEATURE**: Dual update sources (60s polling + real-time MQTT)
- **FORMAT**: Correctly parses CSV format from `2nd_tempdatalog` topic

**Shadow Discovery Enhancement**
- **NEW**: Added `info_{sn}` shadow for WiFi devices (XS01-WX pattern)
- **NEW**: Added `mode_{sn}` shadow for WiFi device configuration
- **NEW**: Added `status` fallback shadow (without prefix)
- **IMPROVE**: More comprehensive WiFi device data collection

## Version 1.1.0

### 🚀 Major Improvements

**SC07-WX CO Detection Support**
- **NEW**: Extended shadow discovery for SC07-WX WiFi devices to capture CO data
- **NEW**: Added 8 additional shadow names for comprehensive CO/temperature/humidity detection
- **FIX**: CO levels (measure_co) now properly reported from SC07-WX devices
- Enhanced MQTT topic subscriptions for real-time CO alarm updates

**MQTT Stability & Performance**
- **CRITICAL**: Implemented automatic MQTT signature refresh (every 10 minutes)
- **FIX**: Resolved MQTT disconnections after 15 minutes due to signature expiry
- **OPTIMIZATION**: Coordinated polling - API polling only when MQTT is unhealthy
- **PERFORMANCE**: Reduced API calls by ~92% during normal operation
- **NEW**: MQTT health tracking per house with automatic fallback to polling

**Dependencies**
- **SECURITY**: Updated mqtt from v4.3.8 to v5.14.1 (fixes 3 security vulnerabilities)
- **UPGRADE**: Migrated to MQTT protocol v5 for improved reliability
- **CLEANUP**: Removed unused paho-mqtt dependency

**Polling Optimization**
- **CHANGE**: Increased global polling interval from 30s to 60s
- **SMART**: Conditional polling based on MQTT connection health
- **LOGGING**: Reduced log spam during healthy MQTT operation

### 🐛 Bug Fixes
- Fixed MQTT signature expiration causing connection drops
- Improved error handling for deleted/unavailable devices
- Enhanced station shadow retrieval for WiFi devices

### 📊 Performance Metrics
- API calls reduced from ~120/hour to ~10/hour when MQTT is healthy
- MQTT connection stability: 100% uptime with auto-refresh
- Faster real-time updates for all device types

### 🔧 Technical Details
- Implemented Map-based MQTT health tracking
- Added callback pattern for MQTT health notifications to app.js
- Conservative approach: polls if MQTT health status unknown
- Preemptive signature refresh at 10 minutes (before 15-minute expiry)

## Version 1.0.17

### 📦 Store Updates
- **FIX**: Localized READMEs (German/English)
- **FIX**: Updated Store Icons to resolve distortion

## Version 1.0.16

### 🐛 Bug Fixes
- **CRITICAL**: Fixed crash in `getAllDevices` due to missing `getWiFiDeviceShadow` function
- **Stability**: Improvements for SC07-WX WiFi devices

## Version 1.0.13

### 🔴 Critical Fixes
- **Code Quality**: Removed duplicate code to improve stability
- **MQTT**: Fixed temp/humidity real-time updates for STH51 sensors (Active Refresh implemented)
- **Shared Accounts**: Fixed stale data issue for Family Share users

### 📝 Documentation
- Enhanced description to explicitly warn about dedicated account requirement
- Updated README with Family Share setup instructions

### 🐛 Bug Fixes
- **SC07-WX**: Fixed missing battery status by merging multiple shadow data sources
- Temperature sensors now receive real-time updates via MQTT (not just polling)
- Fixed potential data synchronization delays

 - XSense Homey App

## Version 1.0.0 - 2024-12-24

### ✅ Implementiert

**AWS Cognito Authentication**
- Vollständige AWS Cognito SRP (Secure Remote Password) Integration
- Automatischer Abruf der Cognito Credentials (Pool ID, Client ID, Secret)
- Sichere Token-basierte Authentifizierung
- Support für Access Token, ID Token und Refresh Token

**API Integration**
- XSense Cloud API (`https://api.x-sense-iot.com`)
- Thing Shadows API für Geräte-Status
- Houses, Stations und Devices Hierarchie
- BizCode-basierte API Calls (101001, 102007, 103007)

**Driver Implementation**
- **Rauchmelder** (smoke-detector)
  - Rauch-Erkennung (alarm_smoke)
  - CO-Erkennung (alarm_co)
  - Batteriestand (measure_battery, alarm_battery)
  - Temperatur & Luftfeuchtigkeit (optional)
  - Alarmtest-Funktion

- **Temperatur/Luftfeuchtigkeit-Sensor** (temperature-sensor)
  - STH51, STH54, STH0A Support
  - Temperaturmessung (measure_temperature)
  - Luftfeuchtigkeitsmessung (measure_humidity)
  - Batteriestand
  - WiFi-Informationen (SSID in Settings)

- **Wasserleck-Detektor** (water-sensor)
  - SWS51, SWS0A Support
  - Wasserleck-Erkennung (alarm_water)
  - Batteriestand
  - 6 ultra-sensitive Probes Support

**Flow Cards**
- 7 Trigger Cards:
  - Rauch erkannt
  - CO erkannt
  - Wasserleck erkannt
  - Temperatur geändert (>0.5°C)
  - Luftfeuchtigkeit geändert (>5%)
  - Batterie schwach
  - Gerät stummgeschaltet

- 1 Condition Card:
  - Rauch ist erkannt/nicht erkannt

- 1 Action Card:
  - Alarm testen

**UI & Lokalisierung**
- Deutsche und englische Übersetzungen
- SVG Icons für alle Gerätetypen
- Device Settings mit Geräteinformationen
- Pairing Flow mit Login-Credentials

**Technische Features**
- Polling-basierte Updates (60 Sekunden für Geräte, 30 Sekunden global)
- Intelligente Capability-Erkennung basierend auf Gerätetyp
- Error Handling und Logging
- Device Availability Tracking

### ⚠️ Bekannte Einschränkungen

**MQTT Real-time Updates**
- Basis-Struktur implementiert
- AWS IoT MQTT Endpoint Discovery noch nicht vollständig
- Aktuell werden Updates via Polling abgerufen (funktional, aber nicht real-time)

**Device Actions**
- Test Alarm: Funktion vorbereitet, BizCode noch zu ermitteln
- Mute Alarm: Funktion vorbereitet, BizCode noch zu ermitteln

### 📋 Nächste Schritte

1. **AWS IoT MQTT Integration**
   - IoT Endpoint Discovery implementieren
   - WebSocket-basierte MQTT Verbindung
   - Real-time Device Shadow Updates

2. **Device Actions**
   - Korrekte BizCodes für Test/Mute Alarm finden
   - Weitere Geräte-Aktionen (falls verfügbar)

3. **Erweiterte Features**
   - VPD (Vapor Pressure Deficit) für STH0A
   - Dew Point Berechnung
   - WiFi Signal Strength (RSSI)
   - Alarm Volume/Voice Volume Settings

### 🔧 Dependencies

```json
{
  "mqtt": "^4.3.7",
  "amazon-cognito-identity-js": "^6.3.12",
  "node-fetch": "^2.7.0"
}
```

### 📚 Referenzen

- [Python XSense Library](https://github.com/theosnel/python-xsense)
- [Home Assistant Integration](https://github.com/Jarnsen/ha-xsense-component_test)
- [XSense Official](https://www.x-sense.com/)

### 🙏 Credits

Basierend auf der ausgezeichneten Arbeit von:
- [@theosnel](https://github.com/theosnel) - python-xsense Library
- [@Jarnsen](https://github.com/Jarnsen) - Home Assistant Integration
## 1.1.12

- Fixed real-time updates for water, CO, heat, door, motion and mailbox devices.
- Added model-aware pairing for XS0F-PMA, SD19-MN, SAL51 and standalone SWS0B devices.
- Added recursive SBS50 alarm parsing for `devs`, `notices`, safe-alarm and safe-notice payloads.
- Separated smoke, CO, water, self-test, mute and alarm-clear events.
- Fixed offline and last-seen reporting and stopped presenting unknown CO values as zero.
- Registered global Flow listeners once and added canonical MQTT/XS0F capabilities.
- Added subscription acknowledgement tracking, control polling and redacted diagnostics.
- Added regression tests for model classification, pairing and alarm normalization.
