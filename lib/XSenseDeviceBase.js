'use strict';

const { normalizeLatestSourceTimestamp } = require('./SourceTimestamp');
const LatestValueQueue = require('./LatestValueQueue');

const Homey = require('homey');

/**
 * Base class for all X-Sense devices with common functionality
 * - Device initialization
 * - API client setup
 * - Callback management (memory leak prevention)
 * - Debouncing for capability updates
 * - Common device update logic
 */
class XSenseDeviceBase extends Homey.Device {

  async onInit() {
    await super.onInit();

    // Debounce timers for capability updates
    this.updateDebounceTimers = new Map();
    this.pendingUpdates = new Map();
    this.updateCallback = null; // Callback reference for cleanup
    this.realtimeTemperatureUpdates = new LatestValueQueue((data) => {
      this._handleDeviceUpdate(data).catch(error => this.error('Realtime update failed:', error));
    });

    // Default debounce delays (ms)
    this.debounceDelays = {
      'measure_temperature': 1000,    // 1 second for temperature
      'measure_humidity': 1000,       // 1 second for humidity
      'measure_battery': 2000,        // 2 seconds for battery
      'measure_rssi': 2000,           // 2 seconds for WiFi signal
      'measure_last_seen': 500,       // 0.5 seconds for last seen
      'default': 500                  // 0.5 seconds for everything else
    };

    // Initialize common device properties
    this._initializeCommon();

    // Capability options are persisted per paired device. Homey only exposes
    // them after the complete subclass onInit lifecycle has finished.
    this.ready()
      .then(() => this._migrateLastReportCapabilityTitle())
      .catch(error => this.error('Failed to schedule capability title update:', error));

    // alarm_mqtt_connected showed a healthy link as an active alarm; replaced by alarm_mqtt_disconnected
    if (this.hasCapability('alarm_mqtt_connected')) {
      await this.removeCapability('alarm_mqtt_connected').catch(error => this.error('Failed to remove alarm_mqtt_connected:', error));
    }
  }

  async _migrateLastReportCapabilityTitle() {
    const capabilityId = 'measure_last_seen';
    if (!this.hasCapability(capabilityId)) return;
    if (this.getStoreValue('lastReportTitleVersion') === 1) return;

    const expectedTitle = {
      en: 'Last Device Report',
      de: 'Letzter Gerätebericht',
    };
    try {
      await this.setCapabilityOptions(capabilityId, {
        title: expectedTitle,
      });
      await this.setStoreValue('lastReportTitleVersion', 1);
    } catch (error) {
      this.error('Failed to update last device report title:', error);
    }
  }

  /**
   * Initialize common device properties
   */
  _initializeCommon() {
    // Get device data
    this.deviceData = this.getData();
    this.settings = this.getSettings();

    // Get credentials from store (async for encryption support)
    const store = this.getStore();
    this.email = store.email;
    this.password = store.password;

    // Older paired devices had station/house IDs only in store, so copy them over
    if (!this.deviceData.stationId && store.stationId) {
      this.deviceData.stationId = store.stationId;
    }
    if (!this.deviceData.houseId && store.houseId) {
      this.deviceData.houseId = store.houseId;
    }
    if (!this.deviceData.deviceSn && store.deviceSn) {
      this.deviceData.deviceSn = store.deviceSn;
    }
    if (!this.deviceData.stationSn && store.stationSn) {
      this.deviceData.stationSn = store.stationSn;
    }
    if (!this.deviceData.deviceType && store.deviceType) {
      this.deviceData.deviceType = store.deviceType;
    }
    // Last-resort compatibility for old pairings that only persisted an ID.
    if (!this.deviceData.deviceSn && this.deviceData.id) {
      this.deviceData.deviceSn = this.deviceData.id;
    }
  }

  /**
   * Setup API client
   */
  async _setupAPIClient() {
    try {
      this.api = await this.homey.app.getAPIClient(this.email, this.password);
    } catch (error) {
      this.error('Error setting up API client:', error);
      throw error;
    }
  }

  async _startCloudUpdates({ pollIntervalMs = 300000, initialSync = true } = {}) {
    await this._setupAPIClient();
    if (!this.updateCallback) this._registerUpdateCallback();

    if (this.deviceData.houseId || this.deviceData.stationId) {
      try {
        await this.api.connectMQTT(this.deviceData.houseId, this.deviceData.stationId);
      } catch (error) {
        this.error('MQTT initialization failed; control polling remains active:', error);
      }
    }

    if (initialSync) {
      try {
        const synced = await this.api.syncDevice(this.deviceData.id);
        if (synced) await this._handleDeviceUpdate(synced);
        else await this.updateDevice();
      } catch (error) {
        this.error('Initial sync failed; falling back to regular update:', error);
        await this.updateDevice();
      }
    }

    if (this.pollInterval) clearInterval(this.pollInterval);
    this.pollInterval = setInterval(() => {
      this.updateDevice().catch((error) => this.error('Control poll failed:', error));
    }, pollIntervalMs);
  }

  /**
   * Register update callback (FIXED: Prevents memory leaks)
   */
  _registerUpdateCallback() {
    if (!this.api) {
      this.error('API client not initialized');
      return;
    }

    this.updateCallback = (type, data) => {
      if (type === 'device') {
        const sameId = data.id === this.deviceData.id;
        const dataSn = data.deviceSn || data.deviceSN;
        const sameSn =
          dataSn &&
          (dataSn === this.deviceData.deviceSn || dataSn === this.deviceData.id);

        if (!sameId && !sameSn) {
          const deviceType = (this.deviceData.deviceType || this.deviceData.type || '').toUpperCase();
          const updateType = (data.deviceType || data.type || '').toUpperCase();
          const updateSn = dataSn || data.sn;
          const missingSn = !this.deviceData.deviceSn;

          if (
            missingSn &&
            deviceType.startsWith('STH') &&
            updateType.startsWith('STH') &&
            data.stationId &&
            this.deviceData.stationId &&
            data.stationId === this.deviceData.stationId
          ) {
            this.log(`Fallback match by station for temperature sensor type=${updateType || 'unknown'}`);
            if (updateSn) {
              this.deviceData.deviceSn = updateSn;
              this.setStoreValue('deviceSn', updateSn).catch(this.error);
            }
          } else {
            return;
          }
        }
        if (!this._loggedUpdateMatch && this.deviceData?.deviceType?.startsWith('STH')) {
          this._loggedUpdateMatch = true;
          this.log(`Matched temperature update type=${data.type || data.deviceType || 'unknown'}`);
        }
        const matchedType = (data.deviceType || data.type || this.deviceData.deviceType || '').toUpperCase();
        if (matchedType.startsWith('STH') && !data.normalizedEvent) {
          this.realtimeTemperatureUpdates.push(data);
        } else {
          this._handleDeviceUpdate(data).catch((error) => this.error('Realtime update failed:', error));
        }
      }
    };

    this.api.onUpdate(this.updateCallback);
  }

  /**
   * Update device from API (common implementation)
   */
  async updateDevice() {
    if (!this.api) {
      this.error('API client not initialized');
      return;
    }

    try {
      const devices = await this.api.getDevices(this.deviceData.stationId);

      // Match by ID preferred, fallback to SN
      const deviceData = devices.find(d =>
        d.id === this.deviceData.id ||
        d.deviceSn === this.deviceData.deviceSn
      );

      if (deviceData) {
        await this._handleDeviceUpdate(deviceData);
        // Only set available if we successfully updated
        if (!this.getAvailable()) {
          this.setAvailable();
        }
      } else {
        // Don't mark unavailable immediately to avoid flickering on partial API returns
        this.log('Device not found in API update, skipping update');
      }
    } catch (error) {
      this.error('Error updating device:', error);
      // this.setUnavailable(this.homey.__('error.update_failed')); // Optional: keep available but log error
    }
  }

  /**
   * Debounce capability updates to prevent UI flickering
   * @param {string} capabilityId - Capability to update
   * @param {*} value - New value
   * @param {number} delay - Optional delay override (ms)
   */
  _debounceCapabilityUpdate(capabilityId, value, delay = null) {
    // Get debounce delay
    const debounceDelay = delay || this.debounceDelays[capabilityId] || this.debounceDelays.default;

    // Store pending value
    this.pendingUpdates.set(capabilityId, value);

    // Clear existing timer
    if (this.updateDebounceTimers.has(capabilityId)) {
      clearTimeout(this.updateDebounceTimers.get(capabilityId));
    }

    // Set new timer
    const timer = setTimeout(async () => {
      const pendingValue = this.pendingUpdates.get(capabilityId);

      try {
        // Only update if value actually changed
        const currentValue = this.getCapabilityValue(capabilityId);
        if (currentValue !== pendingValue) {
          await this.setCapabilityValue(capabilityId, pendingValue);
          this.log(`[Debounced] Updated ${capabilityId} to ${pendingValue}`);
        }
      } catch (error) {
        this.error(`Failed to update ${capabilityId}:`, error);
      }

      // Cleanup
      this.updateDebounceTimers.delete(capabilityId);
      this.pendingUpdates.delete(capabilityId);
    }, debounceDelay);

    this.updateDebounceTimers.set(capabilityId, timer);
  }

  /**
   * Handle device data update (base implementation)
   * Override in subclasses for device-specific logic
   * @param {Object} deviceData - Device data from API
   */
  async _handleDeviceUpdate(deviceData) {
    if (!deviceData) {
      this.log('No device data provided for update');
      return;
    }

    try {
      const updateSn =
        deviceData.deviceSn ||
        deviceData.deviceSN ||
        deviceData.sn;
      if (updateSn && this.deviceData.deviceSn !== updateSn) {
        this.deviceData.deviceSn = updateSn;
        this.setStoreValue('deviceSn', updateSn).catch(this.error);
      }

      // Add optional capabilities if data is available
      await this._ensureOptionalCapabilities(deviceData);

      // Update common capabilities

      const batteryPercent = this._normalizeNumber(this._getFirstValue(deviceData, [
        'battery', 'batteryPercent', 'bat'
      ], deviceData.status));

      // Battery level (batInfo is 0-3 scale)
      if (this.hasCapability('measure_battery')) {
        if (deviceData.batInfo !== undefined) {
          const batteryLevel = Math.round((parseInt(deviceData.batInfo, 10) * 100) / 3);
          await this.setCapabilityValue('measure_battery', Math.min(100, Math.max(0, batteryLevel))).catch(e => this.error('Battery update failed:', e));
        } else if (batteryPercent !== undefined) {
          await this.setCapabilityValue('measure_battery', Math.min(100, Math.max(0, batteryPercent))).catch(e => this.error('Battery update failed:', e));
        }
      }

      // Battery alarm
      if (this.hasCapability('alarm_battery')) {
        let batteryLevel = null;
        if (deviceData.batInfo !== undefined) {
          batteryLevel = Math.round((parseInt(deviceData.batInfo, 10) * 100) / 3);
        } else if (batteryPercent !== undefined) {
          batteryLevel = batteryPercent;
        }
        if (batteryLevel !== null) {
          const lowBattery = batteryLevel < 20;
          await this.setCapabilityValue('alarm_battery', lowBattery).catch(e => this.error('Battery alarm update failed:', e));
        }
      }

      // Signal strength (rfLevel is 0-4 scale or direct dBm)
      if (this.hasCapability('measure_signal_strength')) {
        const signalVal = this._getFirstValue(deviceData, [
          'wifiRssi', 'wifiRSSI', 'signal', 'rssi', 'rfLevel', 'signalLevel'
        ], deviceData.status);
        const signalNum = this._normalizeNumber(signalVal);

        if (signalNum !== undefined) {
          let signalStrengthDbm = -100;

          if (signalNum < 0) {
            // Already in dBm
            signalStrengthDbm = signalNum;
          } else if (signalNum > 4) {
            // Likely RSSI as positive value (e.g. 65 => -65 dBm)
            signalStrengthDbm = -Math.abs(signalNum);
          } else {
            // Convert 0-4 scale to dBm
            const s = Math.round(signalNum);
            if (s >= 4) signalStrengthDbm = -55;
            else if (s === 3) signalStrengthDbm = -67;
            else if (s === 2) signalStrengthDbm = -79;
            else if (s === 1) signalStrengthDbm = -91;
            else if (s === 0) signalStrengthDbm = -100;
          }

          await this.setCapabilityValue('measure_signal_strength', signalStrengthDbm).catch(e => this.error('Signal strength update failed:', e));
        }
      }

      // Online status
      const onlineValue = this._getFirstValue(deviceData, ['online', 'onLine'], deviceData.status);
      const isOnline = this._normalizeBool(onlineValue);
      if (isOnline === false && this.getAvailable()) {
        await this.setUnavailable(this.homey.__('error.device_offline')).catch(this.error);
      } else if (isOnline === true && !this.getAvailable()) {
        await this.setAvailable().catch(this.error);
      }

      // Update last seen timestamp only when data is from the cloud/device
      // and normalize to ISO UTC to avoid timezone drift in UI rendering.
      if (this.hasCapability('measure_last_seen')) {
        const sourceTs = this._getSourceTimestamp(deviceData);
        if (sourceTs) {
          const current = this.getCapabilityValue('measure_last_seen');
          if (current !== sourceTs) {
            await this.setCapabilityValue('measure_last_seen', sourceTs).catch(e => {});
          }
        }
      }

      // Update optional diagnostics / parity capabilities
      await this._updateOptionalCapabilities(deviceData);

    } catch (error) {
      this.error('Error in base _handleDeviceUpdate:', error);
    }
  }

  /**
   * Wrapper for temp data sync request (used by TemperatureSensor)
   */
  async _requestTempDataSync() {
    if (!this.api) {
      this.error('API client not initialized');
      return;
    }

    try {
      // Station ID is needed, devices list is optional/all
      await this.api.requestTempDataSync(this.deviceData.stationId, [this.deviceData.deviceSn]);
    } catch (err) {
      this.error('Failed to request temp data sync:', err);
    }
  }

  /**
   * Update capability immediately (bypass debouncing)
   * Use for critical updates like alarms
   */
  async _immediateCapabilityUpdate(capabilityId, value) {
    // Cancel pending debounced update
    if (this.updateDebounceTimers.has(capabilityId)) {
      clearTimeout(this.updateDebounceTimers.get(capabilityId));
      this.updateDebounceTimers.delete(capabilityId);
      this.pendingUpdates.delete(capabilityId);
    }

    // Update immediately
    try {
      const currentValue = this.getCapabilityValue(capabilityId);
      if (currentValue !== value) {
        await this.setCapabilityValue(capabilityId, value);
        this.log(`[Immediate] Updated ${capabilityId} to ${value}`);
      }
    } catch (error) {
      this.error(`Failed to update ${capabilityId}:`, error);
    }
  }

  _getFirstValue(deviceData, keys, status = null) {
    for (const key of keys) {
      if (deviceData && deviceData[key] !== undefined && deviceData[key] !== null) {
        return deviceData[key];
      }
      if (status && status[key] !== undefined && status[key] !== null) {
        return status[key];
      }
    }
    return undefined;
  }

  _normalizeBool(value) {
    if (value === undefined || value === null) return undefined;
    if (typeof value === 'boolean') return value;
    if (typeof value === 'number') return value === 1;
    if (typeof value === 'string') {
      const v = value.trim().toLowerCase();
      if (v === '1' || v === 'true' || v === 'yes') return true;
      if (v === '0' || v === 'false' || v === 'no') return false;
    }
    return undefined;
  }

  _normalizeNumber(value) {
    if (value === undefined || value === null) return undefined;
    if (typeof value === 'number') return Number.isNaN(value) ? undefined : value;
    if (typeof value === 'string') {
      const parsed = parseFloat(value);
      return Number.isNaN(parsed) ? undefined : parsed;
    }
    return undefined;
  }

  _getSourceTimestamp(deviceData) {
    const status = deviceData?.status;
    const keys = [
      'lastSeen',
      'last_seen',
      'sourceTimestamp',
      'lastTempUpdate',
      'onlineTime',
      'onLineTime',
      'updateTime',
      'updatedAt',
      'timestamp',
      'ts',
      'time',
    ];
    const values = [];
    for (const key of keys) {
      if (deviceData?.[key] !== undefined && deviceData[key] !== null) {
        values.push(deviceData[key]);
      }
      if (status?.[key] !== undefined && status[key] !== null) {
        values.push(status[key]);
      }
    }

    return normalizeLatestSourceTimestamp(values);
  }

  _getMQTTLinkDown() {
    const app = this.homey && this.homey.app;
    const houseId = this.deviceData && this.deviceData.houseId;
    if (!app || typeof app.isMQTTLinkDown !== 'function' || !houseId) return undefined;
    return app.isMQTTLinkDown(houseId);
  }

  async _ensureOptionalCapabilities(deviceData) {
    const status = deviceData.status || {};
    const mqttLinkDown = this._getMQTTLinkDown();

    const candidates = [
      { cap: 'measure_wifi_rssi', value: this._getFirstValue(deviceData, ['wifiRssi', 'wifiRSSI'], status) },
      { cap: 'measure_rf_level', value: this._getFirstValue(deviceData, ['rfLevel'], status) },
      { cap: 'measure_wifi_ssid', value: this._getFirstValue(deviceData, ['wifiSsid', 'ssid', 'wifiSSID'], status) },
      { cap: 'measure_ip_address', value: this._getFirstValue(deviceData, ['ip', 'ipAddress'], status) },
      { cap: 'measure_sw_version', value: this._getFirstValue(deviceData, ['sw', 'swVersion', 'firmwareVersion'], status) },
      { cap: 'measure_wifi_sw', value: this._getFirstValue(deviceData, ['wifiSw', 'wifi_sw', 'wifiSW'], status) },
      { cap: 'measure_alarm_volume', value: this._getFirstValue(deviceData, ['alarmVol', 'alarm_volume'], status) },
      { cap: 'measure_voice_volume', value: this._getFirstValue(deviceData, ['voiceVol', 'voice_volume'], status) },
      { cap: 'alarm_life_end', value: this._getFirstValue(deviceData, ['isLifeEnd', 'lifeEnd'], status) },
      { cap: 'alarm_muted', value: this._getFirstValue(deviceData, ['muteStatus', 'mute'], status) },
      { cap: 'alarm_activate', value: this._getFirstValue(deviceData, ['activate'], status) },
      { cap: 'alarm_ac_break', value: this._getFirstValue(deviceData, ['acBreak'], status) },
      { cap: 'alarm_base_removed', value: this._getFirstValue(deviceData, ['baseRemove'], status) },
      { cap: 'alarm_mqtt_disconnected', value: mqttLinkDown },
    ];

    for (const { cap, value } of candidates) {
      if (value !== undefined && !this.hasCapability(cap)) {
        try {
          await this.addCapability(cap);
        } catch (err) {
          this.error(`Failed to add capability ${cap}:`, err);
        }
      }
    }
  }

  async _updateOptionalCapabilities(deviceData) {
    const status = deviceData.status || {};

    const wifiRssi = this._normalizeNumber(this._getFirstValue(deviceData, ['wifiRssi', 'wifiRSSI'], status));
    if (wifiRssi !== undefined && this.hasCapability('measure_wifi_rssi')) {
      await this.setCapabilityValue('measure_wifi_rssi', wifiRssi).catch(e => this.error('WiFi RSSI update failed:', e));
    }

    const rfLevel = this._normalizeNumber(this._getFirstValue(deviceData, ['rfLevel'], status));
    if (rfLevel !== undefined && this.hasCapability('measure_rf_level')) {
      await this.setCapabilityValue('measure_rf_level', rfLevel).catch(e => this.error('RF level update failed:', e));
    }

    const wifiSsid = this._getFirstValue(deviceData, ['wifiSsid', 'ssid', 'wifiSSID'], status);
    if (wifiSsid !== undefined && this.hasCapability('measure_wifi_ssid')) {
      await this.setCapabilityValue('measure_wifi_ssid', String(wifiSsid)).catch(e => this.error('WiFi SSID update failed:', e));
    }

    const ipAddress = this._getFirstValue(deviceData, ['ip', 'ipAddress'], status);
    if (ipAddress !== undefined && this.hasCapability('measure_ip_address')) {
      await this.setCapabilityValue('measure_ip_address', String(ipAddress)).catch(e => this.error('IP address update failed:', e));
    }

    const swVersion = this._getFirstValue(deviceData, ['sw', 'swVersion', 'firmwareVersion'], status);
    if (swVersion !== undefined && this.hasCapability('measure_sw_version')) {
      await this.setCapabilityValue('measure_sw_version', String(swVersion)).catch(e => this.error('Firmware version update failed:', e));
    }

    const wifiSw = this._getFirstValue(deviceData, ['wifiSw', 'wifi_sw', 'wifiSW'], status);
    if (wifiSw !== undefined && this.hasCapability('measure_wifi_sw')) {
      await this.setCapabilityValue('measure_wifi_sw', String(wifiSw)).catch(e => this.error('WiFi firmware update failed:', e));
    }

    const alarmVol = this._normalizeNumber(this._getFirstValue(deviceData, ['alarmVol', 'alarm_volume'], status));
    if (alarmVol !== undefined && this.hasCapability('measure_alarm_volume')) {
      await this.setCapabilityValue('measure_alarm_volume', alarmVol).catch(e => this.error('Alarm volume update failed:', e));
    }

    const voiceVol = this._normalizeNumber(this._getFirstValue(deviceData, ['voiceVol', 'voice_volume'], status));
    if (voiceVol !== undefined && this.hasCapability('measure_voice_volume')) {
      await this.setCapabilityValue('measure_voice_volume', voiceVol).catch(e => this.error('Voice volume update failed:', e));
    }

    const lifeEnd = this._normalizeBool(this._getFirstValue(deviceData, ['isLifeEnd', 'lifeEnd'], status));
    if (lifeEnd !== undefined && this.hasCapability('alarm_life_end')) {
      await this.setCapabilityValue('alarm_life_end', lifeEnd).catch(e => this.error('Life end update failed:', e));
    }

    const muted = this._normalizeBool(this._getFirstValue(deviceData, ['muteStatus', 'mute'], status));
    if (muted !== undefined && this.hasCapability('alarm_muted')) {
      const prev = this.getCapabilityValue('alarm_muted');
      await this.setCapabilityValue('alarm_muted', muted).catch(e => this.error('Muted update failed:', e));
      if (muted && !prev) {
        try {
          await this.homey.flow.getDeviceTriggerCard('device_muted').trigger(this, {
            device: this.getName()
          });
        } catch (err) {
          this.error('Failed to trigger device_muted flow:', err);
        }
      }
    }

    const activate = this._normalizeBool(this._getFirstValue(deviceData, ['activate'], status));
    if (activate !== undefined && this.hasCapability('alarm_activate')) {
      await this.setCapabilityValue('alarm_activate', activate).catch(e => this.error('Activate update failed:', e));
    }

    const acBreak = this._normalizeBool(this._getFirstValue(deviceData, ['acBreak'], status));
    if (acBreak !== undefined && this.hasCapability('alarm_ac_break')) {
      await this.setCapabilityValue('alarm_ac_break', acBreak).catch(e => this.error('AC status update failed:', e));
    }

    const baseRemoved = this._normalizeBool(this._getFirstValue(deviceData, ['baseRemove'], status));
    if (baseRemoved !== undefined && this.hasCapability('alarm_base_removed')) {
      await this.setCapabilityValue('alarm_base_removed', baseRemoved).catch(e => this.error('Base removal update failed:', e));
    }

    await this.syncMQTTLinkCapability();
  }

  // Also called by the app whenever the displayed MQTT link state for this house changes
  async syncMQTTLinkCapability() {
    const linkDown = this._getMQTTLinkDown();
    if (linkDown === undefined || !this.hasCapability('alarm_mqtt_disconnected')) return;
    if (this.getCapabilityValue('alarm_mqtt_disconnected') === linkDown) return;
    await this.setCapabilityValue('alarm_mqtt_disconnected', linkDown).catch(e => this.error('MQTT status update failed:', e));
  }

  /**
   * Cleanup debounce timers and callbacks on device deletion
   */
  async onDeleted() {
    this.log('Device is being deleted, cleaning up...');

    // Clear all pending timers
    for (const timer of this.updateDebounceTimers.values()) {
      clearTimeout(timer);
    }
    this.updateDebounceTimers.clear();
    this.pendingUpdates.clear();
    this.realtimeTemperatureUpdates?.cancel();

    // FIX: Remove callback from API to prevent memory leak
    if (this.updateCallback && this.api) {
      this.api.removeUpdateCallback(this.updateCallback);
      this.updateCallback = null;
    }

    // Clear polling interval
    if (this.pollInterval) {
      clearInterval(this.pollInterval);
      this.pollInterval = null;
    }

    this.log('Device cleanup completed');
  }

  /**
   * onUninit is called when the device is uninitialized (e.g., app restart)
   */
  async onUninit() {
    // Reuse onDeleted cleanup logic
    await this.onDeleted();
  }
}

module.exports = XSenseDeviceBase;
