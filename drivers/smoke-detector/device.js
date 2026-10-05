'use strict';

const XSenseDeviceBase = require('../../lib/XSenseDeviceBase');
const DeviceModelRegistry = require('../../lib/DeviceModelRegistry');

class SmokeDetectorDevice extends XSenseDeviceBase {
  /**
   * onInit is called when device is initialized.
   */
  async onInit() {
    await super.onInit();

    this.log('SmokeDetectorDevice has been initialized');
    this._prevSmokeStatus = null;

    // Force add Capability if missing (for existing devices)
    if (!this.hasCapability('measure_signal_strength')) {
      await this.addCapability('measure_signal_strength').catch(this.error);
    }

    try {
      await this._startCloudUpdates({ pollIntervalMs: 300000 });
    } catch (error) {
      this.error('Error initializing device:', error);
      this.setUnavailable(this.homey.__('error.initialization_failed'));
    }

    // Register capability listeners
    this._registerCapabilityListeners();

    // Initialize capabilities to ensure fresh timestamps appear in Homey UI.
    // Always force-set so that Homey records the current time as last-updated,
    // preventing the "56 years ago" display when the capability was never touched.
    const capDefaults = {
      alarm_smoke: false,
      alarm_co: false,
      measure_smoke_status: 'OK',
    };
    for (const [cap, defaultVal] of Object.entries(capDefaults)) {
      if (this.hasCapability(cap)) {
        const current = this.getCapabilityValue(cap);
        // Only force-set if still null (first add); existing values are preserved to avoid overwriting real state
        if (current === null || current === undefined) {
          this.setCapabilityValue(cap, defaultVal).catch(this.error);
        }
      }
    }

  }

  /**
   * Handle device data update (Smoke Detector specific implementation)
   * Overrides base class method to add smoke/CO alarm logic
   * @param {Object} deviceData - Device data from API
   */
  async _handleDeviceUpdate(deviceData) {
    // Call base implementation first for common capabilities
    await super._handleDeviceUpdate(deviceData);

    try {
      // Parse status if available
      const status = deviceData.status || {};
      const normalizedEvent = deviceData.normalizedEvent;

      // Older app versions could pair SWS children with the smoke driver.
      // Preserve safety until the user can re-pair them with the water driver.
      if (DeviceModelRegistry.entityType(deviceData) === 'water') {
        if (!this.hasCapability('alarm_water')) await this.addCapability('alarm_water').catch(this.error);
        if (this.hasCapability('alarm_water')) {
          let waterAlarm;
          if (normalizedEvent === 'water_alarm') waterAlarm = true;
          else if (normalizedEvent === 'alarm_clear') waterAlarm = false;
          if (waterAlarm !== undefined) {
            await this.setCapabilityValue('alarm_water', waterAlarm).catch(e => this.error('Water alarm migration update failed:', e));
          }
        }
      }

      // Smoke alarm (status.a = alarm state: 0=OK, 1=Alarm)
      // Also check alarmStatus from real-time event messages (which use a flat structure
      // with alarmStatus instead of the shadow's status.a field). Without this check,
      // real smoke alarms received via the @xsense/events topic never trigger alarm_smoke.
      if (this.hasCapability('alarm_smoke')) {
        let smokeAlarm;
        if (normalizedEvent === 'smoke_alarm') smokeAlarm = true;
        else if (normalizedEvent === 'alarm_clear') smokeAlarm = false;
        else if (normalizedEvent !== 'co_alarm' && normalizedEvent !== 'self_test' && normalizedEvent !== 'mute' && status.a !== undefined) {
          smokeAlarm = status.a === '1' || status.a === 1;
        } else if (deviceData.smokeAlarm !== undefined) {
          smokeAlarm = this._normalizeBool(deviceData.smokeAlarm);
        }

        if (smokeAlarm !== undefined) {
          const previous = this.getCapabilityValue('alarm_smoke');
          await this.setCapabilityValue('alarm_smoke', smokeAlarm).catch(e => this.error('Smoke alarm update failed:', e));
          if (smokeAlarm && !previous) {
            await this.homey.flow.getDeviceTriggerCard('smoke_detected')
              .trigger(this, { device: this.getName() })
              .catch(e => this.error('Failed to trigger smoke_detected:', e));
          }
        }
      }

      // CO alarm (if device supports it)
      if (this.hasCapability('alarm_co')) {
        let coAlarm;
        if (normalizedEvent === 'co_alarm') coAlarm = true;
        else if (normalizedEvent === 'alarm_clear') coAlarm = false;
        else if (status.coAlarm !== undefined) coAlarm = this._normalizeBool(status.coAlarm);
        else if (deviceData.coAlarm !== undefined) coAlarm = this._normalizeBool(deviceData.coAlarm);
        if (coAlarm !== undefined) {
          await this.setCapabilityValue('alarm_co', coAlarm).catch(e => this.error('CO alarm update failed:', e));
        }
      }

      // CO value (measure_co)
      if (this.hasCapability('measure_co')) {
        const rawCo = this._getFirstValue(deviceData, ['coPpm', 'coLevel', 'coValue', 'co'], status);
        const coVal = this._normalizeNumber(rawCo);
        if (coVal !== undefined) {
          await this.setCapabilityValue('measure_co', coVal).catch(e => this.error('CO value update failed:', e));
        }
      }

      // Smoke status text
      if (this.hasCapability('measure_smoke_status')) {
        let statusText = this.getCapabilityValue('measure_smoke_status') || 'OK';
        if (normalizedEvent === 'alarm_clear') {
          statusText = 'OK';
        } else if (normalizedEvent === 'smoke_alarm' || normalizedEvent === 'co_alarm') {
          statusText = 'ALARM';
        } else if (normalizedEvent === 'self_test' || status.a === '2' || status.a === 2) {
          statusText = 'TEST';
        } else if (normalizedEvent === 'mute' || status.a === '3' || status.a === 3) {
          statusText = 'MUTED';
        } else if (status.a === '1' || status.a === 1) {
          statusText = 'ALARM';
        } else if (status.a === '0' || status.a === 0) {
          statusText = 'OK';
        }
        await this.setCapabilityValue('measure_smoke_status', statusText).catch(e => this.error('Smoke status update failed:', e));

        // Fire smoke_test_detected trigger on transition INTO test mode
        if (statusText === 'TEST' && this._prevSmokeStatus !== 'TEST') {
          this.homey.flow.getDeviceTriggerCard('smoke_test_detected')
            .trigger(this, { device: this.getName() })
            .catch(e => this.error('Failed to trigger smoke_test_detected:', e));
        }
        this._prevSmokeStatus = statusText;
        // A button test is a one-shot event, nothing reports its end
        if (statusText === 'TEST') this._scheduleTestStatusReset();
      }

      // Temperature (some smoke detectors have temperature sensors)
      const rawTemperature = this._getFirstValue(deviceData, ['temperature', 'temp'], status);
      if (this.hasCapability('measure_temperature') && rawTemperature !== undefined) {
        const temp = parseFloat(rawTemperature);
        if (!isNaN(temp)) {
          await this.setCapabilityValue('measure_temperature', temp).catch(e => this.error('Temperature update failed:', e));
        }
      }

      const rawHumidity = this._getFirstValue(deviceData, ['humidity', 'humi'], status);
      if (this.hasCapability('measure_humidity') && rawHumidity !== undefined) {
        const humidity = parseFloat(rawHumidity);
        if (!isNaN(humidity)) {
          await this.setCapabilityValue('measure_humidity', humidity).catch(e => this.error('Humidity update failed:', e));
        }
      }

    } catch (error) {
      this.error('Error handling smoke detector device update:', error);
    }
  }

  /**
   * Mute the alarm
   */
  async muteAlarm() {
    if (!this.api) {
      throw new Error('API client not initialized');
    }

    try {
      await this.api.muteAlarm(this.deviceData.id, this.deviceData.deviceSn);
      this.log('Alarm muted successfully');
      return true;
    } catch (error) {
      this.error('Failed to mute alarm:', error);
      throw error;
    }
  }

  /**
   * Wait for a positive test acknowledgement from MQTT/event updates.
   */
  _waitForTestAck(timeoutMs = 20000) {
    if (!this.api) {
      throw new Error('API client not initialized');
    }

    let cancelWait;
    const promise = new Promise((resolve, reject) => {
      let settled = false;
      let timeoutHandle = null;

      const finish = (err) => {
        if (settled) return;
        settled = true;
        if (timeoutHandle) {
          clearTimeout(timeoutHandle);
        }
        this.api.removeUpdateCallback(onUpdate);
        if (err) {
          reject(err);
        } else {
          resolve(true);
        }
      };
      cancelWait = () => finish(new Error('Test acknowledgement wait cancelled'));

      const onUpdate = (type, data) => {
        if (type !== 'device' || !data) {
          return;
        }

        const sameId = data.id === this.deviceData.id;
        const dataSn = data.deviceSn || data.deviceSN || data.sn;
        const sameSn = dataSn && (dataSn === this.deviceData.deviceSn || dataSn === this.deviceData.id);
        if (!sameId && !sameSn) {
          return;
        }

        const status = data.status || {};
        const isTest =
          data.normalizedEvent === 'self_test' ||
          status.a === 2 ||
          status.a === '2' ||
          data.alarmStatus === 2 ||
          data.event === 'self_test' ||
          data.event === 'self_test_triggered';

        if (isTest) {
          finish();
        }
      };

      timeoutHandle = setTimeout(() => {
        finish(new Error(`No test acknowledgement within ${Math.round(timeoutMs / 1000)}s`));
      }, timeoutMs);

      this.api.onUpdate(onUpdate);
    });
    promise.cancel = cancelWait;
    return promise;
  }

  _scheduleTestStatusReset() {
    if (this._testResetTimer) this.homey.clearTimeout(this._testResetTimer);
    this._testResetTimer = this.homey.setTimeout(() => {
      this._testResetTimer = null;
      if (this._prevSmokeStatus !== 'TEST') return;
      this.setCapabilityValue('measure_smoke_status', 'OK').catch(this.error);
      this._prevSmokeStatus = 'OK';
    }, 30000);
  }

  /**
   * Test the alarm (triggers a test beep)
   */
  async testAlarm() {
    if (!this.api) {
      throw new Error('API client not initialized');
    }

    if (!DeviceModelRegistry.supportsRemoteTest(this.deviceData)) {
      throw new Error(`Remote self-test is not supported by ${DeviceModelRegistry.typeOf(this.deviceData) || 'this model'}`);
    }

    let acknowledgement;
    try {
      acknowledgement = this._waitForTestAck(20000);
      await this.api.testAlarm(this.deviceData.id);
      await acknowledgement;

      this.log('Test alarm acknowledged successfully');

      if (this.hasCapability('measure_smoke_status')) {
        const prev = this._prevSmokeStatus;
        if (prev !== 'TEST') {
          this.homey.flow.getDeviceTriggerCard('smoke_test_detected')
            .trigger(this, { device: this.getName() })
            .catch(e => this.error('Failed to trigger smoke_test_detected:', e));
        }
        this._prevSmokeStatus = 'TEST';
        this._scheduleTestStatusReset();
      }

      return true;
    } catch (error) {
      if (acknowledgement?.cancel) {
        acknowledgement.cancel();
        await acknowledgement.catch(() => {});
      }
      this.error('Failed to trigger test alarm:', error);
      if (this.hasCapability('measure_smoke_status')) {
        this.setCapabilityValue('measure_smoke_status', 'OK').catch(this.error);
        this._prevSmokeStatus = 'OK';
      }
      throw error;
    }
  }

  /**
   * Register capability listeners
   */
  _registerCapabilityListeners() {
    // Currently no controllable capabilities
    // Smoke detectors are mainly monitoring devices
  }
}

module.exports = SmokeDetectorDevice;
