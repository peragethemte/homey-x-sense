'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const Module = require('node:module');

// The Homey SDK only exists on the Homey runtime; stub just enough to load app.js and the device base
const originalLoad = Module._load;
Module._load = function load(request, ...rest) {
  if (request === 'homey') {
    class Stub { log() {} error() {} }
    return { App: Stub, Device: Stub };
  }
  return originalLoad.call(this, request, ...rest);
};
const XSenseApp = require('../app');
const XSenseDeviceBase = require('../lib/XSenseDeviceBase');
Module._load = originalLoad;

function makeApp(devices) {
  const app = new XSenseApp();
  app.log = () => {};
  app.error = () => {};
  app.mqttHealthy = new Map();
  app.mqttHealthChangedAt = new Map();
  app.homey = { drivers: { getDrivers: () => ({ d: { getDevices: () => devices } }) } };
  return app;
}

function makeDevice(houseId, app) {
  const device = Object.create(XSenseDeviceBase.prototype);
  const caps = new Map([['alarm_mqtt_connected', true]]);
  device.homey = { app };
  device.deviceData = { houseId };
  device.writes = [];
  device.hasCapability = (id) => caps.has(id);
  device.getCapabilityValue = (id) => caps.get(id);
  device.setCapabilityValue = async (id, value) => { caps.set(id, value); device.writes.push(value); };
  device.error = () => {};
  return device;
}

test('health change is pushed to every device of the house and no other house', async () => {
  const devices = [];
  const app = makeApp(devices);
  const a = makeDevice('house-1', app);
  const b = makeDevice('house-1', app);
  const other = makeDevice('house-2', app);
  devices.push(a, b, other);

  app.setMQTTHealth('house-1', false, 'test');
  await new Promise((resolve) => setImmediate(resolve));

  assert.deepEqual(a.writes, [false]);
  assert.deepEqual(b.writes, [false]);
  assert.deepEqual(other.writes, []);
});

test('unchanged health does not rewrite capabilities', async () => {
  const devices = [];
  const app = makeApp(devices);
  const a = makeDevice('house-1', app);
  devices.push(a);

  app.setMQTTHealth('house-1', true, 'first');
  app.setMQTTHealth('house-1', true, 'again');
  await new Promise((resolve) => setImmediate(resolve));

  // Capability already true, so neither call writes
  assert.deepEqual(a.writes, []);
});

test('sync skips devices without the capability or with unknown health', async () => {
  const app = makeApp([]);
  const device = makeDevice('house-1', app);
  device.hasCapability = () => false;
  app.mqttHealthy.set('house-1', false);
  await device.syncMQTTConnectedCapability();
  assert.deepEqual(device.writes, []);

  const unknown = makeDevice('house-9', app);
  await unknown.syncMQTTConnectedCapability();
  assert.deepEqual(unknown.writes, []);
});

test('subscription health notifications carry a diagnostic reason', () => {
  const XSenseAPI = require('../lib/XSenseAPI');
  const calls = [];
  const api = new XSenseAPI('test@example.test', 'secret', {
    app: { log() {}, error() {}, setMQTTHealth: (...args) => calls.push(args) },
  });
  const info = {
    house: { houseId: 'house-1' },
    subscriptions: new Set(['@xsense/events/safealarm/house-1']),
    desiredSubscriptions: new Set(['@xsense/events/safealarm/house-1', '$aws/things/x/shadow/name/mainpage/update']),
    pendingSubscriptions: new Set(),
  };

  api._updateMQTTSubscriptionHealth(info);

  assert.equal(calls[0][1], false);
  assert.match(calls[0][2], /safety 1\/2, safealarm=true/);
  api.destroy();
});

test('signature refresh interval defaults to 10 minutes and can be disabled via env', () => {
  const XSenseAPI = require('../lib/XSenseAPI');
  const make = () => new XSenseAPI('t@example.test', 's', { app: { log() {}, error() {} } });
  const defaulted = make();
  assert.equal(defaulted.signatureRefreshMs, 600000);
  defaulted.destroy();

  process.env.XSENSE_MQTT_SIGNATURE_REFRESH_MS = '0';
  const disabled = make();
  assert.equal(disabled.signatureRefreshMs, 0);
  disabled.destroy();
  delete process.env.XSENSE_MQTT_SIGNATURE_REFRESH_MS;
});
