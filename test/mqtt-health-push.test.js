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
const XSenseAPI = require('../lib/XSenseAPI');
Module._load = originalLoad;

function makeApp(devices) {
  const app = new XSenseApp();
  app.log = () => {};
  app.error = () => {};
  app.mqttHealthy = new Map();
  app.mqttHealthChangedAt = new Map();
  app.mqttLinkDown = new Map();
  app.mqttLinkDownTimers = new Map();
  app.homey = {
    setTimeout: (fn, ms) => setTimeout(fn, ms),
    clearTimeout: (timer) => clearTimeout(timer),
    drivers: { getDrivers: () => ({ d: { getDevices: () => devices } }) },
  };
  return app;
}

function makeDevice(houseId, app, initial = false) {
  const device = Object.create(XSenseDeviceBase.prototype);
  const caps = new Map([['alarm_mqtt_disconnected', initial]]);
  device.homey = { app };
  device.deviceData = { houseId };
  device.writes = [];
  device.hasCapability = (id) => caps.has(id);
  device.getCapabilityValue = (id) => caps.get(id);
  device.setCapabilityValue = async (id, value) => { caps.set(id, value); device.writes.push(value); };
  device.error = () => {};
  return device;
}

const flush = () => new Promise((resolve) => setImmediate(resolve));

test('reconnect blips shorter than the grace period never reach device tiles', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const devices = [];
  const app = makeApp(devices);
  const device = makeDevice('house-1', app);
  devices.push(device);

  app.setMQTTHealth('house-1', true, 'connected');
  app.setMQTTHealth('house-1', false, 'reconnect');
  t.mock.timers.tick(59000);
  app.setMQTTHealth('house-1', true, 'connected');
  t.mock.timers.tick(5000);
  await flush();

  assert.deepEqual(device.writes, []);
  assert.equal(app.isMQTTLinkDown('house-1'), false);
});

test('outage longer than the grace period marks every device of the house, and only that house', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const devices = [];
  const app = makeApp(devices);
  const a = makeDevice('house-1', app);
  const b = makeDevice('house-1', app);
  const other = makeDevice('house-2', app);
  devices.push(a, b, other);

  app.setMQTTHealth('house-1', true, 'connected');
  app.setMQTTHealth('house-1', false, 'closed');
  t.mock.timers.tick(60000);
  await flush();
  assert.deepEqual(a.writes, [true]);
  assert.deepEqual(b.writes, [true]);
  assert.deepEqual(other.writes, []);

  app.setMQTTHealth('house-1', true, 'connected');
  await flush();
  assert.deepEqual(a.writes, [true, false]);
});

test('link state is unknown until the first connect, so capabilities are not added early', () => {
  const app = makeApp([]);
  assert.equal(app.isMQTTLinkDown('house-1'), undefined);

  const device = makeDevice('house-1', app);
  assert.equal(device._getMQTTLinkDown(), undefined);
});

test('sync skips devices without the capability', async () => {
  const app = makeApp([]);
  app.mqttLinkDown.set('house-1', true);
  const device = makeDevice('house-1', app);
  device.hasCapability = () => false;
  await device.syncMQTTLinkCapability();
  assert.deepEqual(device.writes, []);
});

test('subscription health notifications carry a diagnostic reason', () => {
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

test('forced signature refresh is off by default and can be enabled via env', () => {
  const make = () => new XSenseAPI('t@example.test', 's', { app: { log() {}, error() {} } });
  const defaulted = make();
  assert.equal(defaulted.signatureRefreshMs, 0);
  defaulted.destroy();

  process.env.XSENSE_MQTT_SIGNATURE_REFRESH_MS = '600000';
  const enabled = make();
  assert.equal(enabled.signatureRefreshMs, 600000);
  enabled.destroy();
  delete process.env.XSENSE_MQTT_SIGNATURE_REFRESH_MS;
});
