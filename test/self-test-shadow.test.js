'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const XSenseAPI = require('../lib/XSenseAPI');

function makeApi() {
  const api = new XSenseAPI('test@example.test', 'secret', { app: { log() {}, error() {} } });
  const updates = [];
  api.onUpdate((type, data) => updates.push({ type, data }));
  return { api, updates };
}

// Shape captured from a real SBS50 station after a physical test button press (identifiers replaced)
const selfTestUpdate = {
  state: {
    reported: {
      AAAA1111: {
        stationSN: 'STATION1',
        deviceSN: 'AAAA1111',
        type: 'XS01-M',
        who: 'XS01-M',
        userId: '',
        selfTest: '0',
        time: '20261005110652',
        zoneName: '',
      },
    },
  },
  clientToken: '-1',
};

test('self-test shadow update emits a self_test event for the tested device', () => {
  const { api, updates } = makeApi();
  api.devicesBySn.set('AAAA1111', 'device-1');
  api.devices.set('device-1', { id: 'device-1', deviceSn: 'AAAA1111', status: { a: 0 } });

  api._handleMQTTMessage('$aws/things/SBS50STATION1/shadow/name/2nd_selftestup/update', Buffer.from(JSON.stringify(selfTestUpdate)));

  assert.equal(updates.length, 1);
  assert.equal(updates[0].data.id, 'device-1');
  assert.equal(updates[0].data.normalizedEvent, 'self_test');
  assert.equal(updates[0].data.selfTestResult, '0');
  assert.equal(updates[0].data.sourceTimestamp, '20261005110652');
  api.destroy();
});

test('self-test event is not stored, so later device merges cannot replay it', () => {
  const { api } = makeApi();
  api.devicesBySn.set('AAAA1111', 'device-1');
  api.devices.set('device-1', { id: 'device-1', deviceSn: 'AAAA1111' });

  api._handleSelfTestShadow(selfTestUpdate);

  assert.equal(api.devices.get('device-1').normalizedEvent, undefined);
  api.destroy();
});

test('self-test for an unknown device is ignored', () => {
  const { api, updates } = makeApi();
  api._handleSelfTestShadow(selfTestUpdate);
  assert.equal(updates.length, 0);
  api.destroy();
});

test('RF stations subscribe to the self-test shadow', () => {
  const { api } = makeApi();
  const topics = [];
  api._subscribeTopic = (_info, list) => topics.push(...list);
  api.stations.set('station-1', { stationId: 'station-1', stationSn: 'STATION1', stationType: 'SBS50', category: 'SBS50' });

  api._subscribeStationTopics({}, 'station-1');

  assert.ok(topics.some((t) => t.endsWith('/shadow/name/2nd_selftestup/update')), topics.join('\n'));
  api.destroy();
});
