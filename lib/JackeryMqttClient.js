'use strict';

const mqtt = require('mqtt');

/** Fixed Jackery MQTT protocol root topic (not user-configurable). */
const TOPIC_PREFIX = 'hb';

/** Matches Home Assistant `REQUEST_INTERVAL` in custom_components/jackery/sensor.py. */
const REQUEST_INTERVAL_MS = 10_000;

/** Delay before the first poll after connect (HA coordinator sleeps 2s at loop start). */
const POLL_START_DELAY_MS = 2000;

/** Gap between type 100 sub-device polls (HA uses asyncio.sleep(0.5)). */
const SUB_DEVICE_POLL_DELAY_MS = 500;

/** Sub-device types polled with type 100 (CT = 2, plug = 6). */
const SUB_DEVICE_DEV_TYPES = [2, 6];

/**
 * RegExp metacharacters in the protocol topic prefix would break the status/event matcher.
 *
 * @param {string} str
 * @returns {string}
 */
function escapeRegExp(str) {
  return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/**
 * Jackery publishes on `…/device/{sn}/(status|event)`; one pattern extracts SN and channel for routing.
 *
 * @param {string} topicPrefix - Protocol root (e.g. `hb`).
 * @returns {RegExp}
 */
function buildTopicPattern(topicPrefix) {
  return new RegExp(`^${escapeRegExp(topicPrefix)}/device/([^/]+)/(status|event)$`);
}

/** Field names used to detect state-bearing payloads after stripping the protocol envelope. */
const STATE_KEYS = [
  'batSoc', 'batInPw', 'batOutPw', 'cellTemp',
  'pvPw', 'inOngridPw', 'outOngridPw', 'swEpsInPw', 'swEpsOutPw',
  'batChgEgy', 'batDisChgEgy',
  'batOtAcEgy', 'acOtBatEgy', 'ongridOtBatEgy', 'batOtGridEgy', 'pvOtBatEgy', 'pvEgy',
  'pv1', 'pv2', 'pv3', 'pv4', 'pv1Egy', 'pv2Egy', 'pv3Egy', 'pv4Egy', 'pvOtOngridEgy',
  'socChgLimit', 'socDischgLimit', 'isAutoStandby', 'autoStandby', 'maxOutPw', 'swEps',
  'ethPort', 'eip', 'emac', 'batNum',
];

const ENVELOPE_KEYS = ['type', 'eventId', 'messageId', 'ts', 'token', 'body'];

/**
 * Strips Jackery envelopes and ignores message types drivers must not treat as unit state.
 *
 * @param {Object} raw - Parsed JSON root.
 * @returns {Object|null} State object, `{}` when empty, or `null` to skip (e.g. type 101).
 */
function extractStateFromPayload(raw) {
  if (!raw || typeof raw !== 'object') return {};

  if (raw.type === 101) return null;

  const body = raw.body;
  if (body != null && typeof body === 'object' && !Array.isArray(body)) {
    // Type 23 stats are per deviceSn; only merge system totals into the main unit.
    if (raw.type === 23) {
      const deviceSn = body.deviceSn;
      if (deviceSn != null && deviceSn !== 'system') return null;
    }
    return body;
  }

  const flat = { ...raw };
  for (const key of ENVELOPE_KEYS) {
    delete flat[key];
  }

  const hasState = STATE_KEYS.some((k) => flat[k] !== undefined && flat[k] !== null);
  return hasState ? flat : {};
}

/**
 * Canonical broker shape so `connect()` can detect config changes and skip redundant reconnects.
 *
 * @param {Object} config - Raw config: { host, port?, username?, password?, useTls? }.
 * @returns {Object} - { host, port, username, password, useTls, topicPrefix }.
 */
function normalizeBrokerConfig(config) {
  return {
    host: config.host || '',
    port: config.port != null ? config.port : 1883,
    username: config.username != null ? config.username : '',
    password: config.password != null ? config.password : '',
    useTls: !!config.useTls,
    topicPrefix: TOPIC_PREFIX,
  };
}

/**
 * Jackery poll and control commands share a fixed JSON envelope on the device `action` topic.
 *
 * @param {Object} opts
 * @param {number} opts.type - Protocol message type (e.g. 25 = status poll).
 * @param {string} opts.token - Device authentication token.
 * @param {*} [opts.body] - Optional body; null for type 25 status requests.
 * @param {number} [opts.eventId] - Protocol event id.
 * @returns {Object}
 */
function buildActionPayload({ type, token, body = null, eventId = 0 }) {
  return {
    type,
    eventId,
    messageId: Math.floor(1000 + Math.random() * 9000),
    ts: Math.floor(Date.now() / 1000),
    token,
    body,
  };
}

/**
 * Shared MQTT session for all Jackery devices on one broker.
 *
 * Polling runs per serial number so battery and inverter devices on the same unit share one
 * 10 s cadence and one inbound fan-out instead of competing clients.
 */
module.exports = class JackeryMqttClient {

  static TOPIC_PREFIX = TOPIC_PREFIX;
  static REQUEST_INTERVAL_MS = REQUEST_INTERVAL_MS;
  static POLL_START_DELAY_MS = POLL_START_DELAY_MS;
  static normalizeBrokerConfig = normalizeBrokerConfig;
  static buildActionPayload = buildActionPayload;

  /**
   * Holds the shared broker session; `app` is used to tear down MQTT when the last device leaves.
   *
   * @param {Homey} homey - Homey instance (for log/error).
   * @param {Object} [app] - App instance; when set, client asks app to destroy it when the last device is unregistered.
   */
  constructor(homey, app) {
    this.homey = homey;
    this._app = app || null;
    this._snEntries = new Map();
    this._client = null;
    this._config = null;
    this._topicPattern = null;
    this._discoveryCallback = null;
    /** @type {Promise<void>|null} */
    this._connectPromise = null;
  }

  /**
   * Pairing listens for status traffic before any device is registered; pass nothing to clear.
   *
   * @param {function(string, string, *): void} [cb] - `(sn, kind, body)`.
   */
  setDiscoveryCallback(cb) {
    this._discoveryCallback = cb ?? null;
  }

  /**
   * Connect to the broker. Replaces the session when settings change; concurrent inits await one promise.
   *
   * @param {Object} config - Connection config (host, port, username, password, useTls).
   * @returns {Promise<void>}
   */
  async connect(config) {
    const normalized = normalizeBrokerConfig(config);
    const sameBroker = (cfg) => cfg && normalized.host === cfg.host
      && normalized.port === cfg.port
      && normalized.useTls === cfg.useTls
      && normalized.username === cfg.username
      && normalized.password === cfg.password
      && normalized.topicPrefix === cfg.topicPrefix;

    if (this._client?.connected && sameBroker(this._config)) {
      return;
    }

    if (!this._connectPromise) {
      this._connectPromise = (async () => {
        await this.disconnect();
        this._config = normalized;
        this._topicPattern = buildTopicPattern(normalized.topicPrefix);
        this._client = this._createClient(this._config);
        await this._waitConnectAndSubscribe();
      })().finally(() => {
        this._connectPromise = null;
      });
    }

    await this._connectPromise;

    if (!sameBroker(this._config)) {
      return this.connect(config);
    }
  }

  /**
   * Broker settings while connected; pairing reuses them when the app client already exists.
   *
   * @returns {Object|null}
   */
  getConfig() {
    if (!this._client || !this._client.connected) return null;
    return { ...this._config };
  }

  /**
   * Jackery topics are `{prefix}/device/{sn}/{channel}` under the fixed protocol root.
   *
   * @param {string} sn - Device serial number.
   * @param {string} channel - e.g. `action`, `status`, `event`.
   * @returns {string}
   */
  topicFor(sn, channel) {
    return `${this._config.topicPrefix}/device/${sn}/${channel}`;
  }

  /**
   * Uses a unique client id so Homey sessions do not collide with other MQTT clients on the broker.
   *
   * @param {Object} config - Normalized broker config.
   * @returns {mqtt.MqttClient}
   * @private
   */
  _createClient(config) {
    const protocol = config.useTls ? 'mqtts' : 'mqtt';
    const url = `${protocol}://${config.host}:${config.port}`;
    const options = {
      clientId: `homey-jackery-${Date.now()}`,
      reconnectPeriod: 5000,
      connectTimeout: 10000,
    };
    if (config.username) options.username = config.username;
    if (config.password) options.password = config.password;
    if (config.useTls) options.rejectUnauthorized = false;
    return mqtt.connect(url, options);
  }

  /**
   * Subscribes only after TCP connect so retained status is not handled before listeners exist.
   *
   * @private
   */
  _waitConnectAndSubscribe() {
    const prefix = this._config.topicPrefix;
    const statusTopic = `${prefix}/device/+/status`;
    const eventTopic = `${prefix}/device/+/event`;

    return new Promise((resolve, reject) => {
      const onConnect = () => {
        this._client.off('error', onError);
        this.log('Connected to MQTT broker');
        this._client.subscribe([statusTopic, eventTopic], (err) => {
          if (err) {
            this.error('Subscribe error:', err);
            reject(err);
            return;
          }
          this._client.on('message', this._routeMessage.bind(this));
          this._client.on('close', this._onDisconnect.bind(this));
          this._client.on('offline', this._onDisconnect.bind(this));
          resolve();
        });
      };
      const onError = (err) => {
        this._client.off('connect', onConnect);
        reject(err);
      };
      this._client.once('connect', onConnect);
      this._client.once('error', onError);
    });
  }

  /**
   * Marks registered devices offline when the broker drops instead of leaving stale capabilities.
   *
   * @private
   */
  _onDisconnect() {
    for (const entry of this._snEntries.values()) {
      for (const handler of entry.handlers.values()) {
        try {
          handler.onBrokerDisconnect?.();
        } catch (err) {
          this.error('onBrokerDisconnect error:', err);
        }
      }
    }
  }

  /**
   * Tears down the broker session; discovery uses a dedicated client and calls this in `finally`.
   *
   * @returns {Promise<void>}
   */
  async disconnect() {
    if (!this._client) return;

    return new Promise((resolve) => {
      this._client.end(false, {}, () => {
        this._client = null;
        this._config = null;
        this._topicPattern = null;
        this.log('Disconnected from MQTT broker');
        resolve();
      });
    });
  }

  /**
   * Low-level publish wrapper so action helpers share the same connected-client guard.
   *
   * @param {string} topic - Full topic.
   * @param {string} payload - Payload string (e.g. JSON).
   * @returns {Promise<void>}
   */
  publish(topic, payload) {
    if (!this._client || !this._client.connected) {
      return Promise.reject(new Error('MQTT client not connected'));
    }
    return new Promise((resolve, reject) => {
      this._client.publish(topic, payload, { qos: 0 }, (err) => {
        err ? reject(err) : resolve();
      });
    });
  }

  /**
   * Serializes poll and control envelopes onto the Jackery `action` channel for one unit.
   *
   * @param {string} sn - Device serial number.
   * @param {Object} payload - Action object (type, token, ts, messageId, body, etc.).
   * @returns {Promise<void>}
   */
  publishAction(sn, payload) {
    if (!this._config) {
      return Promise.reject(new Error('MQTT client not configured'));
    }
    const topic = this.topicFor(sn, 'action');
    return this.publish(topic, JSON.stringify(payload));
  }

  /**
   * Request fresh device status (type 25). Batteries often stay silent until polled.
   *
   * @param {string} sn - Device serial number.
   * @param {string} token - Device authentication token.
   * @returns {Promise<void>}
   */
  publishStatusPoll(sn, token) {
    const payload = buildActionPayload({ type: 25, token, body: null });
    return this.publishAction(sn, payload);
  }

  /**
   * Type 100 poll for CT meters and plugs; HA issues the same requests between main-unit polls.
   *
   * @param {string} sn - Device serial number.
   * @param {string} token - Device authentication token.
   * @param {number} devType - Protocol devType (2 = CT, 6 = plug).
   * @returns {Promise<void>}
   */
  publishSubDevicePoll(sn, token, devType) {
    const payload = buildActionPayload({
      type: 100,
      token,
      body: { devType },
    });
    return this.publishAction(sn, payload);
  }

  /**
   * Full HA coordinator poll sequence (type 25, then type 100 per sub-device type).
   *
   * @param {string} sn - Device serial number.
   * @param {string} token - Device authentication token.
   * @returns {Promise<void>}
   */
  async publishPeriodicPolls(sn, token) {
    await this.publishStatusPoll(sn, token);
    for (let i = 0; i < SUB_DEVICE_DEV_TYPES.length; i++) {
      await this.publishSubDevicePoll(sn, token, SUB_DEVICE_DEV_TYPES[i]);
      if (i < SUB_DEVICE_DEV_TYPES.length - 1) {
        await this._delay(SUB_DEVICE_POLL_DELAY_MS);
      }
    }
  }

  /**
   * Pushes unit settings over MQTT (HA type 1, body cmd 5) when Homey settings or Flow change them.
   *
   * @param {string} sn - Device serial number.
   * @param {string} token - Device authentication token.
   * @param {Object} params - Fields such as socChgLimit, socDischgLimit, autoStandby.
   * @returns {Promise<void>}
   */
  publishMainDeviceControl(sn, token, params) {
    const payload = buildActionPayload({
      type: 1,
      token,
      eventId: 3,
      body: { cmd: 5, rc: 1, ...params },
    });
    return this.publishAction(sn, payload);
  }

  /**
   * Spaces sub-device polls on the Homey clock to match HA `asyncio.sleep(0.5)` between type 100 calls.
   *
   * @param {number} ms
   * @returns {Promise<void>}
   * @private
   */
  _delay(ms) {
    return new Promise((resolve) => {
      this.homey.setTimeout(resolve, ms);
    });
  }

  /**
   * Register a JackeryMqttDevice to receive messages for its SN. Several devices may share
   * a single SN; only the first registration starts the shared poll loop.
   *
   * @param {import('./JackeryMqttDevice')} device - Device instance (getData().sn, onMessage, _onBrokerDisconnect).
   */
  registerDevice(device) {
    const { sn } = device.getData();
    if (!sn) {
      throw new Error('Missing sn in device data');
    }

    device._mqttClient = this;
    device._sn = sn;

    let entry = this._snEntries.get(sn);
    if (!entry) {
      entry = {
        handlers: new Map(),
        pollStartTimer: null,
        pollInterval: null,
      };
      this._snEntries.set(sn, entry);
    }

    if (entry.handlers.has(device)) return;

    entry.handlers.set(device, {
      onMessage: device.onMessage.bind(device),
      onBrokerDisconnect: device._onBrokerDisconnect && device._onBrokerDisconnect.bind(device),
      getToken: () => (typeof device._getToken === 'function' ? device._getToken() : ''),
    });
    this.log(`Registering device: ${sn} (${entry.handlers.size} handler(s))`);

    if (entry.handlers.size === 1) {
      this._startPollForDevice(sn);
    }
  }

  /**
   * Unregister a device. Stops the per-SN poll loop when the last handler leaves and asks
   * the app to destroy the client when no SNs remain.
   *
   * @param {import('./JackeryMqttDevice')} device - Device instance to remove.
   */
  unregisterDevice(device) {
    const sn = device && (device._sn || device.getData?.().sn);
    if (!sn) return;

    const entry = this._snEntries.get(sn);
    if (!entry) return;
    if (!entry.handlers.delete(device)) return;

    this.log(`Unregistering device: ${sn} (${entry.handlers.size} handler(s) remaining)`);

    if (entry.handlers.size === 0) {
      this._stopPollForDevice(sn);
      this._snEntries.delete(sn);
    }

    if (this._snEntries.size === 0 && this._app && this._app.destroyMqttClient) {
      this._app.destroyMqttClient().catch(err => this.error('destroyMqttClient:', err));
    }
  }

  /**
   * Start the shared type 25/100 poll loop for a device SN. Mirrors HA
   * `_periodic_data_request`: 2 s warm-up, then one cycle every `REQUEST_INTERVAL_MS`.
   *
   * @param {string} sn - Device serial number.
   * @private
   */
  _startPollForDevice(sn) {
    const entry = this._snEntries.get(sn);
    if (!entry) return;
    this._stopPollForDevice(sn);

    const runPolls = () => {
      this._sendPeriodicPollsForDevice(sn).catch(err => this.error(`Poll error for ${sn}:`, err));
    };

    entry.pollStartTimer = this.homey.setTimeout(() => {
      entry.pollStartTimer = null;
      runPolls();
      entry.pollInterval = this.homey.setInterval(runPolls, REQUEST_INTERVAL_MS);
    }, POLL_START_DELAY_MS);
  }

  /**
   * Clears poll timers for one SN so unregister or reconnect does not leave duplicate intervals.
   *
   * @param {string} sn - Device serial number.
   * @private
   */
  _stopPollForDevice(sn) {
    const entry = this._snEntries.get(sn);
    if (!entry) return;
    if (entry.pollStartTimer) {
      this.homey.clearTimeout(entry.pollStartTimer);
      entry.pollStartTimer = null;
    }
    if (entry.pollInterval) {
      this.homey.clearInterval(entry.pollInterval);
      entry.pollInterval = null;
    }
  }

  /**
   * Uses the first handler token for a shared SN so battery and inverter devices do not double-poll.
   *
   * @param {string} sn - Device serial number.
   * @returns {Promise<void>}
   * @private
   */
  async _sendPeriodicPollsForDevice(sn) {
    const entry = this._snEntries.get(sn);
    if (!entry || entry.handlers.size === 0) return;

    let token = '';
    for (const handler of entry.handlers.values()) {
      try {
        const t = handler.getToken();
        if (t) {
          token = t;
          break;
        }
      } catch (err) {
        this.error(`getToken error for ${sn}:`, err);
      }
    }
    if (!token) return;

    await this.publishPeriodicPolls(sn, token);
  }

  /**
   * Fan-out for one inbound message: pairing discovery first, then every handler registered for the SN.
   *
   * @param {string} topic - Full topic.
   * @param {Buffer} buffer - Raw payload.
   * @private
   */
  _routeMessage(topic, buffer) {
    if (!this._topicPattern) return;

    const match = topic.match(this._topicPattern);
    if (!match) return;

    const [, sn, kind] = match;
    const body = this._parsePayload(buffer);
    if (body === null) return;

    if (this._discoveryCallback) {
      try {
        this._discoveryCallback(sn, kind, body);
      } catch (err) {
        this.error('Discovery callback error:', err);
      }
    }

    const entry = this._snEntries.get(sn);
    if (!entry) return;

    for (const handler of entry.handlers.values()) {
      try {
        handler.onMessage(sn, kind, body);
      } catch (err) {
        this.error(`Device ${sn} onMessage error:`, err);
      }
    }
  }

  /**
   * Normalizes inbound JSON so drivers only receive fields meant for capability and settings sync.
   *
   * @param {Buffer} buffer - Raw payload.
   * @returns {Object|null} Parsed state slice, `{}` when empty, or `null` when JSON is invalid.
   * @private
   */
  _parsePayload(buffer) {
    const str = buffer.toString();
    if (!str) return {};

    try {
      const raw = JSON.parse(str);
      return extractStateFromPayload(raw);
    } catch (err) {
      this.error('Invalid JSON payload:', err.message);
      return null;
    }
  }

  /**
   * Prefixes app log output so MQTT client lines are easy to filter in Homey CLI.
   *
   * @param {...*} args
   */
  log(...args) {
    this.homey.log('[JackeryMqttClient]', ...args);
  }

  /**
   * Prefixes app error output so MQTT client lines are easy to filter in Homey CLI.
   *
   * @param {...*} args
   */
  error(...args) {
    this.homey.error('[JackeryMqttClient]', ...args);
  }

};
