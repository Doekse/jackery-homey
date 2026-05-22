'use strict';

const Homey = require('homey');
const JackeryMqttClient = require('./JackeryMqttClient');

/** Pair/repair window before giving up when the target SN never answers polls. */
const DISCOVERY_TIMEOUT_MS = 15000;

/** Re-polls silent units during pairing; HA also relies on periodic type 25/100 while waiting. */
const DISCOVERY_POLL_INTERVAL_MS = 2000;

/** Initial delay after subscribe so the first type 25/100 round matches HA coordinator warm-up. */
const DISCOVERY_WAKE_DELAY_MS = 2000;

/**
 * Base driver for Jackery MQTT pairing and repair.
 *
 * Pairing: intro → loading → connection → credentials → list_devices → add_devices.
 * Repair reuses connection + credentials and verifies the SN on MQTT before saving store.
 */
module.exports = class JackeryMqttDriver extends Homey.Driver {

  /**
   * Subclasses exclude SNs that belong to another driver or role (e.g. non-SolarVault units).
   *
   * @param {string} sn - Device serial number from the MQTT topic.
   * @param {Object} [_body] - Parsed status body (when available).
   * @returns {boolean} Whether to include the device in the pair list.
   */
  onFilterDevice(sn, _body) {
    return !!sn;
  }

  /**
   * Optional pair-list icon; SolarVault picks battery artwork from `batNum` in status.
   *
   * @param {string} sn - Device serial number.
   * @returns {string|undefined} Path relative to driver assets, or undefined for the default.
   */
  getDeviceIcon(sn) {
    return undefined;
  }

  /**
   * One Jackery SN may surface as multiple Homey devices (e.g. battery + inverter).
   *
   * @param {string} sn - Device serial number.
   * @param {Object} [_body] - Parsed status body (when available).
   * @returns {Promise<Array<{ name: string, data: Object, icon?: string, class?: string, capabilities?: string[], capabilitiesOptions?: Object }>>}
   */
  async onBuildPairDevices(sn, _body) {
    const icon = this.getDeviceIcon(sn);
    return [{ name: sn, data: { sn }, ...(icon && { icon }) }];
  }

  /**
   * Temporary MQTT session for pair/repair: polls known SNs until timeout or the repair target appears.
   *
   * @param {Object} brokerConfig - Normalized broker config.
   * @param {Object} [opts] - Optional.
   * @param {string} [opts.targetSn] - Repair: stop early once this SN is seen.
   * @param {string} [opts.wakeSn] - Pairing: serial to poll before any status is seen.
   * @param {string} [opts.token] - Device token for type 25/100 polls (required for silent devices).
   * @returns {Promise<{ seen: Map<string, Object>, store: Object }>}
   * @private
   */
  async _runDiscovery(brokerConfig, opts = {}) {
    const { targetSn, wakeSn, token } = opts;
    const seen = new Map();
    const snsToPoll = new Set();
    if (targetSn) snsToPoll.add(targetSn);
    if (wakeSn) snsToPoll.add(wakeSn);

    let resolveDiscovery;
    const discoveryDone = new Promise((resolve) => { resolveDiscovery = resolve; });

    const noteDevice = (sn) => {
      if (targetSn && sn !== targetSn) return;
      if (!this.onFilterDevice(sn)) return;
      if (!seen.has(sn)) {
        seen.set(sn, {});
      }
      snsToPoll.add(sn);
      if (targetSn && sn === targetSn) {
        resolveDiscovery();
      }
    };

    const discoveryCallback = (sn, kind, body) => {
      if (kind !== 'status') return;
      noteDevice(sn);
      if (body && typeof body === 'object') {
        const existing = seen.get(sn) || {};
        seen.set(sn, { ...existing, ...body });
      }
    };

    const client = new JackeryMqttClient(this.homey);
    client.setDiscoveryCallback(discoveryCallback);

    let pollTimer = null;

    const runPolls = async () => {
      if (!token || snsToPoll.size === 0) return;
      for (const sn of snsToPoll) {
        try {
          await client.publishPeriodicPolls(sn, token);
          this.log(`Discovery poll (type 25 & 100) → ${sn}`);
        } catch (err) {
          this.error(`Discovery poll failed for ${sn}:`, err);
        }
      }
    };

    try {
      await client.connect(brokerConfig);

      await new Promise((resolve) => {
        this.homey.setTimeout(resolve, DISCOVERY_WAKE_DELAY_MS);
      });

      await runPolls();
      pollTimer = this.homey.setInterval(() => {
        runPolls().catch((err) => this.error('Discovery poll loop:', err));
      }, DISCOVERY_POLL_INTERVAL_MS);

      if (targetSn) {
        await Promise.race([
          discoveryDone,
          new Promise((resolve) => {
            this.homey.setTimeout(resolve, DISCOVERY_TIMEOUT_MS);
          }),
        ]);
      } else {
        await new Promise((resolve) => {
          this.homey.setTimeout(resolve, DISCOVERY_TIMEOUT_MS);
        });
      }
    } finally {
      if (pollTimer) {
        this.homey.clearInterval(pollTimer);
      }
      client.setDiscoveryCallback(null);
      await client.disconnect();
    }

    const normalized = JackeryMqttClient.normalizeBrokerConfig(brokerConfig);
    const store = {
      host: normalized.host,
      port: normalized.port,
      username: normalized.username,
      password: normalized.password,
      useTls: normalized.useTls,
    };

    return { seen, store };
  }

  /**
   * Builds the pair list after credentials are saved; broker config must come from the connection step.
   *
   * @returns {Promise<Array<{ name: string, data: Object, store: Object }>>}
   * @private
   */
  async _discoverDevices() {
    const { seen, store } = await this._runDiscovery(this._pairBrokerConfig, {
      token: this._pairToken,
      wakeSn: this._pairWakeSn,
    });
    const token = this._pairToken;
    const result = [];
    for (const [sn, body] of seen) {
      if (!this.onFilterDevice(sn)) continue;
      const entries = await this.onBuildPairDevices(sn, body);
      for (const entry of entries) {
        result.push({
          ...entry,
          store: { ...store, ...(token && { token }) },
        });
      }
    }
    return result;
  }

  /**
   * Wires Homey pairing views to broker save, credential wake-SN, and MQTT discovery.
   *
   * @param {Object} session - Homey pairing session.
   */
  onPair(session) {
    this._pairBrokerConfig = null;
    this._pairToken = null;
    this._pairWakeSn = null;

    session.setHandler('showView', async (viewId) => {
      if (viewId === 'loading') {
        const client = await this.homey.app.getMqttClient();
        const config = client ? client.getConfig() : null;
        if (config) {
          this._pairBrokerConfig = JackeryMqttClient.normalizeBrokerConfig(config);
        }
        await session.nextView();
      }
    });

    session.setHandler('get_pair_context', () => ({ isRepair: false }));

    session.setHandler('get_connection_form_defaults', async () => (
      this._pairBrokerConfig
        ? { ...this._pairBrokerConfig }
        : JackeryMqttClient.normalizeBrokerConfig({
          host: '', port: 1883, username: '', password: '', useTls: false,
        })
    ));

    session.setHandler('get_credentials_form_defaults', async () => ({
      token: this._pairToken || '',
      deviceSn: this._pairWakeSn || '',
    }));

    session.setHandler('save_broker_config', (config) => {
      this._pairBrokerConfig = JackeryMqttClient.normalizeBrokerConfig(config);
      return Promise.resolve();
    });

    session.setHandler('save_credentials', (data) => {
      const token = typeof data === 'string' ? data : data?.token;
      const trimmedToken = token != null ? String(token).trim() : '';
      if (trimmedToken) {
        this._pairToken = trimmedToken;
      }
      const sn = data?.deviceSn != null ? String(data.deviceSn).trim() : '';
      if (!sn) {
        throw new Error(this.homey.__('pair.credentials.serial_required'));
      }
      this._pairWakeSn = sn;
      return Promise.resolve();
    });

    session.setHandler('list_devices', async () => this.onPairListDevices());

    session.setHandler('add_devices', async (selectedDevices) => {
      const token = this._pairToken;
      if (!token) {
        throw new Error(this.homey.__('pair.token.required'));
      }
      return selectedDevices.map((device) => ({
        ...device,
        store: {
          ...device.store,
          token,
        },
      }));
    });
  }

  /**
   * Homey `list_devices` handler; requires broker config and wake credentials from earlier steps.
   *
   * @returns {Promise<Array<{ name: string, data: Object, store: Object }>>}
   */
  async onPairListDevices() {
    const config = this._pairBrokerConfig;
    if (!config) {
      throw new Error(this.homey.__('pair.list_devices.no_config'));
    }
    if (!this._pairToken || !this._pairWakeSn) {
      throw new Error(this.homey.__('pair.list_devices.no_credentials'));
    }

    let devices;
    try {
      devices = await this._discoverDevices();
    } catch (err) {
      throw new Error(`${this.homey.__('pair.list_devices.connection_failed')} ${err.message || ''}`);
    }
    if (devices.length === 0) {
      throw new Error(this.homey.__('pair.list_devices.no_devices'));
    }
    return devices;
  }

  /**
   * Re-validates broker + token on MQTT before persisting store and re-attaching the live client.
   *
   * @param {Object} session - Homey repair session.
   * @param {Homey.Device} device - Device being repaired.
   */
  onRepair(session, device) {
    this._repairBrokerConfig = null;

    session.setHandler('get_pair_context', () => ({ isRepair: true }));

    session.setHandler('get_connection_form_defaults', async () => {
      const store = device.getStore();
      return JackeryMqttClient.normalizeBrokerConfig(store);
    });

    session.setHandler('get_credentials_form_defaults', async () => {
      const store = device.getStore();
      return {
        token: store.token || '',
        deviceSn: device.getData()?.sn || '',
      };
    });

    session.setHandler('save_repair_broker_config', (config) => {
      this._repairBrokerConfig = JackeryMqttClient.normalizeBrokerConfig(config);
      return Promise.resolve();
    });

    session.setHandler('save_repair_credentials', async (data) => {
      const token = typeof data === 'string' ? data : data?.token;
      const trimmedToken = token != null ? String(token).trim() : '';
      if (!trimmedToken) {
        throw new Error(this.homey.__('pair.token.required'));
      }

      const targetSn = data?.deviceSn != null ? String(data.deviceSn).trim() : '';
      if (!targetSn) {
        throw new Error(this.homey.__('pair.credentials.serial_required'));
      }

      const brokerConfig = this._repairBrokerConfig
        || JackeryMqttClient.normalizeBrokerConfig(device.getStore());
      if (!brokerConfig.host?.trim()) {
        throw new Error(this.homey.__('pair.connection.host_required'));
      }

      const { seen, store } = await this._runDiscovery(brokerConfig, {
        targetSn,
        token: trimmedToken,
      });
      if (!seen.has(targetSn) || !this.onFilterDevice(targetSn)) {
        throw new Error(this.homey.__('repair.device_not_found'));
      }

      await Promise.all([
        device.setStoreValue('host', store.host),
        device.setStoreValue('port', store.port),
        device.setStoreValue('username', store.username),
        device.setStoreValue('password', store.password),
        device.setStoreValue('useTls', store.useTls),
        device.setStoreValue('token', trimmedToken),
      ]);

      const currentData = device.getData() || {};
      if (currentData.sn !== targetSn) {
        if (currentData.sn && device._mqttClient) {
          device._mqttClient.unregisterDevice(device);
        }
        await device.setData({ ...currentData, sn: targetSn });
      }

      const client = await this.homey.app.getMqttClient(brokerConfig);
      if (typeof device.onMqttInit === 'function') {
        device.onMqttInit(client);
      }
      await device.setAvailable();
    });
  }

};
