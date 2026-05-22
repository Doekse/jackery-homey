'use strict';

const Homey = require('homey');
const JackeryMqttClient = require('./JackeryMqttClient');
const { SettingToBool } = require('./Utils/SettingToBool');

/** Marks the device offline when no MQTT traffic arrives within one HA poll interval. */
const STALE_DATA_TIMEOUT_MS = 60000;

/** Clamps `maxOutPw` to the Jackery HA UI limit (protocol cap, not hardware rating). */
const MAX_ONGRID_OUTPUT_W = 10000;

/** Unit settings pushed with cmd 5 and mirrored from inbound status. */
const DEVICE_SETTING_KEYS = ['socChgLimit', 'socDischgLimit', 'maxOutPw', 'isAutoStandby', 'autoStandby'];

/** Homey stores these as checkboxes; Jackery expects 0/1 on the wire. */
const BOOLEAN_SETTING_KEYS = new Set(['isAutoStandby']);

/** Advanced settings filled from MQTT for display only (never sent on cmd 5). */
const DEVICE_INFO_FROM_BODY = [
  ['deviceEthIp', 'eip'],
  ['deviceEthMac', 'emac'],
  ['deviceWifiIp', 'wip'],
  ['deviceWifiMac', 'mac'],
  ['deviceBatteryCount', 'batNum'],
];

/**
 * Base Jackery MQTT device: uses the app-level client; poll cadence is per SN, shared by every device with that serial number.
 */
class JackeryMqttDevice extends Homey.Device {

  /**
   * Pairing stores the unit token in device store; polls and cmd 5 require it on every action.
   *
   * @returns {string}
   * @private
   */
  _getToken() {
    const storeToken = this.getStore()?.token;
    if (typeof storeToken === 'string' && storeToken.trim()) {
      return storeToken.trim();
    }
    return '';
  }

  async onInit() {
    this._lastMessageAt = 0;
    this._staleCheckInterval = null;
    this._stateCache = {};
    this._syncingSettings = false;

    const store = this.getStore();
    const brokerConfig = JackeryMqttClient.normalizeBrokerConfig({
      host: store.host,
      port: store.port,
      username: store.username,
      password: store.password,
      useTls: store.useTls,
    });

    const unreachableMsg = this.homey.__('errors.broker_unreachable');
    if (!brokerConfig.host?.trim()) {
      await this.setUnavailable(unreachableMsg).catch(this.error);
      return;
    }

    if (!this._getToken()) {
      await this.setUnavailable(this.homey.__('pair.token.required')).catch(this.error);
      return;
    }

    try {
      await this._connectMqtt(brokerConfig);
    } catch (err) {
      this.error('Broker connection failed:', err);
      await this.setUnavailable(unreachableMsg).catch(this.error);
    }
  }

  /**
   * Reuses the app singleton so multiple devices on one broker share one TCP session.
   *
   * @param {Object} brokerConfig - Normalized broker config.
   * @returns {Promise<void>}
   * @private
   */
  async _connectMqtt(brokerConfig) {
    const client = await this.homey.app.getMqttClient(brokerConfig);
    this.onMqttInit(client);
    this._startStaleCheck();
  }

  /**
   * Hooks this device into the shared client poll loop and message fan-out for its SN.
   *
   * @param {import('./JackeryMqttClient')} client - Connected Jackery MQTT client.
   */
  onMqttInit(client) {
    client.registerDevice(this);
  }

  async onDeleted() {
    this._stopStaleCheck();
    if (this._mqttClient) {
      this._mqttClient.unregisterDevice(this);
    }
  }

  /**
   * Surfaces broker loss in the device UI instead of showing last-known capability values.
   *
   * @private
   */
  _onBrokerDisconnect() {
    this.setUnavailable(this.homey.__('errors.offline')).catch(this.error);
  }

  /**
   * Merges status/event bodies into a cache so subclasses see the latest combined unit state.
   *
   * @param {string} sn - Device serial number.
   * @param {string} kind - `status` or `event`.
   * @param {Object} body - Parsed MQTT body.
   */
  onMessage(sn, kind, body) {
    if (kind !== 'status' && kind !== 'event') return;
    if (!body || typeof body !== 'object') return;

    const keys = Object.keys(body);
    if (keys.length === 0) return;

    Object.assign(this._stateCache, body);

    this._lastMessageAt = Date.now();
    this.setAvailable().catch(this.error);
    this.onDeviceState(this._stateCache)
      .catch(err => this.error(`Error setting device state: ${err.message}`));
  }

  /**
   * Subclasses map protocol fields to driver-specific capabilities.
   *
   * @param {Object} body - Merged status payload.
   * @returns {Promise<void>}
   */
  async onDeviceState(body) {
  }

  /**
   * Treats silence after the last MQTT message as offline (complements broker disconnect).
   *
   * @private
   */
  _startStaleCheck() {
    this._stopStaleCheck();
    this._staleCheckInterval = this.homey.setInterval(() => {
      if (!this._lastMessageAt) return;
      if (Date.now() - this._lastMessageAt > STALE_DATA_TIMEOUT_MS) {
        this.setUnavailable(this.homey.__('errors.offline')).catch(this.error);
      }
    }, JackeryMqttClient.REQUEST_INTERVAL_MS);
  }

  /**
   * Stops the stale-data timer on delete so the device does not keep firing after removal.
   *
   * @private
   */
  _stopStaleCheck() {
    if (this._staleCheckInterval) {
      this.homey.clearInterval(this._staleCheckInterval);
      this._staleCheckInterval = null;
    }
  }

  /**
   * Forwards setting changes to the unit via Jackery cmd 5 when Homey or Flow updates them.
   *
   * @param {Object} params - Protocol fields to set on the unit.
   * @returns {Promise<void>}
   */
  async publishMainDeviceControl(params) {
    if (!this._mqttClient || !this._sn) {
      throw new Error(this.homey.__('errors.offline'));
    }
    const token = this._getToken();
    if (!token) {
      throw new Error(this.homey.__('pair.token.required'));
    }
    await this._mqttClient.publishMainDeviceControl(this._sn, token, params);
  }

  /**
   * Skips undefined MQTT fields; `setCapabilityValue` drives Flow cards declared in compose.
   *
   * @param {string} capabilityId
   * @param {*} raw - Source value from MQTT.
   * @param {function(*): number} [transform]
   * @protected
   */
  _setIfPresent(capabilityId, raw, transform) {
    if (raw === undefined || raw === null) return;
    const value = transform ? transform(raw) : raw;
    if (value === undefined || Number.isNaN(value)) return;
    this.setCapabilityValue(capabilityId, value).catch(this.error);
  }

  /**
   * Homey does not call {@link onSettings} for programmatic {@link setSettings} (e.g. Flow).
   *
   * @param {Object} settings - Partial settings object.
   * @returns {Promise<void>}
   */
  async setSettings(settings) {
    if (this._syncingSettings) {
      return super.setSettings(settings);
    }

    const newSettings = { ...this.getSettings(), ...settings };
    await this._applySettingsChange({
      newSettings,
      changedKeys: Object.keys(settings),
    });
    return super.setSettings(settings);
  }

  /**
   * User edits in advanced settings; skipped while MQTT is syncing to avoid cmd 5 loops.
   *
   * @param {Object} param0
   * @param {Object} param0.newSettings
   * @param {string[]} param0.changedKeys
   */
  async onSettings({ newSettings, changedKeys }) {
    if (this._syncingSettings) return;
    await this._applySettingsChange({ newSettings, changedKeys });
  }

  /**
   * Validates and pushes unit setting changes to the Jackery over MQTT.
   *
   * @param {Object} param0
   * @param {Object} param0.newSettings - Full settings after the change.
   * @param {string[]} param0.changedKeys - Keys the user or Flow changed.
   * @returns {Promise<void>}
   * @private
   */
  async _applySettingsChange({ newSettings, changedKeys }) {
    const changed = changedKeys.filter((key) => DEVICE_SETTING_KEYS.includes(key));
    if (changed.length === 0) return;

    if (changed.includes('autoStandby') && !SettingToBool(newSettings.isAutoStandby)) {
      throw new Error(this.homey.__('errors.auto_standby_not_allowed'));
    }

    const params = {};
    for (const key of changed) {
      const raw = newSettings[key];
      if (raw === undefined || raw === null) continue;

      let value;
      if (BOOLEAN_SETTING_KEYS.has(key)) {
        value = SettingToBool(raw) ? 1 : 0;
      } else {
        if (raw === '') continue;
        value = parseInt(String(raw), 10);
        if (Number.isNaN(value)) continue;
        if (key === 'maxOutPw') {
          value = Math.max(0, Math.min(MAX_ONGRID_OUTPUT_W, value));
        }
      }
      params[key] = value;
    }
    if (Object.keys(params).length === 0) return;

    try {
      await this.publishMainDeviceControl(params);
    } catch (err) {
      this.error('Failed to apply device settings:', err);
      throw new Error(this.homey.__('errors.settings_apply_failed'));
    }
  }

  /**
   * Keeps advanced settings aligned with the unit without echoing MQTT back as user edits.
   *
   * @param {Object} body - Merged status body.
   * @returns {Promise<void>}
   * @protected
   */
  async _syncFromBody(body) {
    const current = this.getSettings();
    const updates = {};

    for (const key of DEVICE_SETTING_KEYS) {
      if (body[key] === undefined || body[key] === null) continue;
      const value = parseInt(String(body[key]), 10);
      if (Number.isNaN(value)) continue;
      if (key === 'autoStandby' && value === 0) continue;

      if (BOOLEAN_SETTING_KEYS.has(key)) {
        const boolVal = value === 1;
        if (SettingToBool(current[key]) === boolVal) continue;
        updates[key] = boolVal;
        continue;
      }

      const currentVal = key === 'autoStandby'
        ? parseInt(String(current[key] ?? '0'), 10)
        : Number(current[key]);
      if (currentVal === value) continue;
      updates[key] = key === 'autoStandby' ? String(value) : value;
    }

    for (const [settingId, jsonKey] of DEVICE_INFO_FROM_BODY) {
      const raw = body[jsonKey];
      if (raw === undefined || raw === null || raw === '') continue;
      const value = settingId === 'deviceBatteryCount'
        ? String(parseInt(String(raw), 10))
        : String(raw).trim();
      if (!value || value === 'NaN') continue;
      if (current[settingId] === value) continue;
      updates[settingId] = value;
    }

    if (body.wname !== undefined && body.wname !== null) {
      const ssid = String(body.wname).trim();
      const display = ssid || '-';
      if (current.deviceWifiSsid !== display) {
        updates.deviceWifiSsid = display;
      }
    }

    if (body.wsig !== undefined && body.wsig !== null) {
      const sig = Number(body.wsig);
      const display = Number.isNaN(sig) || sig === 0 ? '-' : `${sig} dBm`;
      if (current.deviceWifiSignal !== display) {
        updates.deviceWifiSignal = display;
      }
    }

    if (Object.keys(updates).length === 0) return;

    this._syncingSettings = true;
    try {
      await this.setSettings(updates);
      if (typeof this._triggerFlowCards === 'function') {
        const settingUpdates = Object.fromEntries(
          Object.entries(updates).filter(([key]) => DEVICE_SETTING_KEYS.includes(key)),
        );
        if (Object.keys(settingUpdates).length > 0) {
          await this._triggerFlowCards(settingUpdates);
        }
      }
    } catch (err) {
      this.error('Failed to sync settings from device:', err);
    } finally {
      this._syncingSettings = false;
    }
  }

}

module.exports = JackeryMqttDevice;
module.exports.DEVICE_SETTING_KEYS = DEVICE_SETTING_KEYS;
module.exports.BOOLEAN_SETTING_KEYS = BOOLEAN_SETTING_KEYS;
