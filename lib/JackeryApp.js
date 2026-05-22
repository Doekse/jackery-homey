'use strict';

const Homey = require('homey');

const JackeryMqttClient = require('./JackeryMqttClient');

module.exports = class JackeryApp extends Homey.App {

  async onInit() {
    this._jackeryMqttClient = null;
  }

  /**
   * Single broker session for the app: omit `brokerConfig` to read the live client, pass config to connect.
   *
   * @param {Object} [brokerConfig] - { host, port?, username?, password?, useTls? }. Omit to get current client.
   * @param {Object} [options] - Optional when connecting.
   * @param {Function} [options.discoveryCallback] - Called with (sn, kind, body) during pairing; cleared after.
   * @returns {Promise<import('./JackeryMqttClient')|null>}
   */
  async getMqttClient(brokerConfig, options = {}) {
    if (!brokerConfig) {
      return this._jackeryMqttClient || null;
    }
    const normalized = JackeryMqttClient.normalizeBrokerConfig(brokerConfig);
    if (!this._jackeryMqttClient) {
      this._jackeryMqttClient = new JackeryMqttClient(this.homey, this);
    }
    this._jackeryMqttClient.setDiscoveryCallback(options.discoveryCallback);
    await this._jackeryMqttClient.connect(normalized);
    return this._jackeryMqttClient;
  }

  /**
   * Drops the shared client when the last Jackery device unregisters so Homey does not keep MQTT open.
   */
  async destroyMqttClient() {
    if (!this._jackeryMqttClient) return;
    await this._jackeryMqttClient.disconnect();
    this._jackeryMqttClient = null;
  }

  /**
   * Closes MQTT on app shutdown so a reinstall or disable does not leave a broker session behind.
   */
  async onUninit() {
    if (this._jackeryMqttClient) {
      await this._jackeryMqttClient.disconnect();
      this._jackeryMqttClient = null;
    }
  }

};
