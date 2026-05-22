'use strict';

const JackeryMqttDevice = require('./JackeryMqttDevice');
const { setCapabilityOptions } = require('./Utils/SyncCapabilityOptions');
const { parsePvPower } = require('./Utils/CalculateNetPower');

const ENERGY_SCALE = 0.01;

/** Per-string PV power capabilities; MQTT may send scalars or nested objects like total `pvPw`. */
const PV_POWER_MAP = [
  ['measure_power.pv1', 'pv1'],
  ['measure_power.pv2', 'pv2'],
  ['measure_power.pv3', 'pv3'],
  ['measure_power.pv4', 'pv4'],
];

/** Per-string and export energy meters; MQTT counters use 0.01 kWh steps. */
const PV_METER_ENERGY_MAP = [
  ['meter_power.pv1', 'pv1Egy'],
  ['meter_power.pv2', 'pv2Egy'],
  ['meter_power.pv3', 'pv3Egy'],
  ['meter_power.pv4', 'pv4Egy'],
  ['meter_power.pv_exported', 'pvOtOngridEgy'],
];

/**
 * PV device: uses shared `parsePvPower` because Jackery sends `pvPw` as scalar or nested object (HA parity).
 */
module.exports = class JackeryMqttInverterDevice extends JackeryMqttDevice {

  async onInit() {
    await super.onInit();

    await setCapabilityOptions(this);

    const sn = this.getData()?.sn;
    if (sn) {
      await this.setSettings({ deviceSerial: sn }).catch(this.error);
    }
  }

  /**
   * Applies Jackery PV fields to solar capabilities and syncs shared unit settings from MQTT.
   *
   * @param {Object} body - Parsed status body from MQTT.
   */
  async onDeviceState(body) {
    if (body.pvPw !== undefined && body.pvPw !== null) {
      const pvPower = parsePvPower(body.pvPw);
      this.setCapabilityValue('measure_power', pvPower).catch(this.error);
    }

    this._setIfPresent('meter_power', body.pvEgy, (v) => Number(v) * ENERGY_SCALE);

    for (const [capabilityId, jsonKey] of PV_POWER_MAP) {
      if (!this.hasCapability(capabilityId)) continue;
      this._setIfPresent(capabilityId, body[jsonKey], parsePvPower);
    }

    for (const [capabilityId, jsonKey] of PV_METER_ENERGY_MAP) {
      if (!this.hasCapability(capabilityId)) continue;
      this._setIfPresent(capabilityId, body[jsonKey], (v) => Number(v) * ENERGY_SCALE);
    }

    await this._syncFromBody(body);
  }

};
