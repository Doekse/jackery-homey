'use strict';

const JackeryMqttDevice = require('./JackeryMqttDevice');
const { DEVICE_SETTING_KEYS, BOOLEAN_SETTING_KEYS } = require('./JackeryMqttDevice');
const { SettingToBool } = require('./Utils/SettingToBool');
const { setCapabilityOptions } = require('./Utils/SyncCapabilityOptions');
const { calculateBatteryNetPower, calculateEpsNetPower } = require('./Utils/CalculateNetPower');

const ENERGY_SCALE = 0.01;
const TEMP_SCALE = 0.1;

/** MQTT energy counters use 0.01 kWh steps; Homey meters expect kWh. */
const METER_ENERGY_MAP = [
  ['meter_power.charged', 'batChgEgy'],
  ['meter_power.discharged', 'batDisChgEgy'],
  ['meter_power.ac_exported', 'batOtAcEgy'],
  ['meter_power.ac_imported', 'acOtBatEgy'],
  ['meter_power.grid_imported', 'ongridOtBatEgy'],
  ['meter_power.grid_exported', 'batOtGridEgy'],
  ['meter_power.pv_imported', 'pvOtBatEgy'],
];

/**
 * Battery device: capability mapping and Flow triggers aligned with the Jackery HA integration.
 */
module.exports = class JackeryMqttBatteryDevice extends JackeryMqttDevice {

  async onInit() {
    await super.onInit();

    this._registerFlowTriggerCards();

    await setCapabilityOptions(this);

    await this.setEnergy({
      homeBattery: true,
      batteries: ['OTHER'],
      meterPowerImportedCapability: 'meter_power.charged',
      meterPowerExportedCapability: 'meter_power.discharged',
    });

    const sn = this.getData()?.sn;
    if (sn) {
      await this.setSettings({ deviceSerial: sn }).catch(this.error);
    }

    if (this.hasCapability('onoff.eps')) {
      this.registerCapabilityListener('onoff.eps', async (isOn) => {
        await this.publishMainDeviceControl({ swEps: isOn ? 1 : 0 });
      });
    }
  }

  /**
   * @param {Object} body - Parsed status body from MQTT.
   */
  async onDeviceState(body) {
    this._setIfPresent('measure_battery', body.batSoc, (v) => Number(v));

    const netPower = calculateBatteryNetPower(body);
    this.setCapabilityValue('measure_power', netPower).catch(this.error);

    if (this.hasCapability('measure_power.eps')) {
      const epsPower = calculateEpsNetPower(body);
      this.setCapabilityValue('measure_power.eps', epsPower).catch(this.error);
    }

    for (const [capabilityId, jsonKey] of METER_ENERGY_MAP) {
      this._setIfPresent(capabilityId, body[jsonKey], (v) => Number(v) * ENERGY_SCALE);
    }

    if (body.cellTemp !== undefined && body.cellTemp !== null) {
      this.setCapabilityValue('measure_temperature', Number(body.cellTemp) * TEMP_SCALE).catch(this.error);
    }

    if (body.swEps !== undefined && body.swEps !== null && this.hasCapability('onoff.eps')) {
      const isOn = Number(body.swEps) === 1;
      await this._triggerFlowCards({ onoff_eps: isOn });
      this.setCapabilityValue('onoff.eps', isOn).catch(this.error);
    }

    if (body.ethPort !== undefined && body.ethPort !== null && this.hasCapability('alarm_eth')) {
      const disconnected = Number(body.ethPort) !== 1;
      this.setCapabilityValue('alarm_eth', disconnected).catch(this.error);
    }

    await this._syncFromBody(body);
  }

  /**
   * Binds device trigger cards once; dedupe state avoids duplicate fires on unchanged MQTT values.
   *
   * @private
   */
  _registerFlowTriggerCards() {
    this._flowTriggerCards = {
      onoff_eps: {
        true: this.homey.flow.getDeviceTriggerCard('ac_socket_turned_on'),
        false: this.homey.flow.getDeviceTriggerCard('ac_socket_turned_off'),
      },
      isAutoStandby: {
        true: this.homey.flow.getDeviceTriggerCard('auto_standby_allowed_turned_on'),
        false: this.homey.flow.getDeviceTriggerCard('auto_standby_allowed_turned_off'),
      },
      socChgLimit: this.homey.flow.getDeviceTriggerCard('charge_limit_changed'),
      socDischgLimit: this.homey.flow.getDeviceTriggerCard('discharge_limit_changed'),
      maxOutPw: this.homey.flow.getDeviceTriggerCard('max_ongrid_output_changed'),
      autoStandby: this.homey.flow.getDeviceTriggerCard('auto_standby_changed'),
    };
    this._flowTriggerLast = {};
    if (this.hasCapability('onoff.eps')) {
      this._flowTriggerLast.onoff_eps = this.getCapabilityValue('onoff.eps');
    }
    const settings = this.getSettings();
    for (const key of DEVICE_SETTING_KEYS) {
      const raw = settings[key];
      if (raw === undefined || raw === null || raw === '') continue;
      if (BOOLEAN_SETTING_KEYS.has(key)) {
        this._flowTriggerLast[key] = SettingToBool(raw);
        continue;
      }
      const value = parseInt(String(raw), 10);
      if (!Number.isNaN(value)) this._flowTriggerLast[key] = value;
    }
  }

  /**
   * Fires Flow triggers only when MQTT or settings actually change a watched value.
   *
   * @param {Object} changes - e.g. `{ onoff_eps: true }` or `{ socChgLimit: 80 }`.
   * @returns {Promise<void>}
   * @private
   */
  async _triggerFlowCards(changes) {
    for (const [key, value] of Object.entries(changes)) {
      if (BOOLEAN_SETTING_KEYS.has(key)) {
        const boolVal = SettingToBool(value);
        if (this._flowTriggerLast[key] === boolVal) continue;
        const cards = this._flowTriggerCards[key];
        if (this._flowTriggerLast[key] !== undefined && cards) {
          await cards[boolVal ? 'true' : 'false']?.trigger(this, {}, {});
        }
        this._flowTriggerLast[key] = boolVal;
        continue;
      }

      if (typeof value === 'boolean') {
        if (this._flowTriggerLast[key] === value) continue;
        const cards = this._flowTriggerCards[key];
        if (this._flowTriggerLast[key] !== undefined && cards) {
          await cards[value ? 'true' : 'false']?.trigger(this, {}, {});
        }
        this._flowTriggerLast[key] = value;
        continue;
      }

      const num = parseInt(String(value), 10);
      if (Number.isNaN(num) || this._flowTriggerLast[key] === num) continue;

      const card = this._flowTriggerCards[key];
      if (this._flowTriggerLast[key] !== undefined && card) {
        const tokens = { [key]: num };
        await card.trigger(this, tokens, tokens);
      }
      this._flowTriggerLast[key] = num;
    }
  }

};
