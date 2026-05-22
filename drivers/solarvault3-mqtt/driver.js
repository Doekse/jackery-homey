'use strict';

const JackeryMqttDriver = require('../../lib/JackeryMqttDriver');
const { getCapabilityOptions } = require('../../lib/Utils/SyncCapabilityOptions');
const { getIconPath } = require('../../lib/Utils/SyncIconPath');
const JackeryMqttBatteryDevice = require('../../lib/JackeryMqttBatteryDevice');
const JackeryMqttInverterDevice = require('../../lib/JackeryMqttInverterDevice');

/** Battery device capabilities must be fixed at pair time; Homey does not add them later. */
const BATTERY_CAPABILITIES = [
  'measure_battery',
  'measure_power',
  'measure_power.eps',
  'measure_temperature',
  'meter_power.charged',
  'meter_power.discharged',
  'meter_power.ac_imported',
  'meter_power.ac_exported',
  'meter_power.grid_imported',
  'meter_power.grid_exported',
  'meter_power.pv_imported',
  'onoff.eps',
  'alarm_eth',
];

/** PV device capabilities must be fixed at pair time; Homey does not add them later. */
const INVERTER_CAPABILITIES = [
  'measure_power',
  'measure_power.pv1',
  'measure_power.pv2',
  'measure_power.pv3',
  'measure_power.pv4',
  'meter_power',
  'meter_power.pv1',
  'meter_power.pv2',
  'meter_power.pv3',
  'meter_power.pv4',
  'meter_power.pv_exported',
];

/**
 * SolarVault 3 driver: one MQTT unit becomes separate battery and PV Homey devices.
 */
module.exports = class JackerySolarVaultDriver extends JackeryMqttDriver {

  async onInit() {
    this.log('SolarVault 3 MQTT Driver has been initialized');
    this._registerFlowConditionCards();
    this._registerFlowActionCards();
  }

  /**
   * Conditions for settings and sub-capabilities Homey does not generate from compose alone.
   *
   * @private
   */
  _registerFlowConditionCards() {
    this.homey.flow.getConditionCard('alarm_eth_status').registerRunListener(async (args, state) => {
      this._requireCapability(args.device, 'alarm_eth', 'errors.flow_alarm_eth_unavailable');
      const disconnected = await args.device.getCapabilityValue('alarm_eth');
      return state.inverted ? !disconnected : disconnected === true;
    });

    this.homey.flow.getConditionCard('ac_socket_is_on').registerRunListener(async (args, state) => {
      this._requireCapability(args.device, 'onoff.eps', 'errors.flow_ac_socket_unavailable');
      const isOn = await args.device.getCapabilityValue('onoff.eps');
      return state.inverted ? !isOn : isOn === true;
    });

    this.homey.flow.getConditionCard('charge_limit_is').registerRunListener(async (args, state) => {
      this._requireBattery(args.device);
      const match = Number(args.device.getSettings().socChgLimit) === Number(args.value);
      return state.inverted ? !match : match;
    });

    this.homey.flow.getConditionCard('discharge_limit_is').registerRunListener(async (args, state) => {
      this._requireBattery(args.device);
      const match = Number(args.device.getSettings().socDischgLimit) === Number(args.value);
      return state.inverted ? !match : match;
    });

    this.homey.flow.getConditionCard('max_ongrid_output_is').registerRunListener(async (args, state) => {
      this._requireBattery(args.device);
      const match = Number(args.device.getSettings().maxOutPw) === Number(args.value);
      return state.inverted ? !match : match;
    });

    this.homey.flow.getConditionCard('auto_standby_is').registerRunListener(async (args, state) => {
      this._requireBattery(args.device);
      const match = parseInt(args.device.getSettings().autoStandby, 10) === parseInt(args.mode, 10);
      return state.inverted ? !match : match;
    });

    this.homey.flow.getConditionCard('auto_standby_allowed_is').registerRunListener(async (args, state) => {
      this._requireBattery(args.device);
      const allowed = args.device.getSettings().isAutoStandby === true
        || parseInt(String(args.device.getSettings().isAutoStandby), 10) === 1;
      return state.inverted ? !allowed : allowed;
    });
  }

  /**
   * Actions for `onoff.eps` and unit settings; sub-capability toggles lack SDK auto-actions.
   *
   * @private
   */
  _registerFlowActionCards() {
    this.homey.flow.getActionCard('ac_socket_turn_on').registerRunListener(async (args) => {
      this._requireCapability(args.device, 'onoff.eps', 'errors.flow_ac_socket_unavailable');
      await args.device.setCapabilityValue('onoff.eps', true);
    });

    this.homey.flow.getActionCard('ac_socket_turn_off').registerRunListener(async (args) => {
      this._requireCapability(args.device, 'onoff.eps', 'errors.flow_ac_socket_unavailable');
      await args.device.setCapabilityValue('onoff.eps', false);
    });

    this.homey.flow.getActionCard('ac_socket_toggle').registerRunListener(async (args) => {
      this._requireCapability(args.device, 'onoff.eps', 'errors.flow_ac_socket_unavailable');
      const isOn = await args.device.getCapabilityValue('onoff.eps');
      await args.device.setCapabilityValue('onoff.eps', !isOn);
    });

    this.homey.flow.getActionCard('set_charge_limit').registerRunListener(async (args) => {
      this._requireBattery(args.device);
      const value = Math.max(0, Math.min(100, parseInt(args.value, 10)));
      await args.device.setSettings({ socChgLimit: value });
    });

    this.homey.flow.getActionCard('set_discharge_limit').registerRunListener(async (args) => {
      this._requireBattery(args.device);
      const value = Math.max(0, Math.min(100, parseInt(args.value, 10)));
      await args.device.setSettings({ socDischgLimit: value });
    });

    this.homey.flow.getActionCard('set_max_ongrid_output').registerRunListener(async (args) => {
      this._requireBattery(args.device);
      const value = Math.max(0, Math.min(10000, parseInt(args.value, 10)));
      await args.device.setSettings({ maxOutPw: value });
    });

    this.homey.flow.getActionCard('set_auto_standby_mode').registerRunListener(async (args) => {
      this._requireBattery(args.device);
      await args.device.setSettings({ autoStandby: String(args.mode) });
    });

    this.homey.flow.getActionCard('enable_auto_standby_allowed').registerRunListener(async (args) => {
      this._requireBattery(args.device);
      await args.device.setSettings({ isAutoStandby: true });
    });

    this.homey.flow.getActionCard('disable_auto_standby_allowed').registerRunListener(async (args) => {
      this._requireBattery(args.device);
      await args.device.setSettings({ isAutoStandby: false });
    });
  }

  /**
   * Flow cards target either device; missing capabilities get a localized error instead of a silent no-op.
   *
   * @param {import('homey/lib/Device')} device
   * @param {string} capabilityId
   * @param {string} localeKey
   * @private
   */
  _requireCapability(device, capabilityId, localeKey) {
    if (!device.hasCapability(capabilityId)) {
      throw new Error(this.homey.__(localeKey));
    }
  }

  /**
   * Unit settings Flow cards apply only to the battery device, not the PV device on the same SN.
   *
   * @param {import('homey/lib/Device')} device
   * @private
   */
  _requireBattery(device) {
    if (device.getData()?.role !== 'battery') {
      throw new Error(this.homey.__('errors.flow_battery_settings_unavailable'));
    }
  }

  /**
   * Picks battery vs PV device implementation from pair-time `data.role`.
   *
   * @param {import('homey/lib/Device')} device
   * @returns {typeof JackeryMqttBatteryDevice | typeof JackeryMqttInverterDevice}
   */
  onMapDeviceClass(device) {
    const role = device.getData()?.role;
    return role === 'inverter' ? JackeryMqttInverterDevice : JackeryMqttBatteryDevice;
  }

  /**
   * Exposes one Jackery SN as battery and inverter pair-list entries with role-specific capabilities.
   *
   * @param {string} sn - Device serial number from MQTT discovery.
   * @param {Object} [body] - Parsed status body (when available).
   * @returns {Promise<Array<Object>>}
   */
  async onBuildPairDevices(sn, body) {
    return [
      {
        name: `SolarVault 3 Battery (${sn})`,
        data: { sn, role: 'battery' },
        class: 'battery',
        capabilities: BATTERY_CAPABILITIES,
        capabilitiesOptions: getCapabilityOptions(BATTERY_CAPABILITIES),
        icon: getIconPath(body),
      },
      {
        name: `SolarVault 3 Inverter (${sn})`,
        data: { sn, role: 'inverter' },
        class: 'solarpanel',
        capabilities: INVERTER_CAPABILITIES,
        capabilitiesOptions: getCapabilityOptions(INVERTER_CAPABILITIES),
        icon: '/models/battery-base.svg',
      },
    ];
  }

};
