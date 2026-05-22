'use strict';

const JackeryMqttDevice = require('../../lib/JackeryMqttDevice');

/**
 * Fallback device class; {@link JackerySolarVaultDriver#onMapDeviceClass} maps roles to battery/PV subclasses.
 */
module.exports = class JackerySolarVaultDevice extends JackeryMqttDevice {

  async onInit() {
    await super.onInit();
    this.log('SolarVault Device has been initialized');
  }

};
