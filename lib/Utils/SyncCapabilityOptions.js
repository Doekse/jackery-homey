'use strict';

const appManifest = require('../../app.json');

const SOLARVAULT_DRIVER_ID = 'solarvault3-mqtt';

/**
 * Reads compose manifest options so pair-time capability metadata matches the built driver.
 *
 * @param {string[]} capabilityIds
 * @returns {Record<string, object>}
 */
function getCapabilityOptions(capabilityIds) {
  const driver = appManifest.drivers?.find((entry) => entry.id === SOLARVAULT_DRIVER_ID);
  const source = driver?.capabilitiesOptions ?? {};
  /** @type {Record<string, object>} */
  const picked = {};

  for (const id of capabilityIds) {
    if (source[id]) {
      picked[id] = source[id];
    }
  }

  return picked;
}

/**
 * Re-applies manifest capability options on init so locale/title changes from compose reach devices.
 *
 * @param {import('homey/lib/Device')} device
 * @returns {Promise<void>}
 */
async function setCapabilityOptions(device) {
  const options = getCapabilityOptions(device.getCapabilities());

  await Promise.all(
    Object.entries(options).map(([capabilityId, capabilityOptions]) =>
      device.setCapabilityOptions(capabilityId, capabilityOptions),
    ),
  );
}

module.exports = {
  getCapabilityOptions,
  setCapabilityOptions,
};
