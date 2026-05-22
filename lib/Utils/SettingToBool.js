'use strict';

/**
 * Homey checkboxes and Jackery MQTT use different boolean encodings.
 *
 * @param {boolean|number|string|undefined|null} raw
 * @returns {boolean}
 */
function SettingToBool(raw) {
  return raw === true || parseInt(String(raw), 10) === 1;
}

module.exports = {
  SettingToBool,
};
