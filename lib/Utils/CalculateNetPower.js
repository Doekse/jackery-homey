'use strict';

/**
 * Jackery sends `pvPw` as a number or nested object; HA uses the same shape in `_calculate_energy_flow`.
 *
 * @param {*} pvVal - `pvPw` field from status body.
 * @returns {number} Watts.
 */
function parsePvPower(pvVal) {
  if (pvVal == null) return 0;
  if (typeof pvVal === 'object' && !Array.isArray(pvVal)) {
    const nested = pvVal.pvPw ?? pvVal.w ?? pvVal.power ?? 0;
    return Number(nested) || 0;
  }
  return Number(pvVal) || 0;
}

/**
 * Signed battery power using the HA host balance (`calc_batt_net_power`): positive = charging.
 *
 * @param {Object} data - Merged MQTT status body.
 * @returns {number} Watts.
 */
function calculateBatteryNetPower(data) {
  const pv = parsePvPower(data.pvPw ?? 0);

  const ongridCharge = Number(data.inOngridPw) || 0;
  const ongridSupply = Number(data.outOngridPw) || 0;
  const pOng = ongridCharge - ongridSupply;

  const acIn = Number(data.swEpsInPw) || 0;
  const acOut = Number(data.swEpsOutPw) || 0;
  const pAc = acIn - acOut;

  return pv + pAc + pOng;
}

/**
 * Single signed EPS power (HA input minus output) for the `measure_power.eps` capability.
 *
 * @param {Object} data - Merged MQTT status body.
 * @returns {number} Watts.
 */
function calculateEpsNetPower(data) {
  const acIn = Number(data.swEpsInPw) || 0;
  const acOut = Number(data.swEpsOutPw) || 0;
  return acIn - acOut;
}

module.exports = {
  parsePvPower,
  calculateBatteryNetPower,
  calculateEpsNetPower,
};
