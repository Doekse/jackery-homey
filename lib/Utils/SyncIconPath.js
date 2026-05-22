'use strict';

/**
 * Pair-list battery icon reflects module count from MQTT `batNum` when discovery already saw status.
 *
 * @param {Object} [body] - Parsed MQTT status.
 * @returns {string} Path relative to driver assets.
 */
function getIconPath(body) {
  const raw = body?.batNum;
  const batNum = raw != null ? parseInt(String(raw), 10) : NaN;
  const count = Number.isNaN(batNum) ? 0 : Math.max(0, batNum);
  if (count >= 3) return '/models/battery-base-3.svg';
  if (count === 2) return '/models/battery-base-2.svg';
  if (count === 1) return '/models/battery-base-1.svg';
  return '/models/battery-base.svg';
}

module.exports = { getIconPath };
