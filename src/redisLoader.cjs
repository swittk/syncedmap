'use strict';

/*
 * Keep this helper as a separate CommonJS file (not bundled into the ESM
 * output). Node's CJS resolver uses this file's own location to find the
 * optional peer dependency in both native ESM and CommonJS consumers.
 * A local-only SyncedMap never calls this loader or loads ioredis.
 */
module.exports = function loadOptionalRedis() {
  try {
    const loaded = require('ioredis');
    return loaded.default || loaded.Redis || loaded;
  } catch (error) {
    if (error && error.code === 'MODULE_NOT_FOUND' &&
        /['"]ioredis['"]/.test(String(error.message))) {
      throw new Error('Redis mode requires the optional peer packages ioredis and redlock.');
    }
    throw error;
  }
};
