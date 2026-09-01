#!/usr/bin/env node

import '../../shared/src/dotenv.ts';

import {colorConsole, createLogContext} from '../../shared/src/logging.ts';
import {parseOptions} from '../../shared/src/options.ts';
import {ZERO_ENV_VAR_PREFIX as envNamePrefix} from '../../zero-cache/src/config/zero-config.ts';
import {
  archiveDrillOptions,
  drillArchive,
} from '../../zero-cache/src/scripts/archive-drill.ts';

async function main() {
  const config = parseOptions(archiveDrillOptions, {envNamePrefix});
  const lc = createLogContext(config);
  const result = await drillArchive(lc, config);
  colorConsole.log(JSON.stringify(result, null, 2));
  process.exit(result.outcome === 'match' ? 0 : 1);
}

void main();
