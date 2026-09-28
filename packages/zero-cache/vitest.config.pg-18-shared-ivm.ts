import {mergeConfig} from 'vitest/config';
import {configForVersion} from './vitest.config.ts';

// The view-syncer suites again, with each view-syncer's pipelines reading a
// SharedSnapshot (the experimental `sharedIvmSnapshot`) rather than a
// Snapshotter of its own: advancements are diffed and pushed by the shared
// snapshot, in rounds, and handed to the view-syncer. The two modes must be
// indistinguishable to a view-syncer's clients. On the newest Postgres only,
// which is the one pull requests test against.
const merged = mergeConfig(configForVersion(18, import.meta.url), {
  test: {
    name: 'zero-cache/pg-18/shared-ivm-snapshot',
    env: {ZERO_TEST_SHARED_IVM_SNAPSHOT: '1'},
  },
});
// mergeConfig concatenates arrays; these need to replace.
merged.test.include = ['src/services/view-syncer/**/*.pg.test.ts'];
merged.test.exclude = [];
export default merged;
