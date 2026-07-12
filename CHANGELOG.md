<a name="Unreleased"></a>

# Unreleased

## Changes
- Added new `Fake` (in-memory) adapter for testing & development. It covers the same functionality as the Redis adapter, but stores the jobs only in the memory, so no Redis server is needed. The storage is shared between adapter instances with the same prefix, so multiple brokers within the same process can communicate with each other.
- Moved the common `serializeJob`, `deserializeJob`, `getBackoffTime` and `formatZrangeResultToObject` methods from `RedisAdapter` to `BaseAdapter`.
- Integration tests can be executed with the Fake adapter using the `WF_TEST_ADAPTER=Fake` environment variable (`npm run test:integration:fake`). CI runs the test suite with both adapters.
- Fixed adapter class resolution in `Adapters.resolve()` (the `{ type: Adapters.Redis }` form silently fell back to the default Redis adapter). The `options` property is optional now and the given class is validated.
- Fixed job locking: the adapters throw `WorkflowAlreadyLocked` error on lock contention, so the job processor skips the locked job instead of moving it to the failed queue.
- Updated dependencies. TypeScript stays on 6.x: TypeScript 7.0 ships without the Compiler API which `typescript-eslint` relies on (a new API is expected in TypeScript 7.1).

---
<a name="v0.2.1"></a>

# v0.2.1 (2026-03-29)

## Bug fixes
- Fixed invalid JSON in postbuild-generated `dist/cjs/package.json` and `dist/esm/package.json` (unquoted keys broke CJS imports).

---
<a name="v0.2.0"></a>

# v0.2.0 (2026-03-28)

## Breaking changes
- **Minimum Node.js version changed from 20 to 22.**

## Changes
- Updated Moleculer peer dependency to include released `^0.15.0`.
- Updated `moleculer-repl` to `^0.8.0` and `moleculer-web` to `^0.11.0`.
- Upgraded ESLint to v10 and TypeScript to v6.
- Removed unused `eslint-plugin-promise` and `eslint-plugin-node`.
- Updated all other dependencies to their latest versions.
- Fixed TypeScript 6 build configuration (`rootDir`, `ignoreDeprecations`).
- CI: Dropped Node.js 20 from test matrix.

---
<a name="v0.1.2"></a>

# v0.1.2 (2025-08-09)

## Changes
- Fixing types.
- Update dependencies.

---
<a name="v0.1.1"></a>

# v0.1.1 (2025-06-01)

## Changes
- Fixing build script.

---
<a name="v0.1.0"></a>

# v0.1.0 (2025-06-01)

First public version.
