import 'fake-indexeddb/auto';
import { test, expect, describe, vi } from 'vitest';
import { PersistedObject } from '../../../src/utils/PersistedObject';
import { IndexedDBStorage } from '../../../src';
import { randomUUID } from 'crypto';
import createLogger from '../../../src/utils/log';

async function idbSnapshot(idb, { includeMeta }) {
  const keys = await idb.getAllKeys();
  const res = {};
  for (const key of keys) {
    if (key === '__meta' && !includeMeta) {
      continue;
    }
    res[key] = await idb.getItem(key);
  }
  return res;
}

const devNullLogger = createLogger(false, () => ({}));

async function nextMillisecond() {
  const now = Date.now();
  while (Date.now() === now) {
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
}

test('PersistedObject saves values to storage', async () => {
  const idb = new IndexedDBStorage(randomUUID(), 'querySubs');
  const PO = new PersistedObject<string, string, string>({
    persister: idb,
    merge: (_k, storage, memory) => storage || memory || 'none',
    serialize: (_k, x) => x,
    parse: (_k, x) => x,
    objectSize: (_v) => 0,
    logger: devNullLogger,
    saveThrottleMs: 0,
    gc: {
      maxAgeMs: Number.MAX_SAFE_INTEGER,
      maxEntries: Number.MAX_SAFE_INTEGER,
      maxSize: Number.MAX_SAFE_INTEGER,
    },
  });

  PO.updateInPlace((prev) => {
    prev.a = 'b';
  });

  await PO.flush();

  const snapshot = await idbSnapshot(idb, { includeMeta: false });

  expect(snapshot).toStrictEqual({ a: 'b' });
});

test('PersistedObject merges existing values', async () => {
  const idb = new IndexedDBStorage(randomUUID(), 'querySubs');
  let storageV;
  let memoryV;
  const PO = new PersistedObject<string, string, string>({
    persister: idb,
    merge: (_k, storage, memory) => {
      storageV = storage;
      memoryV = memory;
      return 'merged-value';
    },
    serialize: (_k, x) => x,
    parse: (_k, x) => x,
    objectSize: (_v) => 0,
    logger: devNullLogger,
    saveThrottleMs: 0,
    gc: {
      maxAgeMs: Number.MAX_SAFE_INTEGER,
      maxEntries: Number.MAX_SAFE_INTEGER,
      maxSize: Number.MAX_SAFE_INTEGER,
    },
  });

  PO.updateInPlace((prev) => {
    prev.a = 'b';
  });

  await PO.flush();

  // We don't run merge if there was nothing in the store
  expect(storageV).toBeUndefined();
  expect(memoryV).toBeUndefined();
  expect(PO.currentValue.a).toEqual('b');

  const snapshot = await idbSnapshot(idb, { includeMeta: false });

  expect(snapshot).toStrictEqual({ a: 'b' });

  let storageV2;
  let memoryV2;
  const PO2 = new PersistedObject<string, string, string>({
    persister: idb,
    merge: (_k, storage, memory) => {
      storageV2 = storage;
      memoryV2 = memory;
      return 'merged-value-2';
    },
    serialize: (_k, x) => x,
    parse: (_k, x) => x,
    objectSize: (_v) => 0,
    logger: devNullLogger,
    saveThrottleMs: 0,
    gc: {
      maxAgeMs: Number.MAX_SAFE_INTEGER,
      maxEntries: Number.MAX_SAFE_INTEGER,
      maxSize: Number.MAX_SAFE_INTEGER,
    },
  });

  PO2.updateInPlace((prev) => {
    prev.a = 'c';
  });

  await PO2.flush();

  expect(storageV2).toEqual('b');
  expect(memoryV2).toEqual('c');
  expect(PO2.currentValue.a).toEqual('merged-value-2');

  const snapshot2 = await idbSnapshot(idb, { includeMeta: false });

  expect(snapshot2).toStrictEqual({ a: 'merged-value-2' });
});

test('PersistedObject notifies you when it loads a key from storage', async () => {
  const idb = new IndexedDBStorage(randomUUID(), 'querySubs');
  const PO = new PersistedObject({
    persister: idb,
    merge: (_k, _storage, _memory) => 'merged',
    serialize: (_k, x) => x,
    parse: (_k, x) => x,
    objectSize: () => 0,
    logger: devNullLogger,
    saveThrottleMs: 0,
    gc: {
      maxAgeMs: Number.MAX_SAFE_INTEGER,
      maxEntries: Number.MAX_SAFE_INTEGER,
      maxSize: Number.MAX_SAFE_INTEGER,
    },
  });
});

test('PersistedObject garbage collects when we exceed max items', async () => {
  const idb = new IndexedDBStorage(randomUUID(), 'querySubs');
  const PO = new PersistedObject({
    persister: idb,
    merge: (_k, storage, memory) => storage || memory || 'new',
    serialize: (_k, x) => x,
    parse: (_k, x) => x,
    objectSize: () => 0,
    logger: devNullLogger,
    saveThrottleMs: 0,
    gc: {
      maxAgeMs: Number.MAX_SAFE_INTEGER,
      maxEntries: 3,
      maxSize: Number.MAX_SAFE_INTEGER,
    },
  });

  for (const [i, k] of Object.entries(['a', 'b', 'c', 'd', 'e'])) {
    PO.updateInPlace((prev) => {
      prev[k] = parseInt(i, 10) + 1;
    });
    await PO.flush();
  }

  // @ts-expect-error: allow access to private fields for test
  await PO._gc();
  await PO.flush();

  const snapshot = await idbSnapshot(idb, { includeMeta: false });

  // We don't get rid of live keys
  expect(PO.currentValue).toStrictEqual({ a: 1, b: 2, c: 3, d: 4, e: 5 });
  expect(snapshot).toStrictEqual({
    a: 1,
    b: 2,
    c: 3,
    d: 4,
    e: 5,
  });

  PO.unloadKey('e');

  await PO.flush();

  // @ts-expect-error: allow access to private fields for test
  await PO._gc();
  await PO.flush();

  const snapshot2 = await idbSnapshot(idb, { includeMeta: false });

  // It does get rid of unloaded keys
  expect(PO.currentValue).toStrictEqual({ a: 1, b: 2, c: 3, d: 4 });
  expect(snapshot2).toStrictEqual({ a: 1, b: 2, c: 3, d: 4 });

  // Simulate a reload of the page
  const PO2 = new PersistedObject({
    persister: idb,
    merge: (_k, storage, memory) => storage || memory || 'new',
    serialize: (_k, x) => x,
    parse: (_k, x) => x,
    objectSize: () => 0,
    logger: devNullLogger,
    saveThrottleMs: 0,
    gc: {
      maxAgeMs: Number.MAX_SAFE_INTEGER,
      maxEntries: 3,
      maxSize: Number.MAX_SAFE_INTEGER,
    },
  });

  await PO2.waitForMetaToLoad();

  // @ts-expect-error: allow access to private fields for test
  await PO2._gc();
  await PO2.flush();
  const snapshot3 = await idbSnapshot(idb, { includeMeta: false });

  expect(snapshot3).toStrictEqual({ b: 2, c: 3, d: 4 });
});

test('PersistedObject garbage collects when we exceed max size', async () => {
  const idb = new IndexedDBStorage(randomUUID(), 'querySubs');
  const PO = new PersistedObject<string, number, number>({
    persister: idb,
    merge: (_k, storage, memory) => storage || memory || 0,
    serialize: (_k, x) => x,
    parse: (_k, x) => x,
    objectSize: (v) => v,
    logger: devNullLogger,
    saveThrottleMs: 0,
    gc: {
      maxAgeMs: Number.MAX_SAFE_INTEGER,
      maxEntries: Number.MAX_SAFE_INTEGER,
      maxSize: 100,
    },
  });

  for (const [i, k] of [
    [10, 'a'],
    [20, 'b'],
    [50, 'c'],
    [50, 'd'],
    [50, 'e'],
  ]) {
    PO.updateInPlace((prev) => {
      prev[k] = i as number;
    });
    await PO.flush();
  }

  // @ts-expect-error: allow access to private fields for test
  await PO._gc();
  await PO.flush();

  const snapshot = await idbSnapshot(idb, { includeMeta: false });

  // We don't get rid of live keys
  expect(PO.currentValue).toStrictEqual({ a: 10, b: 20, c: 50, d: 50, e: 50 });
  expect(snapshot).toStrictEqual({
    a: 10,
    b: 20,
    c: 50,
    d: 50,
    e: 50,
  });

  PO.unloadKey('e');

  await PO.flush();

  // @ts-expect-error: allow access to private fields for test
  await PO._gc();
  await PO.flush();

  const snapshot2 = await idbSnapshot(idb, { includeMeta: false });

  // It does get rid of unloaded keys
  expect(PO.currentValue).toStrictEqual({ a: 10, b: 20, c: 50, d: 50 });
  expect(snapshot2).toStrictEqual({ a: 10, b: 20, c: 50, d: 50 });

  // Simulate a reload of the page
  const PO2 = new PersistedObject<string, number, number>({
    persister: idb,
    merge: (_k, storage, memory) => storage || memory || 0,
    serialize: (_k, x) => x,
    parse: (_k, x) => x,
    objectSize: (v) => v,
    logger: devNullLogger,
    saveThrottleMs: 0,
    gc: {
      maxAgeMs: Number.MAX_SAFE_INTEGER,
      maxEntries: Number.MAX_SAFE_INTEGER,
      maxSize: 100,
    },
  });

  await PO2.waitForMetaToLoad();

  // @ts-expect-error: allow access to private fields for test
  await PO2._gc();
  await PO.flush();
  const snapshot3 = await idbSnapshot(idb, { includeMeta: false });

  expect(snapshot3).toStrictEqual({ c: 50, d: 50 });
});

test('PersistedObject garbage collects when we exceed max age', async () => {
  const idb = new IndexedDBStorage(randomUUID(), 'querySubs');
  const PO = new PersistedObject<string, number, number>({
    persister: idb,
    merge: (_k, storage, memory) => storage || memory || 0,
    serialize: (_k, x) => x,
    parse: (_k, x) => x,
    objectSize: (v) => v,
    logger: createLogger(true, () => ({})),
    saveThrottleMs: 0,
    gc: {
      maxAgeMs: 0,
      maxEntries: Number.MAX_SAFE_INTEGER,
      maxSize: Number.MAX_SAFE_INTEGER,
    },
  });

  for (const [i, k] of [
    [10, 'a'],
    [20, 'b'],
    [50, 'c'],
    [50, 'd'],
    [50, 'e'],
  ]) {
    PO.updateInPlace((prev) => {
      prev[k] = i as number;
    });
    await PO.flush();
  }

  // @ts-expect-error: allow access to private fields for test
  await PO._gc();
  await PO.flush();

  const snapshot = await idbSnapshot(idb, { includeMeta: false });

  // We don't get rid of live keys
  expect(PO.currentValue).toStrictEqual({ a: 10, b: 20, c: 50, d: 50, e: 50 });
  expect(snapshot).toStrictEqual({
    a: 10,
    b: 20,
    c: 50,
    d: 50,
    e: 50,
  });

  PO.unloadKey('e');

  await PO.flush();

  // A key saved in this millisecond has not exceeded the max age
  await nextMillisecond();

  // @ts-expect-error: allow access to private fields for test
  await PO._gc();

  await PO.flush();

  const snapshot2 = await idbSnapshot(idb, { includeMeta: false });

  // It does get rid of unloaded keys
  expect(PO.currentValue).toStrictEqual({ a: 10, b: 20, c: 50, d: 50 });
  expect(snapshot2).toStrictEqual({ a: 10, b: 20, c: 50, d: 50 });

  // Simulate a reload of the page
  const PO2 = new PersistedObject<string, number, number>({
    persister: idb,
    merge: (_k, storage, memory) => storage || memory || 0,
    serialize: (_k, x) => x,
    parse: (_k, x) => x,
    objectSize: (v) => v,
    logger: devNullLogger,
    saveThrottleMs: 0,
    gc: {
      maxAgeMs: 0,
      maxEntries: Number.MAX_SAFE_INTEGER,
      maxSize: Number.MAX_SAFE_INTEGER,
    },
  });

  await PO2.waitForMetaToLoad();

  // @ts-expect-error: allow access to private fields for test
  await PO2._gc();
  await PO2.flush();
  const snapshot3 = await idbSnapshot(idb, { includeMeta: false });

  expect(snapshot3).toStrictEqual({});
});

// ---------------------------------------------------------------------------
// A key unloaded while its save waits

/** Strings saved a second after they change, or at the next flush. */
function stringsIn(idb, maxEntries = Number.MAX_SAFE_INTEGER) {
  return new PersistedObject<string, string, string>({
    persister: idb,
    merge: (_k, storage, memory) => memory ?? storage,
    serialize: (_k, x) => x,
    parse: (_k, x) => x,
    objectSize: (x) => x.length,
    logger: devNullLogger,
    saveThrottleMs: 1000,
    gc: {
      maxAgeMs: Number.MAX_SAFE_INTEGER,
      maxEntries,
      maxSize: Number.MAX_SAFE_INTEGER,
    },
  });
}

async function save(PO, k, v) {
  await PO.waitForMetaToLoad();
  PO.updateInPlace((prev) => {
    prev[k] = v;
  });
  await PO.waitForKeyToLoad(k);
  await PO.flush();
}

test('PersistedObject saves the value of a key that is unloaded before its save', async () => {
  const idb = new IndexedDBStorage(randomUUID(), 'querySubs');
  const PO = stringsIn(idb);
  await save(PO, 'a', 'first');

  PO.updateInPlace((prev) => {
    prev.a = 'the second';
  });
  PO.unloadKey('a');
  await PO.flush();

  expect(PO.currentValue).toStrictEqual({});
  const snapshot = await idbSnapshot(idb, { includeMeta: true });
  expect(snapshot.a).toBe('the second');
  expect(snapshot.__meta.objects.a.size).toBe(10);
});

test('PersistedObject loads the newest value of a key that was unloaded before its save', async () => {
  const idb = new IndexedDBStorage(randomUUID(), 'querySubs');
  const PO = stringsIn(idb);
  await save(PO, 'a', 'first');

  PO.updateInPlace((prev) => {
    prev.a = 'second';
  });
  PO.unloadKey('a');

  expect(await PO.waitForKeyToLoad('a')).toBe('second');
});

test('PersistedObject leaves storage as it is when a key is unloaded before it has loaded', async () => {
  const idb = new IndexedDBStorage(randomUUID(), 'querySubs');
  await save(stringsIn(idb), 'a', 'first');

  const PO = stringsIn(idb);
  await PO.waitForMetaToLoad();
  PO.updateInPlace((prev) => {
    prev.a = 'second';
  });
  PO.unloadKey('a');
  await PO.flush();

  const snapshot = await idbSnapshot(idb, { includeMeta: true });
  expect(snapshot.a).toBe('first');
  expect(snapshot.__meta.objects.a.size).toBe(5);
  expect(PO.currentValue).toStrictEqual({});
});

test('PersistedObject leaves storage as it is when a key is unloaded before the meta has loaded', async () => {
  const idb = new IndexedDBStorage(randomUUID(), 'querySubs');
  await save(stringsIn(idb), 'a', 'first');

  const PO = stringsIn(idb);
  PO.updateInPlace((prev) => {
    prev.a = 'second';
  });
  PO.unloadKey('a');
  await PO.waitForMetaToLoad();
  await PO.flush();

  const snapshot = await idbSnapshot(idb, { includeMeta: true });
  expect(snapshot.a).toBe('first');
  expect(snapshot.__meta.objects.a.size).toBe(5);
});

test('PersistedObject removes a deleted key from storage when it is unloaded and loaded again', async () => {
  const idb = new IndexedDBStorage(randomUUID(), 'querySubs');
  const PO = stringsIn(idb);
  await save(PO, 'a', 'first');

  PO.updateInPlace((prev) => {
    delete prev.a;
  });
  PO.unloadKey('a');

  expect(await PO.waitForKeyToLoad('a')).toBeUndefined();
  await PO.flush();
  const snapshot = await idbSnapshot(idb, { includeMeta: true });
  expect(snapshot.a).toBeUndefined();
  expect(snapshot.__meta.objects).toStrictEqual({});
});

test('PersistedObject clears a key that was unloaded before its save', async () => {
  const idb = new IndexedDBStorage(randomUUID(), 'querySubs');
  const PO = stringsIn(idb);
  await save(PO, 'a', 'first');
  await save(PO, 'b', 'kept');

  PO.updateInPlace((prev) => {
    prev.a = 'second';
  });
  PO.unloadKey('a');
  await PO.clearUnloadedKeys();
  await PO.flush();

  const snapshot = await idbSnapshot(idb, { includeMeta: true });
  expect(snapshot.a).toBeUndefined();
  expect(Object.keys(snapshot.__meta.objects)).toStrictEqual(['b']);
});

test('PersistedObject garbage collects a key that was saved as it was unloaded', async () => {
  const idb = new IndexedDBStorage(randomUUID(), 'querySubs');
  const PO = stringsIn(idb, 1);
  await save(PO, 'a', 'first');
  await save(PO, 'b', 'live');

  PO.updateInPlace((prev) => {
    prev.a = 'second';
  });
  PO.unloadKey('a');

  // @ts-expect-error: allow access to private fields for test
  await PO._gc();
  await PO.flush();

  const snapshot = await idbSnapshot(idb, { includeMeta: true });
  expect(snapshot.a).toBeUndefined();
  expect(Object.keys(snapshot.__meta.objects)).toStrictEqual(['b']);
});

// ---------------------------------------------------------------------------
// A key unloaded while it is read from storage

test('PersistedObject does not load a key that is unloaded while it is read', async () => {
  const idb = new IndexedDBStorage(randomUUID(), 'querySubs');
  await save(stringsIn(idb), 'a', 'first');

  const PO = stringsIn(idb);
  const loaded = [];
  PO.onKeyLoaded = (k) => loaded.push(k);
  const waiter = PO.waitForKeyToLoad('a');
  PO.unloadKey('a');

  expect(await waiter).toBeUndefined();
  expect(PO.currentValue).toStrictEqual({});
  expect(loaded).toStrictEqual([]);

  // the next load reads it
  expect(await PO.waitForKeyToLoad('a')).toBe('first');
  expect(loaded).toStrictEqual(['a']);
});

test('PersistedObject loads a key once when it is unloaded and loaded again while it is read', async () => {
  const idb = new IndexedDBStorage(randomUUID(), 'querySubs');
  await save(stringsIn(idb), 'a', 'first');

  const PO = stringsIn(idb);
  const loaded = [];
  PO.onKeyLoaded = (k) => loaded.push(k);
  const firstWaiter = PO.waitForKeyToLoad('a');

  // storage changes between the two reads, as another tab changes it
  const stored = idb.setItem('a', 'second');
  PO.unloadKey('a');
  const secondWaiter = PO.waitForKeyToLoad('a');

  expect(await firstWaiter).toBe('second');
  expect(await secondWaiter).toBe('second');
  await stored;
  expect(PO.currentValue).toStrictEqual({ a: 'second' });
  expect(loaded).toStrictEqual(['a']);
});

test('IndexedDBStorage recovers when the database connection closes', async () => {
  const idb = new IndexedDBStorage(randomUUID(), 'kv');

  await idb.setItem('key1', 'value1');
  expect(await idb.getItem('key1')).toBe('value1');

  // Simulate the browser closing the connection unexpectedly.
  // Remove the onclose handler so recovery relies entirely on _withRetry
  // catching the InvalidStateError (the same codepath as the original bug).
  const db = await idb._dbPromise;
  db.onclose = null;
  db.close();

  // All operations should recover transparently
  await idb.setItem('key2', 'value2');
  expect(await idb.getItem('key1')).toBe('value1');
  expect(await idb.getItem('key2')).toBe('value2');

  await idb.multiSet([
    ['key3', 'value3'],
    ['key4', 'value4'],
  ]);
  expect(await idb.getItem('key3')).toBe('value3');
  expect(await idb.getItem('key4')).toBe('value4');

  const keys = await idb.getAllKeys();
  expect(keys.sort()).toStrictEqual(['key1', 'key2', 'key3', 'key4']);

  await idb.removeItem('key4');
  expect(await idb.getItem('key4')).toBe(null);
});

test('IndexedDBStorage explicitly commits write transactions', async () => {
  const commitSpy = vi.spyOn(IDBTransaction.prototype, 'commit');
  try {
    const idb = new IndexedDBStorage(randomUUID(), 'kv');

    await idb.setItem('key1', 'value1');
    await idb.multiSet([
      ['key2', 'value2'],
      ['key3', 'value3'],
    ]);
    await idb.removeItem('key3');

    expect(commitSpy).toHaveBeenCalledTimes(3);
    expect(await idb.getItem('key1')).toBe('value1');
    expect(await idb.getItem('key2')).toBe('value2');
    expect(await idb.getItem('key3')).toBe(null);
  } finally {
    commitSpy.mockRestore();
  }
});
