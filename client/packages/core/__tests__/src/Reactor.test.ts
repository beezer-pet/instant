// https://www.npmjs.com/package/fake-indexeddb
import 'fake-indexeddb/auto';
import { test, expect } from 'vitest';
import { setTimeout as sleep } from 'timers/promises';

import IndexedDBStorage from '../../src/IndexedDBStorage';

import Reactor from '../../src/Reactor';
import InMemoryStorage from '../../src/InMemoryStorage';
import * as instaml from '../../src/instaml';
import * as instatx from '../../src/instatx';
import zenecaAttrs from './data/zeneca/attrs.json';
import zenecaTriples from './data/zeneca/triples.json';
import uuid from '../../src/utils/id';
import { weakHash } from '../../src';
import { AttrsStoreClass } from '../../src/store';

const zenecaAttrsStore = new AttrsStoreClass(
  zenecaAttrs.reduce((res, x) => {
    res[x.id] = x;
    return res;
  }, {}),
  null,
);

async function waitForLoaded(reactor) {
  await reactor.querySubs.waitForMetaToLoad();
  await reactor.kv.waitForMetaToLoad();
  await reactor.kv.waitForKeyToLoad('pendingMutations');
  await reactor.querySubs.flush();
  await reactor.kv.flush();
}

test('querySubs round-trips', async () => {
  const appId = uuid();
  const reactor = new Reactor({ appId });
  reactor._initStorage(IndexedDBStorage);
  reactor._setAttrs(zenecaAttrs);
  const q = { users: {} };

  await waitForLoaded(reactor);

  const resultOne = new Promise((resolve, reject) => {
    reactor.subscribeQuery(q, (res) => {
      if (res.error) {
        reject(res.error);
      }
      resolve(res);
    });
  });

  // Initialize the store
  reactor._handleReceive(0, {
    op: 'add-query-ok',
    q,
    'processed-tx-id': 0,
    result: [
      {
        data: {
          'datalog-result': {
            'join-rows': [zenecaTriples],
          },
        },
        'child-nodes': [],
      },
    ],
  });

  const data1 = await resultOne;

  // Make sure the store has the data we expect
  expect(data1.data.users.map((x) => x.handle)).toEqual([
    'joe',
    'alex',
    'stopa',
    'nicolegf',
  ]);

  await waitForLoaded(reactor);

  // Create a new reactor
  const reactor2 = new Reactor({ appId });
  reactor2._initStorage(IndexedDBStorage);
  reactor2._setAttrs(zenecaAttrs);

  await waitForLoaded(reactor2);
  await reactor2.querySubs.waitForKeyToLoad(weakHash(q));

  // Check that it pull the data from indexedDB
  const res = await new Promise((resolve, reject) => {
    reactor2.subscribeQuery(q, (res) => {
      if (res.error) {
        reject(res.error);
      }
      resolve(res);
    });
  });

  expect(res.data.users.map((x) => x.handle)).toEqual([
    'joe',
    'alex',
    'stopa',
    'nicolegf',
  ]);
});

test('rewrite mutations', () => {
  const appId = uuid();
  const reactor = new Reactor({ appId });

  const bookId = 'bookId';
  const bookshelfId = 'bookshelfId';
  const ops = [
    instatx.tx.books[bookId].update({ title: 'title' }),
    instatx.tx.users[instatx.lookup('handle', 'stopa')].update({
      email: 's@example.com',
    }),
    instatx.tx.bookshelves[bookshelfId].link({
      users: { handle: 'stopa' },
    }),
    instatx.tx.bookshelves[bookshelfId].unlink({
      users: ['handle', 'joe'],
    }),
    instatx.tx.bookshelves[bookshelfId].unlink({
      users: instatx.lookup('handle', 'joe'),
    }),
  ];

  // create transactions without any attributes
  const optimisticSteps = instaml.transform(
    { attrsStore: new AttrsStoreClass({}, null) },
    ops,
  );

  const mutations = new Map([['k', { 'tx-steps': optimisticSteps }]]);

  const rewrittenWithoutAttrs = reactor
    ._rewriteMutations(new AttrsStoreClass({}, null), mutations)
    .get('k')['tx-steps'];

  // Check that we didn't clobber anything in our rewrite
  expect(rewrittenWithoutAttrs).toEqual(optimisticSteps);

  // rewrite them with the new server attributes
  const rewrittenSteps = reactor
    ._rewriteMutations(zenecaAttrsStore, mutations)
    .get('k')['tx-steps'];

  const serverSteps = instaml.transform({ attrsStore: zenecaAttrsStore }, ops);
  expect(rewrittenSteps).toEqual(serverSteps);
});

test('rewrite mutations works with multiple transactions', () => {
  const appId = uuid();
  const reactor = new Reactor({ appId });
  reactor._initStorage(InMemoryStorage);

  const bookId = 'bookId';
  const bookshelfId = 'bookshelfId';
  const ops = [
    instatx.tx.books[bookId].update({ title: 'title' }),
    instatx.tx.users[instatx.lookup('handle', 'stopa')].update({
      email: 's@example.com',
    }),
    instatx.tx.bookshelves[bookshelfId].link({
      users: { handle: 'stopa' },
    }),
    instatx.tx.bookshelves[bookshelfId].unlink({
      users: ['handle', 'joe'],
    }),
    instatx.tx.bookshelves[bookshelfId].unlink({
      users: instatx.lookup('handle', 'joe'),
    }),
  ];

  const keys = ['a', 'b', 'c', 'd'];

  for (const k of keys) {
    const attrs = reactor.optimisticAttrs();
    const steps = instaml.transform({ attrsStore: attrs }, ops);
    const mut = {
      op: 'transact',
      'tx-steps': steps,
    };
    reactor._updatePendingMutations((prev) => {
      prev.set(k, mut);
      return prev;
    });
  }

  // rewrite them with the new server attributes
  const rewrittenMutations = reactor._rewriteMutations(
    zenecaAttrsStore,
    reactor._pendingMutations(),
  );

  const serverSteps = instaml.transform({ attrsStore: zenecaAttrsStore }, ops);
  for (const k of keys) {
    expect(rewrittenMutations.get(k)['tx-steps']).toEqual(serverSteps);
  }
});

test('optimisticTx is not overwritten by refresh-ok', async () => {
  const appId = uuid();
  const reactor = new Reactor({ appId });
  reactor._initStorage(IndexedDBStorage);
  reactor._setAttrs(zenecaAttrs);
  const q = { users: {} };
  const joe_id = 'ce942051-2d74-404a-9c7d-4aa3f2d54ae4';

  await waitForLoaded(reactor);

  let data = null;

  reactor.subscribeQuery(q, (res) => {
    data = res;
  });

  // Initialize the store
  reactor._handleReceive(0, {
    op: 'add-query-ok',
    q,
    'processed-tx-id': 0,
    result: [
      {
        data: {
          'datalog-result': {
            'join-rows': [zenecaTriples],
          },
        },
        'child-nodes': [],
      },
    ],
  });

  await reactor.querySubs.flush();

  expect(data.data.users.map((x) => x.handle)).toEqual([
    'joe',
    'alex',
    'stopa',
    'nicolegf',
  ]);

  // first optimistic
  const ops2 = [
    instatx.tx.users[joe_id].update({
      handle: 'joe2',
    }),
  ];

  reactor.pushTx(ops2);

  await reactor.querySubs.flush();

  expect(data.data.users.map((x) => x.handle)).toEqual([
    'joe2',
    'alex',
    'stopa',
    'nicolegf',
  ]);

  const [eventId2] = reactor._pendingMutations().keys();

  // second optimistic
  const ops3 = [
    instatx.tx.users[joe_id].update({
      handle: 'joe3',
    }),
  ];

  reactor.pushTx(ops3);

  await reactor.querySubs.flush();

  expect(data.data.users.map((x) => x.handle)).toEqual([
    'joe3',
    'alex',
    'stopa',
    'nicolegf',
  ]);

  const [_, eventId3] = reactor._pendingMutations().keys();

  // confirmation from server for first optimistic
  reactor._handleReceive(1, {
    op: 'transact-ok',
    'client-event-id': eventId2,
    'tx-id': 100,
  });

  await waitForLoaded(reactor);

  expect(data.data.users.map((x) => x.handle)).toEqual([
    'joe3',
    'alex',
    'stopa',
    'nicolegf',
  ]);

  // query refresh after first tx
  reactor._handleReceive(3, {
    op: 'refresh-ok',
    'processed-tx-id': 100,
    attrs: zenecaAttrs,
    computations: [
      {
        'instaql-query': q,
        'instaql-result': [
          {
            data: {
              'datalog-result': {
                'join-rows': [zenecaTriples],
              },
            },
            'child-nodes': [],
          },
        ],
      },
    ],
  });

  // make sure it doesn’t override local results
  await waitForLoaded(reactor);

  expect(data.data.users.map((x) => x.handle)).toEqual([
    'joe3',
    'alex',
    'stopa',
    'nicolegf',
  ]);

  // confirmation from server for second optimistic
  reactor._handleReceive(2, {
    op: 'transact-ok',
    'client-event-id': eventId3,
    'tx-id': 101,
  });

  await waitForLoaded(reactor);

  expect(data.data.users.map((x) => x.handle)).toEqual([
    'joe3',
    'alex',
    'stopa',
    'nicolegf',
  ]);

  // make sure it still doesn’t override local results
  await waitForLoaded(reactor);

  expect(data.data.users.map((x) => x.handle)).toEqual([
    'joe3',
    'alex',
    'stopa',
    'nicolegf',
  ]);
});

test('a subscription is told of a change in a date alone', async () => {
  const joe_id = 'ce942051-2d74-404a-9c7d-4aa3f2d54ae4';
  const createdAtId = '2ffdf0fc-1561-4fc5-96db-2210a41adfa6';
  const attrs = zenecaAttrs.map((attr) =>
    attr.id === createdAtId ? { ...attr, 'checked-data-type': 'date' } : attr,
  );
  const reactor = new Reactor({ appId: uuid(), useDateObjects: true });
  reactor._initStorage(IndexedDBStorage);
  reactor._setAttrs(attrs);
  const q = { users: {} };

  await waitForLoaded(reactor);

  const seen = [];
  reactor.subscribeQuery(q, (res) => {
    seen.push(res.data.users.find((user) => user.id === joe_id).createdAt);
  });

  // The store writes its dates into the triples it is given.
  const result = (triples) => [
    {
      data: {
        'datalog-result': {
          'join-rows': [triples.map((triple) => [...triple])],
        },
      },
      'child-nodes': [],
    },
  ];

  reactor._handleReceive(0, {
    op: 'add-query-ok',
    q,
    'processed-tx-id': 0,
    result: result(zenecaTriples),
  });
  await reactor.querySubs.flush();

  const refresh = (createdAt) =>
    reactor._handleReceive(1, {
      op: 'refresh-ok',
      'processed-tx-id': 1,
      attrs,
      computations: [
        {
          'instaql-query': q,
          'instaql-result': result(
            zenecaTriples.map((triple) =>
              triple[0] === joe_id && triple[1] === createdAtId
                ? [triple[0], triple[1], createdAt, triple[3]]
                : triple,
            ),
          ),
        },
      ],
    });

  refresh('2021-01-14 18:51:23.742637');
  await waitForLoaded(reactor);
  // the same moment again is no change
  refresh('2021-01-14 18:51:23.742637');
  await waitForLoaded(reactor);

  expect(seen).toEqual([
    new Date('2021-01-07T18:51:23.742Z'),
    new Date('2021-01-14T18:51:23.742Z'),
  ]);
});

test("we don't cleanup mutations we're still waiting on", async () => {
  const appId = uuid();
  const reactor = new Reactor({
    appId,
    pendingTxCleanupTimeout: 1,
    pendingMutationCleanupThreshold: 0,
  });

  reactor._initStorage(IndexedDBStorage);
  reactor._setAttrs(zenecaAttrs);
  const q = { users: {} };
  const joe_id = 'ce942051-2d74-404a-9c7d-4aa3f2d54ae4';

  await waitForLoaded(reactor);

  // Add two transactions
  const ops1 = [
    instatx.tx.users[joe_id].update({
      handle: 'joe2',
    }),
  ];

  reactor.pushTx(ops1);

  const ops2 = [
    instatx.tx.users[joe_id].update({
      handle: 'joe2',
    }),
  ];

  reactor.pushTx(ops2);

  await reactor.kv.flush();

  const [ev1, ev2] = reactor._pendingMutations().keys();

  // Mark one as received
  reactor._handleReceive(1, {
    op: 'transact-ok',
    'client-event-id': ev1,
    'tx-id': 100,
  });

  // Wait for the pendingTxCleanupTimeout to expire
  await sleep(10);

  // Cleanup shouldn't remove the mutation we're still waiting on
  await reactor._cleanupPendingMutationsTimeout();

  const remainingKeys = new Array(...reactor._pendingMutations().keys());

  expect(remainingKeys).toStrictEqual([ev2]);
});

test('getLocalId always returns the same id', async () => {
  const appId = uuid();

  const idbs = new Map();

  // Class that reuses the same underlying storage for indexeddb.
  // This will make fake-indexeddb act more like the browser, otherwise
  // we get a fresh in-memory store each time we call indexedDB.open
  class IDBStorage {
    constructor(appId, dbName) {
      if (idbs.get(dbName)) {
        return idbs.get(dbName);
      }
      const idb = new IndexedDBStorage(appId, dbName);
      idbs.set(dbName, idb);
      return idb;
    }
  }

  const reactor = new Reactor({
    appId,
    pendingTxCleanupTimeout: 1,
    pendingMutationCleanupThreshold: 0,
  });

  reactor._initStorage(IDBStorage);

  const promises = [];

  const ids = new Set();

  for (const _ of [...new Array(1000)]) {
    const p = reactor.getLocalId('id').then((id) => ids.add(id));
    promises.push(p);
  }

  await Promise.all(promises);

  expect(ids.size).toBe(1);
  await reactor.querySubs.flush();

  await reactor.kv.flush();

  const reactor2 = new Reactor({
    appId,
    pendingTxCleanupTimeout: 1,
    pendingMutationCleanupThreshold: 0,
  });

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

  reactor2._initStorage(IDBStorage);

  const id = await reactor2.getLocalId('id');

  expect(id).toStrictEqual([...ids][0]);
});

// ---------------------------------------------------------------------------
// A transaction the server refuses

const joeId = 'ce942051-2d74-404a-9c7d-4aa3f2d54ae4';
const handleAttrId = '6a089759-2a2f-4898-9bb8-a7bc9f6f791a';
const usersQ = { users: {} };

function usersResult(triples) {
  return [
    {
      data: { 'datalog-result': { 'join-rows': [triples] } },
      'child-nodes': [],
    },
  ];
}

/** zeneca's triples once another writer has given joe the handle `handle`. */
function triplesWithJoeAs(handle) {
  return zenecaTriples.map((t) =>
    t[0] === joeId && t[1] === handleAttrId ? [t[0], t[1], handle, t[3]] : t,
  );
}

/** A reactor subscribed to `usersQ`, and every answer its subscriber got. */
async function reactorOnUsers(appId = uuid()) {
  const reactor = new Reactor({ appId });
  reactor._initStorage(IndexedDBStorage);
  reactor._setAttrs(zenecaAttrs);
  await waitForLoaded(reactor);
  await reactor.querySubs.waitForKeyToLoad(weakHash(usersQ));
  const seen = [];
  const close = reactor.subscribeQuery(usersQ, (res) =>
    seen.push(res.data.users),
  );
  await waitForLoaded(reactor);
  return { reactor, seen, close };
}

async function answerUsers(reactor, triples, processedTxId) {
  reactor._handleReceive(0, {
    op: 'add-query-ok',
    q: usersQ,
    'processed-tx-id': processedTxId,
    result: usersResult(triples),
  });
  await waitForLoaded(reactor);
}

async function refreshUsers(reactor, triples, processedTxId) {
  reactor._handleReceive(0, {
    op: 'refresh-ok',
    'processed-tx-id': processedTxId,
    attrs: zenecaAttrs,
    computations: [
      { 'instaql-query': usersQ, 'instaql-result': usersResult(triples) },
    ],
  });
  await waitForLoaded(reactor);
}

/** The pending transactions' event ids, in the order they were made. */
function pendingEventIds(reactor) {
  return [...reactor._pendingMutations().entries()]
    .sort(([, a], [, b]) => a.order - b.order)
    .map(([eventId]) => eventId);
}

async function refuse(reactor, eventId) {
  // The transaction's promise resolved as `enqueued`, so the reactor logs
  // the refusal for want of a listener.
  const error = console.error;
  console.error = () => {};
  try {
    reactor._handleReceive(0, {
      op: 'error',
      status: 400,
      type: 'permission-denied',
      message: 'Permission denied: not perms-pass?',
      'client-event-id': eventId,
    });
  } finally {
    console.error = error;
  }
  await waitForLoaded(reactor);
}

const joeOf = (users) => users.find((user) => user.id === joeId);
const handlesOf = (users) => users.map((user) => user.handle);

test('a refused transaction leaves nothing in an answer that arrived while it was pending', async () => {
  const { reactor, seen } = await reactorOnUsers();
  await answerUsers(reactor, zenecaTriples, 1);

  reactor.pushTx(instatx.tx.users[joeId].update({ handle: 'mine' }));
  await waitForLoaded(reactor);
  const [eventId] = pendingEventIds(reactor);

  // another writer changes the same record before the server refuses ours
  await refreshUsers(reactor, triplesWithJoeAs('theirs'), 2);
  await refuse(reactor, eventId);

  expect(seen.map((users) => joeOf(users).handle)).toEqual([
    'joe',
    'mine',
    'theirs',
  ]);
  expect(reactor._pendingMutations().size).toBe(0);
});

test('a transaction refused after a reload leaves nothing in the answer read back from storage', async () => {
  const appId = uuid();
  const { reactor } = await reactorOnUsers(appId);
  await answerUsers(reactor, zenecaTriples, 1);

  reactor.pushTx(instatx.tx.users[joeId].update({ handle: 'mine' }));
  await waitForLoaded(reactor);
  const [eventId] = pendingEventIds(reactor);
  await refreshUsers(reactor, triplesWithJoeAs('theirs'), 2);

  // a second reactor reads the answer and the pending transaction back
  const { reactor: reloaded, seen } = await reactorOnUsers(appId);
  expect(pendingEventIds(reloaded)).toEqual([eventId]);
  expect(joeOf(seen.at(-1)).handle).toEqual('mine');

  await refuse(reloaded, eventId);

  expect(joeOf(seen.at(-1)).handle).toEqual('theirs');
});

test('of two pending transactions on one record, a refusal takes back the refused one alone', async () => {
  const { reactor, seen } = await reactorOnUsers();
  await answerUsers(reactor, zenecaTriples, 1);

  reactor.pushTx(
    instatx.tx.users[joeId].update({ handle: 'first', fullName: 'First' }),
  );
  reactor.pushTx(instatx.tx.users[joeId].update({ handle: 'second' }));
  await waitForLoaded(reactor);
  const [first, second] = pendingEventIds(reactor);

  await refreshUsers(reactor, zenecaTriples, 2);
  await refuse(reactor, first);

  expect(joeOf(seen.at(-1))).toMatchObject({
    handle: 'second',
    fullName: 'Joe Averbukh',
  });
  expect(pendingEventIds(reactor)).toEqual([second]);
});

test('a create the server refused is not sent as an update afterwards', async () => {
  const { reactor, seen } = await reactorOnUsers();
  await answerUsers(reactor, zenecaTriples, 1);

  const newId = uuid();
  const create = () =>
    reactor.pushTx(instatx.tx.users[newId].update({ handle: 'new' }));

  create();
  await waitForLoaded(reactor);
  const [firstSend] = pendingEventIds(reactor);
  expect(handlesOf(seen.at(-1))).toContain('new');

  // an answer arrives while the create is on its way
  await refreshUsers(reactor, triplesWithJoeAs('theirs'), 2);
  await refuse(reactor, firstSend);

  expect(handlesOf(seen.at(-1))).toEqual([
    'theirs',
    'alex',
    'stopa',
    'nicolegf',
  ]);

  create();
  await waitForLoaded(reactor);
  const [secondSend] = pendingEventIds(reactor);
  const secondSteps = reactor._pendingMutations().get(secondSend)['tx-steps'];

  expect(secondSteps.map((step) => step[4]?.mode)).not.toContain('update');
});

test('an accepted transaction is delivered once', async () => {
  const { reactor, seen } = await reactorOnUsers();
  await answerUsers(reactor, zenecaTriples, 1);

  const newId = uuid();
  const idAttrId = '5a4f5a6d-ba83-4bf0-ae78-27a863300224';
  const createdAt = Date.now();
  reactor.pushTx([
    instatx.tx.users[joeId].update({ handle: 'mine' }),
    instatx.tx.users[newId].update({ handle: 'new' }),
  ]);
  await waitForLoaded(reactor);
  const [eventId] = pendingEventIds(reactor);

  // an answer from before the transaction, with nothing changed
  await refreshUsers(reactor, zenecaTriples, 2);

  reactor._handleReceive(0, {
    op: 'transact-ok',
    'client-event-id': eventId,
    'tx-id': 3,
  });
  await waitForLoaded(reactor);

  await refreshUsers(
    reactor,
    [
      ...triplesWithJoeAs('mine'),
      [newId, idAttrId, newId, createdAt],
      [newId, handleAttrId, 'new', createdAt],
    ],
    3,
  );

  expect(seen.map(handlesOf)).toEqual([
    ['joe', 'alex', 'stopa', 'nicolegf'],
    ['mine', 'alex', 'stopa', 'nicolegf', 'new'],
  ]);
  expect(reactor._pendingMutations().size).toBe(0);
});

// ---------------------------------------------------------------------------
// A query subscribed again after the server accepted a transaction

const booksQ = { books: {} };

async function accept(reactor, eventId, txId) {
  reactor._handleReceive(0, {
    op: 'transact-ok',
    'client-event-id': eventId,
    'tx-id': txId,
  });
  await waitForLoaded(reactor);
}

/**
 * The answer of another open query. With it every open query has processed
 * the transactions up to `processedTxId`, and the reactor drops them.
 */
async function answerBooks(reactor, processedTxId) {
  reactor._handleReceive(0, {
    op: 'add-query-ok',
    q: booksQ,
    'processed-tx-id': processedTxId,
    result: usersResult(zenecaTriples),
  });
  await waitForLoaded(reactor);
}

/** Every answer `usersQ` gives a new subscriber before the server answers. */
async function reopenUsers(reactor) {
  const seen = [];
  reactor.subscribeQuery(usersQ, (res) => seen.push(res.data.users));
  await reactor.querySubs.waitForKeyToLoad(weakHash(usersQ));
  await waitForLoaded(reactor);
  return seen;
}

test('a query closed before the answer that holds an accepted transaction has it when opened again', async () => {
  const { reactor, close } = await reactorOnUsers();
  reactor.subscribeQuery(booksQ, () => {});
  await answerUsers(reactor, zenecaTriples, 1);

  reactor.pushTx(instatx.tx.users[joeId].update({ handle: 'mine' }));
  await waitForLoaded(reactor);
  const [eventId] = pendingEventIds(reactor);

  close();
  await accept(reactor, eventId, 3);
  await answerBooks(reactor, 3);
  expect(reactor._pendingMutations().size).toBe(0);

  const seen = await reopenUsers(reactor);
  expect(seen.map((users) => joeOf(users).handle)).toEqual(['mine']);
});

test('a closed query has the transactions accepted since, in their order, when opened again', async () => {
  const { reactor, close } = await reactorOnUsers();
  reactor.subscribeQuery(booksQ, () => {});
  await answerUsers(reactor, zenecaTriples, 1);
  close();

  const newId = uuid();
  const sends = [
    instatx.tx.users[joeId].update({ handle: 'first' }),
    instatx.tx.users[newId].update({ handle: 'new' }),
    instatx.tx.users[joeId].update({ handle: 'second' }),
  ];
  for (const [i, chunk] of sends.entries()) {
    reactor.pushTx(chunk);
    await waitForLoaded(reactor);
    await accept(reactor, pendingEventIds(reactor).at(-1), 3 + i);
  }
  await answerBooks(reactor, 5);
  expect(reactor._pendingMutations().size).toBe(0);

  const seen = await reopenUsers(reactor);
  expect(seen.map(handlesOf)).toEqual([
    ['second', 'alex', 'stopa', 'nicolegf', 'new'],
  ]);

  // the server's answer takes their place
  await answerUsers(reactor, triplesWithJoeAs('theirs'), 6);
  expect(handlesOf(seen.at(-1))).toEqual([
    'theirs',
    'alex',
    'stopa',
    'nicolegf',
  ]);
});

test('a query opened again keeps an answer that has processed the accepted transaction', async () => {
  const { reactor, close } = await reactorOnUsers();
  await answerUsers(reactor, zenecaTriples, 1);

  reactor.pushTx(instatx.tx.users[joeId].update({ handle: 'mine' }));
  await waitForLoaded(reactor);
  const [eventId] = pendingEventIds(reactor);
  await accept(reactor, eventId, 3);

  // another writer changed the record after ours was stored
  await refreshUsers(reactor, triplesWithJoeAs('theirs'), 4);
  expect(reactor._pendingMutations().size).toBe(0);
  close();

  const seen = await reopenUsers(reactor);
  expect(seen.map((users) => joeOf(users).handle)).toEqual(['theirs']);
});

test('a query opened again while the accepted transaction is pending shows it once', async () => {
  const { reactor, close } = await reactorOnUsers();
  reactor.subscribeQuery(booksQ, () => {});
  await answerUsers(reactor, zenecaTriples, 1);
  close();

  const newId = uuid();
  const idAttrId = '5a4f5a6d-ba83-4bf0-ae78-27a863300224';
  reactor.pushTx(instatx.tx.users[newId].update({ handle: 'new' }));
  await waitForLoaded(reactor);
  await accept(reactor, pendingEventIds(reactor)[0], 3);
  expect(reactor._pendingMutations().size).toBe(1);

  const seen = await reopenUsers(reactor);
  await answerUsers(
    reactor,
    [
      ...zenecaTriples,
      [newId, idAttrId, newId, Date.now()],
      [newId, handleAttrId, 'new', Date.now()],
    ],
    3,
  );

  expect(seen.map(handlesOf)).toEqual([
    ['joe', 'alex', 'stopa', 'nicolegf', 'new'],
  ]);
  expect(reactor._pendingMutations().size).toBe(0);
});

test('a query opened again by another user has nothing of the transactions accepted before', async () => {
  const { reactor, close } = await reactorOnUsers();
  reactor.subscribeQuery(booksQ, () => {});
  await answerUsers(reactor, zenecaTriples, 1);

  reactor.pushTx(instatx.tx.users[joeId].update({ handle: 'mine' }));
  await waitForLoaded(reactor);
  const [eventId] = pendingEventIds(reactor);
  await accept(reactor, eventId, 3);

  reactor._transport = { close() {} };
  await reactor.updateUser({ id: uuid() });

  // the next user's answer, from before the server processed the transaction
  await answerUsers(reactor, zenecaTriples, 2);
  close();
  await answerBooks(reactor, 3);

  const seen = await reopenUsers(reactor);
  expect(seen.map((users) => joeOf(users).handle)).toEqual(['joe']);
});

// ---------------------------------------------------------------------------
// A query left before its answer is saved

function receiveUsers(reactor, processedTxId) {
  reactor._handleReceive(0, {
    op: 'add-query-ok',
    q: usersQ,
    'processed-tx-id': processedTxId,
    result: usersResult(zenecaTriples),
  });
}

/** Resolves once storage has answered what the reactor has asked it. */
function storageAnswered(reactor) {
  return reactor.querySubs._persister.getAllKeys();
}

/** What storage holds for `usersQ` once the reactor has cleared and saved. */
async function storedUsers(reactor, appId) {
  await storageAnswered(reactor);
  await waitForLoaded(reactor);
  return new IndexedDBStorage(appId, 'querySubs').getItem(weakHash(usersQ));
}

test('a query left as its answer arrives has the answer in storage', async () => {
  const appId = uuid();
  const { reactor, close } = await reactorOnUsers(appId);
  receiveUsers(reactor, 1);
  close();
  await waitForLoaded(reactor);

  const { seen } = await reactorOnUsers(appId);
  expect(seen.map(handlesOf)).toEqual([['joe', 'alex', 'stopa', 'nicolegf']]);
});

test('a query left after the user changed leaves no answer in storage', async () => {
  const appId = uuid();
  const { reactor, close } = await reactorOnUsers(appId);
  await answerUsers(reactor, zenecaTriples, 1);

  reactor._transport = { close() {} };
  reactor.updateUser({ id: uuid() });
  await storageAnswered(reactor);
  close();

  expect((await storedUsers(reactor, appId))?.result).toBeUndefined();
});

test('a query left as its answer arrives is gone from storage once the user changes', async () => {
  const appId = uuid();
  const { reactor, close } = await reactorOnUsers(appId);
  receiveUsers(reactor, 1);
  close();

  reactor._transport = { close() {} };
  reactor.updateUser({ id: uuid() });

  expect(await storedUsers(reactor, appId)).toBeNull();
});
