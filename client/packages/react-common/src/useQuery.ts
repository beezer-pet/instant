'use client';

import {
  weakHash,
  coerceQuery,
  type InstaQLOptions,
  InstantCoreDatabase,
  InstaQLLifecycleState,
  InstantSchemaDef,
  ValidQuery,
} from '@instantdb/core';
import { useCallback, useMemo, useRef, useSyncExternalStore } from 'react';

const defaultState = {
  isLoading: true,
  data: undefined,
  pageInfo: undefined,
  error: undefined,
} as const;

function stateForResult(result: any) {
  return {
    isLoading: !Boolean(result),
    data: undefined,
    pageInfo: undefined,
    error: undefined,
    ...(result ? result : {}),
  };
}

export function useQueryInternal<
  Q extends ValidQuery<Q, Schema>,
  Schema extends InstantSchemaDef<any, any, any>,
  UseDates extends boolean,
>(
  _core: InstantCoreDatabase<Schema, UseDates>,
  _query: null | Q,
  _opts?: InstaQLOptions,
  serverSnapshot?: () => InstaQLLifecycleState<Schema, Q, UseDates> | undefined,
): {
  state: InstaQLLifecycleState<Schema, Q, UseDates>;
  query: any;
  queryHash: string;
} {
  if (_query && _opts && 'ruleParams' in _opts) {
    _query = { $$ruleParams: _opts['ruleParams'], ..._query };
  }
  const query = _query ? coerceQuery(_query) : null;
  const queryHash = weakHash(query);

  // What the store holds for the query when a render first asks it, until
  // the subscription to it puts its results in `resultCacheRef`.
  const storedResult = useMemo(
    () => stateForResult(_core._reactor.getPreviousResult(query)),
    [queryHash],
  );

  // We use a ref to store the result of the query.
  // This is becuase `useSyncExternalStore` uses `Object.is`
  // to compare the previous and next state.
  // If we don't use a ref, the state will always be considered different, so
  // the component will always re-render.
  const resultCacheRef = useRef<InstaQLLifecycleState<Schema, Q, UseDates>>(
    null!,
  );
  // The hash of the query that the result in `resultCacheRef` is for.
  const resultHashRef = useRef<string>(null!);

  // Cache the server snapshot to satisfy useSyncExternalStore's requirement
  // that getServerSnapshot returns the same reference across calls.
  const serverSnapshotCacheRef = useRef<
    InstaQLLifecycleState<Schema, Q, UseDates>
  >(defaultState as any);

  // Similar to `resultCacheRef`, `useSyncExternalStore` will unsubscribe
  // if `subscribe` changes, so we use `useCallback` to memoize the function.
  const subscribe = useCallback(
    (cb) => {
      // Update the ref when the query changes to avoid showing stale data
      resultCacheRef.current = stateForResult(
        _core._reactor.getPreviousResult(query),
      );
      resultHashRef.current = queryHash;
      // before the subscribeQuery is connected and calls it's own callback,
      // we might have data in the store via SSR, we need to notify useSyncExternalStore that
      // the data has changed.
      cb();

      // Don't subscribe if query is null
      if (!query) {
        const unsubscribe = () => {};
        return unsubscribe;
      }

      const unsubscribe = _core.subscribeQuery<Q, UseDates>(query, (result) => {
        resultCacheRef.current = {
          isLoading: !Boolean(result),
          // @ts-expect-error: ts thinks this will always be overwritten
          data: undefined,
          // @ts-expect-error: ts thinks this will always be overwritten
          pageInfo: undefined,
          // @ts-expect-error: ts thinks this will always be overwritten
          error: undefined,
          ...result,
        };
        resultHashRef.current = queryHash;

        cb();
      });

      return unsubscribe;
    },
    // Build a new subscribe function if the query changes
    [queryHash],
  );

  const state = useSyncExternalStore<
    InstaQLLifecycleState<Schema, Q, UseDates>
  >(
    subscribe,
    () =>
      resultHashRef.current === queryHash
        ? resultCacheRef.current
        : storedResult,
    () => {
      if (serverSnapshot && serverSnapshotCacheRef.current === defaultState) {
        const result = serverSnapshot();
        if (result) {
          serverSnapshotCacheRef.current = result;
        }
      }

      return serverSnapshotCacheRef.current;
    },
  );

  return { state, query, queryHash };
}
