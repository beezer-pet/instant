import { PageInfoResponse } from './queryTypes.ts';
import { AttrsStore, AttrsStoreJson, Store, StoreJson } from './store.ts';

export type QuerySubResult = {
  /**
   * The server's answer, without the pending mutations: `dataForQuery`
   * applies those when it reads the result. A result read from storage for
   * a query that is subscribed also holds the accepted transactions it had
   * not processed.
   */
  store: Store;
  attrsStore: AttrsStore;
  pageInfo?: PageInfoResponse<any> | null | undefined;
  aggregate?: any;
  processedTxId?: number;
  isExternal?: boolean;
  /** The result was read from storage, not received from the server. */
  fromStorage?: boolean;
};

export type QuerySub = {
  q: Object;
  eventId?: string;
  lastAccessed?: number | null | undefined;
  result?: QuerySubResult;
};

export type QuerySubResultInStorage = {
  store: StoreJson;
  attrsStore: AttrsStoreJson;
  pageInfo?: PageInfoResponse<any> | null | undefined;
  aggregate?: any;
  processedTxId?: number;
};

export type QuerySubInStorage = {
  q: Object;
  eventId?: string;
  lastAccessed?: number | null | undefined;
  result?: QuerySubResultInStorage;
};
