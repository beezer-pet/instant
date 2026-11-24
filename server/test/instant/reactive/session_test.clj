(ns instant.reactive.session-test
  (:require
   [clojure.core.async :as a]
   [clojure.java.io :as io]
   [clojure.set :as set]
   [clojure.string :as string]
   [clojure.test :as test :refer [deftest is testing]]
   [datascript.core :as ds]
   [instant.config :as config]
   [instant.data.resolvers :as resolvers]
   [instant.db.datalog :as d]
   [instant.db.instaql :as iq]
   [instant.db.model.attr :as attr-model]
   [instant.db.permissioned-transaction :as permissioned-tx]
   [instant.db.transaction :as tx]
   [instant.fixtures :refer [with-empty-app with-movies-app with-zeneca-app]]
   [instant.flags :as flags]
   [instant.grouped-queue :as grouped-queue]
   instant.isn
   [instant.jdbc.aurora :as aurora]
   [instant.jdbc.sql :as sql]
   [instant.lib.ring.websocket :as ws]
   [instant.model.app-stream :as app-stream-model]
   [instant.model.rule :as rule-model]
   [instant.reactive.ephemeral :as eph]
   [instant.reactive.invalidator :as inv]
   [instant.reactive.query :as rq]
   [instant.reactive.receive-queue :as receive-queue]
   [instant.reactive.session :as session]
   [instant.reactive.store :as rs]
   [instant.reactive.topics :as topics]
   [instant.storage.s3 :as s3]
   [instant.util.async :as ua]
   [instant.util.cache :as cache]
   [instant.util.coll :as ucoll]
   [instant.util.exception :as ex]
   [instant.util.test :as test-util])
  (:import
   (com.hazelcast.core HazelcastInstance)
   (java.io ByteArrayOutputStream IOException InputStream)
   (java.nio.channels ClosedChannelException)
   (java.util UUID)))

(test/use-fixtures :each
  (fn [f]
    (binding [config/*env* :test]
      (f))))

(def ^:private non-existent-app-id
  #uuid "39b33777-3324-42dd-8f47-ce41d246c5a6")

(def ^:dynamic *store*
  nil)

(def ^:dynamic *instaql-query-results*
  nil)

(defn- with-session [f]
  (with-movies-app
    (fn [{movies-app-id :id} movies-resolver]
      (with-zeneca-app
        (fn [{zeneca-app-id :id} zeneca-resolver]
          (let [store (rs/init)
                _ (eph/init-store store)
                receive-q
                (grouped-queue/start {:ctx {:store store}
                                      :group-key-fn session/group-key
                                      :combine-fn   session/combine
                                      :process-fn   #(session/straight-jacket-process-receive-q-event store %2 %3)
                                      :max-workers  1})

                realized-eph?   (atom false)
                eph-hz          (delay
                                  (reset! realized-eph? true)
                                  @(future ;; avoid pinning vthread
                                     (eph/init-hz :test
                                                  store
                                                  (let [id (+ 100000 (rand-int 900000))]
                                                    {:instance-name (str "test-instance-" id)
                                                     :cluster-name  (str "test-cluster-" id)}))))
                eph-room-maps   (atom {})
                socket          {:id               (random-uuid)
                                 :ws-conn          (a/chan 100)
                                 :receive-q        receive-q
                                 :ping-job         (future)
                                 :pending-handlers (session/create-pending-handlers)}
                socket-2        {:id               (random-uuid)
                                 :ws-conn          (a/chan 100)
                                 :receive-q        receive-q
                                 :ping-job         (future)
                                 :pending-handlers (session/create-pending-handlers)}
                socket-3        {:id               (random-uuid)
                                 :ws-conn          (a/chan 100)
                                 :receive-q        receive-q
                                 :ping-job         (future)
                                 :pending-handlers (session/create-pending-handlers)}
                query-reactive  rq/instaql-query-reactive!]
            (session/on-open store socket)
            (session/on-open store socket-2)
            (session/on-open store socket-3)

            (binding [*store*                 store
                      *instaql-query-results* (atom {})]
              (with-redefs [receive-queue/receive-q receive-q
                            eph/room-maps           eph-room-maps
                            eph/hz                  eph-hz
                            ws/send-json!           (fn [_app-id msg fake-ws-conn]
                                                      (a/put! fake-ws-conn msg))
                            rq/instaql-query-reactive!
                            (fn [store {:keys [session-id] :as base-ctx} instaql-query return-type inference?]
                              (let [res (query-reactive store base-ctx instaql-query return-type inference?)]
                                (swap! *instaql-query-results* assoc-in [session-id instaql-query] res)
                                res))]
                (try
                  (f store {:socket   socket
                            :socket-2 socket-2
                            :socket-3 socket-3
                            :movies-app-id movies-app-id
                            :movies-resolver movies-resolver
                            :zeneca-app-id zeneca-app-id
                            :zeneca-resolver zeneca-resolver})
                  (finally
                    (session/on-close store socket)
                    (session/on-close store socket-2)
                    (session/on-close store socket-3)
                    (grouped-queue/stop receive-q)
                    (when @realized-eph?
                      (HazelcastInstance/.shutdown (:hz @eph-hz)))))))))))))

(defn read-msg [{:keys [ws-conn id]}]
  (let [ret (ua/<!!-timeout ws-conn)]
    (if (= :timeout ret)
      (throw (ex-info "Timed out waiting for a response" {:id id}))
      (dissoc ret :client-event-id :trace-id))))

(defn- read-msgs [n socket]
  (set (repeatedly n #(read-msg socket))))

(defn- send-msg [socket msg]
  (session/handle-receive *store* (rs/session *store* (:id socket)) msg {}))

(defn- blocking-send-msg [expected-op socket msg]
  (send-msg socket msg)
  (let [ret (read-msg socket)]
    (is (= expected-op (:op ret)))
    (dissoc ret :trace-id)))

(defn- pretty-auth [{:keys [app user] :as _auth}]
  [(:title app) (:email user)])

(deftest anon-auth
  (with-session
    (fn [store {:keys [socket zeneca-app-id movies-app-id]
                {:keys [id]} :socket}]
      (testing "non-existent app"
        (is (= 400
               (:status (blocking-send-msg
                         :error
                         socket
                         {:op :init :app-id non-existent-app-id})))))
      (testing "existing app"
        (let [{op :op event-auth :auth} (blocking-send-msg :init-ok socket {:op :init :app-id zeneca-app-id})
              store-auth (-> (rs/session store id) :session/auth)]
          (is (= :init-ok op))
          (is (= ["empty-app" nil] (pretty-auth event-auth)))
          (is (= ["empty-app" nil] (pretty-auth store-auth)))))

      (testing "already authed"
        (is (= 400
               (:status (blocking-send-msg :error socket {:op :init :app-id movies-app-id}))))))))

(def ^:private query-1987
  {:kw-q  {:movie {:$ {:where {:year 1987}}}}
   :client-resp '({:data
                   {:datalog-result
                    {:join-rows
                     #{#{("eid-lethal-weapon" :movie/title "Lethal Weapon")
                         ("eid-robocop" :movie/id "eid-robocop")
                         ("eid-lethal-weapon" :movie/id "eid-lethal-weapon")
                         ("eid-predator" :movie/id "eid-predator")
                         ("eid-robocop" :movie/year 1987)
                         ("eid-robocop" :movie/title "RoboCop")
                         ("eid-predator" :movie/year 1987)
                         ("eid-predator" :movie/title "Predator")
                         ("eid-lethal-weapon" :movie/year 1987)}}}},
                   :child-nodes ()})
   :store-resp '[{:data {:k "movie"
                         :datalog-query [[:ea ?movie-0 :movie/year 1987]],
                         :datalog-result
                         {:topics [[:ea _ #{:movie/year} #{1987}]],
                          :symbol-values {?movie-0 #{"eid-lethal-weapon" "eid-robocop" "eid-predator"}},
                          :join-rows
                          #{[["eid-lethal-weapon" :movie/year 1987]]
                            [["eid-predator" :movie/year 1987]]
                            [["eid-robocop" :movie/year 1987]]}}},
                  :child-nodes
                  [{:data {:datalog-query [[:ea "eid-robocop"]],
                           :datalog-result
                           {:topics [[:ea #{"eid-robocop"} _ _]],
                            :symbol-values {}
                            :join-rows
                            #{[["eid-robocop" :movie/title "RoboCop"]]
                              [["eid-robocop" :movie/year 1987]]}}},
                    :child-nodes []}
                   {:data {:datalog-query [[:ea "eid-predator"]],
                           :datalog-result
                           {:topics [[:ea #{"eid-predator"} _ _]],
                            :symbol-values {},
                            :join-rows
                            #{[["eid-predator" :movie/year 1987]]
                              [["eid-predator" :movie/title "Predator"]]}}},
                    :child-nodes []}
                   {:data {:datalog-query [[:ea "eid-lethal-weapon"]],
                           :datalog-result
                           {:topics [[:ea #{"eid-lethal-weapon"} _ _]],
                            :symbol-values {},
                            :join-rows
                            #{[["eid-lethal-weapon" :movie/year 1987]]
                              [["eid-lethal-weapon" :movie/title "Lethal Weapon"]]}}},
                    :child-nodes []}]}]})

(def ^:private query-robocop
  {:kw-q {:movie {:$ {:where {:title "RoboCop"}}}}
   :client-resp '({:data
                   {:datalog-result
                    {:join-rows
                     #{#{("eid-robocop" :movie/id "eid-robocop")
                         ("eid-robocop" :movie/year 1987)
                         ("eid-robocop" :movie/title "RoboCop")}}}},
                   :child-nodes ()})
   :store-resp '[{:data {:k "movie",
                         :datalog-query [[:ea ?movie-0 :movie/title "RoboCop"]],
                         :datalog-result
                         {:topics [[:ea _ #{:movie/title} #{"RoboCop"}]],
                          :symbol-values {?movie-0 #{"eid-robocop"}},
                          :join-rows #{[["eid-robocop" :movie/title "RoboCop"]]}}},
                  :child-nodes
                  [{:data {:datalog-query [[:ea "eid-robocop"]],
                           :datalog-result
                           {:topics [[:ea #{"eid-robocop"} _ _]],
                            :symbol-values {},
                            :join-rows
                            #{[["eid-robocop" :movie/title "RoboCop"]]
                              [["eid-robocop" :movie/year 1987]]}}},
                    :child-nodes []}]}]})

(defn- pretty-subs [subscriptions]
  (->> subscriptions
       (map (juxt :instaql-query :datalog-query))
       (group-by first)
       (map (fn [[k v]] [k (set (map second v))]))
       (into {})))

(defn- drop-join-rows-created-at [datalog-result]
  (update datalog-result :join-rows
          #(set (map (fn [row]
                       (set (map (fn [subrow]
                                   (butlast subrow))
                                 row)))
                     %))))

(defn- remove-created-at [node]
  (-> node
      (update-in [:data :datalog-result] drop-join-rows-created-at)
      (update :child-nodes (partial map remove-created-at))))

(defn- pretty-query [r session-id q]
  (let [res (->> *instaql-query-results*
                 deref
                 (#(get-in % [session-id q]))
                 :instaql-result
                 (resolvers/walk-friendly r))]
    (map remove-created-at res)))

(deftest add-query-requires-auth
  (with-session
    (fn [_store {:keys [socket]}]
      (is (= 400
             (:status (blocking-send-msg
                       :error
                       socket {:op :add-query :q (:kw-q query-1987)})))))))

(deftest add-malformed-query-rejected
  (with-session
    (fn [_store {:keys [socket movies-app-id]}]
      (blocking-send-msg :init-ok socket {:op :init :app-id movies-app-id})
      (testing "malformed query are rejected"
        (is (= 400
               (:status (blocking-send-msg :error socket {:op :add-query :q {:movie "Foo"}}))))))))

(deftest add-nil-query-rejected
  (with-session
    (fn [_store {:keys [socket movies-app-id]}]
      (blocking-send-msg :init-ok socket {:op :init :app-id movies-app-id})
      (testing "nil queries are rejected"
        (is (= 400
               (:status (blocking-send-msg :error socket {:op :add-query :q nil}))))))))

(deftest add-query-works
  (with-session
    (fn [_store {:keys [socket movies-app-id movies-resolver]}]
      (blocking-send-msg :init-ok socket {:op :init :app-id movies-app-id})

      (testing "add query: movies in 1987"
        (let [{:keys [op q result]} (blocking-send-msg :add-query-ok
                                                       socket
                                                       {:op :add-query
                                                        :q (:kw-q query-1987)})]
          (is (= op :add-query-ok))
          (is (= (:kw-q query-1987) q))
          (is (= (:client-resp query-1987)
                 (resolvers/walk-friendly movies-resolver (map remove-created-at result)))))))))

(defn- get-datalog-cache-for-app [store app-id]
  (let [conn (rs/app-conn store app-id)]
    (reduce-kv (fn [acc eid v]
                 (assoc acc (:datalog-query/query (ds/entity @conn eid)) v))
               {}
               (cache/as-map-async (:datalog-query-cache (meta conn))))))

(defn- get-subscriptions-for-app-id [store app-id]
  (let [db @(rs/app-conn store app-id)]
    (for [datom (ds/datoms db :aevt :subscription/app-id)
          :when (= app-id (:v datom))
          :let  [ent (ds/entity db (:e datom))]]
      {:app-id        (:subscription/app-id ent)
       :datalog-query (:datalog-query/query (:subscription/datalog-query ent))
       :instaql-query (:instaql-query/query (:subscription/instaql-query ent))
       :session-id    (:subscription/session-id ent)
       :v             (:subscription/v ent)})))

(deftest add-query-sets-store
  (with-session
    (fn [store {:keys [socket movies-app-id movies-resolver]
                {:keys [id]} :socket}]
      (blocking-send-msg :init-ok socket {:op :init :app-id movies-app-id})
      (blocking-send-msg :add-query-ok
                         socket
                         {:op :add-query
                          :q (:kw-q query-1987)})
      (blocking-send-msg :add-query-ok
                         socket
                         {:op :add-query
                          :q (:kw-q query-robocop)})

      (testing "datalog-cache is set"
        (is (= '#{{:children
                   {:pattern-groups
                    [{:patterns [[:ea ?movie-0 :movie/year 1987]],
                      :children
                      {:pattern-groups [{:patterns [[:ea ?movie-0 #{:movie/id
                                                                    :movie/year
                                                                    :movie/trivia
                                                                    :movie/title}]]}],
                       :join-sym ?movie-0}}]}}
                  {:children
                   {:pattern-groups
                    [{:patterns [[:ea ?movie-0 :movie/title "RoboCop"]],
                      :children
                      {:pattern-groups [{:patterns [[:ea ?movie-0 #{:movie/id
                                                                    :movie/year
                                                                    :movie/trivia
                                                                    :movie/title}]]}],
                       :join-sym ?movie-0}}]}}}
               (->> (get-datalog-cache-for-app store movies-app-id)
                    (resolvers/walk-friendly movies-resolver)
                    keys
                    set))))

      (testing "subs are set"
        (is (= '{{:movie {:$ {:where {:year 1987}}}}
                 #{{:children
                    {:pattern-groups
                     [{:patterns [[:ea ?movie-0 :movie/year 1987]],
                       :children
                       {:pattern-groups [{:patterns [[:ea ?movie-0 #{:movie/id
                                                                     :movie/year
                                                                     :movie/trivia
                                                                     :movie/title}]]}],
                        :join-sym ?movie-0}}]}}},
                 {:movie {:$ {:where {:title "RoboCop"}}}}
                 #{{:children
                    {:pattern-groups
                     [{:patterns [[:ea ?movie-0 :movie/title "RoboCop"]],
                       :children
                       {:pattern-groups [{:patterns [[:ea ?movie-0 #{:movie/id
                                                                     :movie/year
                                                                     :movie/trivia
                                                                     :movie/title}]]}],
                        :join-sym ?movie-0}}]}}}}
               (->> (get-subscriptions-for-app-id store movies-app-id)
                    (resolvers/walk-friendly movies-resolver)
                    pretty-subs))))

      (testing "instaql-queries are set"
        (is (= (:client-resp query-1987)
               (pretty-query movies-resolver id (:kw-q query-1987))))
        (is (= (:client-resp query-robocop)
               (pretty-query movies-resolver id (:kw-q query-robocop))))))))

(deftest add-duplicate-query-returns-query-exists
  (with-session
    (fn [_store {:keys [socket movies-app-id]}]
      (blocking-send-msg :init-ok socket {:op :init :app-id movies-app-id})
      (blocking-send-msg :add-query-ok socket {:op :add-query :q (:kw-q query-robocop)})
      (is (= {:op :add-query-exists,
              :q (:kw-q query-robocop)}
             (blocking-send-msg :add-query-exists socket {:op :add-query
                                                          :q (:kw-q query-robocop)}))))))

(deftest remove-query-works
  (with-session
    (fn [_store {:keys [socket movies-app-id]}]
      (blocking-send-msg :init-ok socket {:op :init :app-id movies-app-id})
      (blocking-send-msg :add-query-ok
                         socket
                         {:op :add-query
                          :q (:kw-q query-1987)})

      (is (= {:op :remove-query-ok,
              :q (:kw-q query-1987)}
             (blocking-send-msg :remove-query-ok
                                socket {:op :remove-query
                                        :q (:kw-q query-1987)}))))))

(deftest remove-query-updates-store
  (with-session
    (fn [store {:keys [socket movies-app-id movies-resolver]}]
      (blocking-send-msg :init-ok socket {:op :init :app-id movies-app-id})

      (blocking-send-msg :add-query-ok
                         socket
                         {:op :add-query
                          :q (:kw-q query-1987)})

      (blocking-send-msg :add-query-ok
                         socket
                         {:op :add-query
                          :q (:kw-q query-robocop)})

      ;; okay, let's delete the first query
      (is (= {:op :remove-query-ok,
              :q (:kw-q query-1987)}
             (blocking-send-msg :remove-query-ok
                                socket
                                {:op :remove-query
                                 :q (:kw-q query-1987)})))

      (testing "stray datalog queries are removed"
        (is (= '#{{:children
                   {:pattern-groups
                    [{:patterns [[:ea ?movie-0 :movie/title "RoboCop"]],
                      :children
                      {:pattern-groups [{:patterns [[:ea ?movie-0 #{:movie/id
                                                                    :movie/year
                                                                    :movie/trivia
                                                                    :movie/title}]]}],
                       :join-sym ?movie-0}}]}}}
               (->> (get-datalog-cache-for-app store movies-app-id)
                    (resolvers/walk-friendly movies-resolver)
                    keys
                    set))))

      (testing "subs for query one are removed"
        (is (= '{{:movie {:$ {:where {:title "RoboCop"}}}}
                 #{{:children
                    {:pattern-groups
                     [{:patterns [[:ea ?movie-0 :movie/title "RoboCop"]],
                       :children
                       {:pattern-groups [{:patterns [[:ea ?movie-0 #{:movie/id
                                                                     :movie/year
                                                                     :movie/trivia
                                                                     :movie/title}]]}],
                        :join-sym ?movie-0}}]}}}}
               (some->> (get-subscriptions-for-app-id store movies-app-id)
                        seq
                        (resolvers/walk-friendly movies-resolver)
                        pretty-subs))))

      ;; okay, now for the second query
      (is (= {:op :remove-query-ok,
              :q (:kw-q query-robocop)}
             (blocking-send-msg :remove-query-ok
                                socket
                                {:op :remove-query
                                 :q (:kw-q query-robocop)})))

      (testing "all subs are gone"
        (is (empty? (get-subscriptions-for-app-id store movies-app-id))))

      (testing "datalog-cache is cleaned"
        (is (empty? (get-datalog-cache-for-app store movies-app-id)))))))

(deftest mark-stale-topics-works
  (with-movies-app
    (fn [{app-id :id :as _app} r]
      (with-session
        (fn [store {{sess-id :id :as socket} :socket}]
          (blocking-send-msg :init-ok socket {:op :init :app-id app-id})
          ;; Reset the tx-id before we test
          (rs/transact! "store/reset-tx-id"
                        (rs/app-conn store app-id)
                        [[:db.fn/call (fn [db]
                                        [[:db/add (ds/entid db [:tx-meta/app-id app-id]) :tx-meta/processed-tx-id 0]
                                         [:db/add (ds/entid db [:tx-meta/app-id app-id]) :tx-meta/processed-isn (instant.isn/test-isn 0)]])]])

          (blocking-send-msg :add-query-ok socket
                             {:op :add-query
                              :q (:kw-q query-1987)})

          ;; No stale queries at first
          (is (empty? (rs/get-stale-instaql-queries store app-id sess-id)))

          (rs/mark-stale-topics! store
                                 app-id
                                 5
                                 (instant.isn/test-isn 5)
                                 [(d/pat->coarse-topic
                                   [:ea
                                    (resolvers/->uuid r "eid-predator")])]
                                 {})

          ;; Now we have a stale query
          (is (= [(:kw-q query-1987)]
                 (map :instaql-query/query (rs/get-stale-instaql-queries store app-id sess-id))))

          ;; We also removed datalog queries from cache
          (is (= '#{}
                 (->> (get-datalog-cache-for-app store app-id)
                      (resolvers/walk-friendly r)
                      keys
                      set)))

          ;; we also recorded the tx-id and isn that was processed
          (is (= 5 (rs/get-processed-tx-id store app-id)))
          (is (= (instant.isn/test-isn 5) (rs/get-processed-isn store app-id))))))))

(deftest refresh-skip-attrs
  (with-movies-app
    (fn [{app-id :id :as _app} r]
      (with-session
        (fn [store {{sess-id :id :as socket} :socket}]
          (send-msg socket {:op :init
                            :app-id app-id
                            :versions {session/core-version-key "0.20.5"}})
          (let [ret (read-msg socket)]
            (is (= :init-ok (:op ret)))
            (is (contains? ret :attrs)))

          (blocking-send-msg :add-query-ok
                             socket
                             {:op :add-query
                              :q (:kw-q query-1987)})

          ;; mark topic as stale
          ;; clear the query hash so that the refresh will trigger a send
          (rs/mark-stale-topics! store
                                 app-id
                                 0
                                 (instant.isn/test-isn 0)
                                 [(d/pat->coarse-topic
                                   [:ea
                                    (resolvers/->uuid r "eid-predator")])]
                                 {})
          (ds/transact! (rs/app-conn store app-id)
                        [[:db/retract [:instaql-query/session-id+query [sess-id (:kw-q query-1987)]] :instaql-query/hash]])

          (testing "if attrs don't change, don't send them"
            (send-msg socket {:session-id (:id socket)
                              :op :refresh})
            (let [ret (read-msg socket)]
              (is (= :refresh-ok (:op ret)))
              (is (not (contains? ret :attrs)))))

          ;; change attrs
          (tx/transact! (aurora/conn-pool :write)
                        (attr-model/get-by-app-id app-id)
                        app-id
                        [[:add-attr {:id (random-uuid)
                                     :forward-identity [(random-uuid) "profile" "id"]
                                     :unique? true
                                     :index? false
                                     :value-type :blob
                                     :cardinality :one}]])

          ;; mark topic as stale
          ;; clear the query hash so that the refresh will trigger a send
          (rs/mark-stale-topics! store
                                 app-id
                                 0
                                 (instant.isn/test-isn 0)
                                 [(d/pat->coarse-topic
                                   [:ea
                                    (resolvers/->uuid r "eid-predator")])]
                                 {})
          (ds/transact! (rs/app-conn store app-id)
                        [[:db/retract [:instaql-query/session-id+query [sess-id (:kw-q query-1987)]] :instaql-query/hash]])

          (testing "if attrs change, they get send again"
            (send-msg socket {:session-id (:id socket)
                              :op :refresh})
            (let [ret (read-msg socket)]
              (is (= :refresh-ok (:op ret)))
              (is (contains? ret :attrs)))))))))

(deftest refresh-sends-attrs-when-schema-changes-without-queries
  (with-session
    (fn [_store {:keys [socket movies-app-id]}]
      (send-msg socket {:op :init
                        :app-id movies-app-id
                        :versions {session/core-version-key "0.20.5"}})
      (let [ret (read-msg socket)]
        (is (= :init-ok (:op ret)))
        (is (contains? ret :attrs)))

      (tx/transact! (aurora/conn-pool :write)
                    (attr-model/get-by-app-id movies-app-id)
                    movies-app-id
                    [[:add-attr {:id (random-uuid)
                                 :forward-identity [(random-uuid) "profile" "id"]
                                 :unique? true
                                 :index? false
                                 :value-type :blob
                                 :cardinality :one}]])

      (send-msg socket {:session-id (:id socket)
                        :op :refresh})
      (let [ret (read-msg socket)]
        (is (= :refresh-ok (:op ret)))
        (is (= [] (:computations ret)))
        (is (contains? ret :attrs))))))

(deftest refresh-skips-unchanged-results
  (with-session
    (fn [store {:keys [socket movies-app-id movies-resolver]}]
      (let [sess-id (:id socket)
            q (:kw-q query-1987)
            eid (resolvers/->uuid movies-resolver "eid-robocop")
            mark-stale! #(rs/mark-stale-topics! store movies-app-id 0
                                               (instant.isn/test-isn 0)
                                               [(d/pat->coarse-topic [:ea eid])]
                                               {})
            refresh! #(#'session/handle-refresh! store sess-id {:op :refresh} (atom {}))]
        (blocking-send-msg :init-ok socket {:op :init
                                           :app-id movies-app-id
                                           :versions {session/core-version-key "0.20.5"}})
        ;; App/query/flag boundaries are covered by query-test. Exercise the
        ;; refresh integration with this disposable app's movie subscription.
        (with-redefs [flags/throttle-refresh? (constantly false)
                      rq/skip-unchanged-result-enabled? (fn [app-id query]
                                                         (and (= app-id movies-app-id) (= query q)))]
          (doseq [return-type [:tree :join-rows]]
            (let [title (str "Updated title " return-type)
                  initial (blocking-send-msg :add-query-ok socket {:op :add-query
                                                                 :q q
                                                                 :return-type return-type})]
              (is (some? (:result initial)) "New subscriptions always receive their result")
              (mark-stale!)
              (is (seq (rs/get-stale-instaql-queries store movies-app-id sess-id)))
              (refresh!)
              (is (nil? (a/poll! (:ws-conn socket))) "Unchanged refreshes send no computation")
              (is (nil? (get-in @*instaql-query-results* [sess-id q :instaql-result])))
              (is (false? (get-in @*instaql-query-results* [sess-id q :result-changed?])))
              (is (empty? (rs/get-stale-instaql-queries store movies-app-id sess-id)))

              (tx/transact! (aurora/conn-pool :write)
                            (attr-model/get-by-app-id movies-app-id)
                            movies-app-id
                            [[:add-triple eid
                              (resolvers/->uuid movies-resolver :movie/title)
                              title]])
              (mark-stale!)
              (refresh!)
              (let [message (read-msg socket)
                    computation (first (:computations message))]
                (is (= :refresh-ok (:op message)))
                (is (= 1 (count (:computations message))))
                (is (true? (:result-changed? computation)))
                (is (some? (:instaql-result computation)))
                (is (not= (:result initial) (:instaql-result computation)))
                (is (if (= :tree return-type)
                      (some #(= title (get % "title"))
                            (get (:instaql-result computation) "movie"))
                      (some #(= title (nth % 2))
                            (-> computation :instaql-result first :data :datalog-result :join-rows first))))
                (is (= (:result-meta initial) (:result-meta computation))))
            (blocking-send-msg :remove-query-ok socket {:op :remove-query :q q}))))))))

(deftest refresh-populates-cache
  (with-movies-app
    (fn [{app-id :id :as _app} r]
      (with-session
        (fn [store {{sess-id :id :as socket} :socket}]
          (blocking-send-msg :init-ok socket {:op :init :app-id app-id})
          (blocking-send-msg :add-query-ok
                             socket
                             {:op :add-query
                              :q (:kw-q query-1987)})
          (rs/mark-stale-topics! store
                                 app-id
                                 0
                                 (instant.isn/test-isn 0)
                                 [(d/pat->coarse-topic
                                   [:ea
                                    (resolvers/->uuid r "eid-predator")])]
                                 {})

          (testing "send refresh"
            ;; clear the query hash so that the refresh will trigger a send
            (ds/transact! (rs/app-conn store app-id)
                          [[:db/retract [:instaql-query/session-id+query [sess-id (:kw-q query-1987)]] :instaql-query/hash]])
            (blocking-send-msg :refresh-ok socket {:session-id (:id socket)
                                                   :op :refresh})

            ;; After refresh there should be no more stale queries
            (is (empty? (rs/get-stale-instaql-queries store app-id sess-id)))

            ;; Datalog cache now has more things
            (is (= '#{{:children
                       {:pattern-groups
                        [{:patterns [[:ea ?movie-0 :movie/year 1987]],
                          :children
                          {:pattern-groups [{:patterns [[:ea ?movie-0 #{:movie/year
                                                                        :movie/id
                                                                        :movie/trivia
                                                                        :movie/title}]]}],
                           :join-sym ?movie-0}}]}}}
                   (->> (get-datalog-cache-for-app store app-id)
                        (resolvers/walk-friendly r)
                        keys
                        set)))))))))

(deftest refresh-cleans-up-stray-datalog-queries
  (with-movies-app
    (fn [{app-id :id :as _app} r]
      (with-session
        (fn [store {{sess-id :id :as socket} :socket}]
          (blocking-send-msg :init-ok socket {:op :init :app-id app-id})
          (let [john-uuid (resolvers/->uuid r "eid-john-mctiernan")
                ted-uuid (resolvers/->uuid r "eid-ted-kotcheff")
                kw-q {:movie {:$ {:where {:director john-uuid}}}}]
            (blocking-send-msg :add-query-ok
                               socket
                               {:op :add-query
                                :q kw-q})

            ;; No stale queries at first
            ;; datalog-cache, subs, instaql look good
            (is (empty? (rs/get-stale-instaql-queries store app-id sess-id)))
            (is (= '#{{:children
                       {:pattern-groups
                        [{:patterns [[:vae ?movie-0 :movie/director "eid-john-mctiernan"]],
                          :children
                          {:pattern-groups [{:patterns [[:ea ?movie-0 #{:movie/year
                                                                        :movie/id
                                                                        :movie/trivia
                                                                        :movie/title}]]}],
                           :join-sym ?movie-0}
                          :topic-scope {:sym ?movie-0
                                        :links #{[:movie/director "eid-john-mctiernan"]}}}]}}}
                   (->> (get-datalog-cache-for-app store app-id)
                        (resolvers/walk-friendly r)
                        keys
                        set)))
            (is (= '{{:movie {:$ {:where {:director "eid-john-mctiernan"}}}}
                     #{{:children
                        {:pattern-groups
                         [{:patterns
                           [[:vae ?movie-0 :movie/director "eid-john-mctiernan"]],
                           :children
                           {:pattern-groups [{:patterns [[:ea ?movie-0 #{:movie/year
                                                                         :movie/id
                                                                         :movie/trivia
                                                                         :movie/title}]]}],
                            :join-sym ?movie-0}
                           :topic-scope {:sym ?movie-0
                                         :links #{[:movie/director "eid-john-mctiernan"]}}}]}}}}
                   (->> (get-subscriptions-for-app-id store app-id)
                        (resolvers/walk-friendly r)
                        pretty-subs)))

            ;; do mutation
            (tx/transact! (aurora/conn-pool :write)
                          (attr-model/get-by-app-id app-id)
                          app-id
                          [[:retract-triple
                            (resolvers/->uuid r "eid-predator")
                            (resolvers/->uuid r :movie/director)
                            john-uuid]
                           [:add-triple
                            (resolvers/->uuid r "eid-predator")
                            (resolvers/->uuid r :movie/director)
                            ted-uuid]])

            ;; mark topic as stale
            (rs/mark-stale-topics! store
                                   app-id
                                   0
                                   (instant.isn/test-isn 0)
                                   [(d/pat->coarse-topic
                                     [:vae '_ '_ john-uuid])]
                                   {})

            ;; send refresh
            (blocking-send-msg :refresh-ok socket {:session-id (:id socket)
                                                   :op :refresh})

            ;; data is cleaned
            (is (empty? (rs/get-stale-instaql-queries store app-id sess-id)))
            (is (= '#{{:children
                       {:pattern-groups
                        [{:patterns [[:vae ?movie-0 :movie/director "eid-john-mctiernan"]],
                          :children
                          {:pattern-groups [{:patterns [[:ea ?movie-0 #{:movie/year
                                                                        :movie/id
                                                                        :movie/trivia
                                                                        :movie/title}]]}],
                           :join-sym ?movie-0}
                          :topic-scope {:sym ?movie-0
                                        :links #{[:movie/director "eid-john-mctiernan"]}}}]}}}
                   (->> (get-datalog-cache-for-app store app-id)
                        (resolvers/walk-friendly r)
                        keys
                        set)))
            (is (= '{{:movie {:$ {:where {:director "eid-john-mctiernan"}}}}
                     #{{:children
                        {:pattern-groups
                         [{:patterns
                           [[:vae ?movie-0 :movie/director "eid-john-mctiernan"]],
                           :children
                           {:pattern-groups [{:patterns [[:ea ?movie-0 #{:movie/year
                                                                         :movie/id
                                                                         :movie/trivia
                                                                         :movie/title}]]}],
                            :join-sym ?movie-0}
                           :topic-scope {:sym ?movie-0
                                         :links #{[:movie/director "eid-john-mctiernan"]}}}]}}}}
                   (->> (get-subscriptions-for-app-id store app-id)
                        (resolvers/walk-friendly r)
                        pretty-subs)))))))))

(defn- hold-first-call
  "Wraps `f` so that its first call delivers `held` and waits for `release`:
   `:before` it runs `f`, `:after` it has run it, or to `:throw` in place of
   running it."
  [f mode held release]
  (let [first-call? (atom true)
        hold (fn []
               (deliver held true)
               @release)]
    (fn [& args]
      (if-not (compare-and-set! first-call? true false)
        (apply f args)
        (case mode
          :before (do (hold) (apply f args))
          :after (let [ret (apply f args)]
                   (hold)
                   ret)
          :throw (do (hold) (throw (ex-info "Run failed" {}))))))))

(defn- run-beside
  "Calls `run` on another thread, holds it at its first call of the function
   in the var `v` (`mode` as in `hold-first-call`) and calls `f` meanwhile.
   Returns when `run` is through, and throws what it threw."
  [v mode run f]
  (let [held (promise)
        release (promise)]
    (with-redefs-fn {v (hold-first-call @v mode held release)}
      (fn []
        (let [running (future (run))]
          (try
            (is (deref held 5000 false) "The run reaches the held call")
            (f)
            (finally
              (deliver release true)))
          (let [ret (deref running 5000 ::timeout)]
            (is (not= ::timeout ret) "The run ends")))))))

(defn- refresh-beside
  "Runs a refresh of the socket's session beside `f`, as `run-beside`."
  [store socket v mode f]
  (run-beside v
              mode
              #(#'session/handle-refresh! store (:id socket) {:op :refresh} (atom {}))
              f))

(defn- with-stale-query
  "Calls `f` with a session that holds query-1987, marked stale."
  [f]
  (with-session
    (fn [store {:keys [socket movies-app-id movies-resolver]}]
      (blocking-send-msg :init-ok socket {:op :init
                                          :app-id movies-app-id
                                          :versions {session/core-version-key "0.20.5"}})
      (blocking-send-msg :add-query-ok socket {:op :add-query
                                               :q (:kw-q query-1987)})
      (rs/mark-stale-topics! store
                             movies-app-id
                             0
                             (instant.isn/test-isn 0)
                             [(d/pat->coarse-topic
                               [:ea (resolvers/->uuid movies-resolver "eid-robocop")])]
                             {})
      (is (seq (rs/get-stale-instaql-queries store movies-app-id (:id socket))))
      (f store socket movies-app-id movies-resolver))))

(defn- query-datoms
  "What the store holds for the app's queries: every datom of its instaql
   queries, subscriptions and datalog queries, with a hash in place of a
   compiled topic."
  [store app-id]
  (->> (ds/datoms @(rs/app-conn store app-id) :eavt)
       (remove #(= "tx-meta" (namespace (:a %))))
       (map (fn [{:keys [e a v]}]
              [e a (if (= :instaql-query/topic a) (hash v) v)]))
       set))

(deftest refresh-leaves-out-a-query-removed-since-it-was-found-stale
  (with-stale-query
    (fn [store socket app-id _r]
      (let [q (:kw-q query-1987)]
        (refresh-beside store socket #'rs/get-stale-instaql-queries :after
                        #(blocking-send-msg :remove-query-ok socket {:op :remove-query :q q}))
        (is (empty? (rs/session-instaql-queries store app-id (:id socket))))
        (is (empty? (query-datoms store app-id)))
        (is (nil? (a/poll! (:ws-conn socket))) "No computation is sent for the removed query")
        (is (some? (:result (blocking-send-msg :add-query-ok socket {:op :add-query :q q}))))))))

(deftest refresh-leaves-nothing-of-a-query-removed-while-it-runs
  (with-stale-query
    (fn [store socket app-id _r]
      (let [q (:kw-q query-1987)]
        (refresh-beside store socket #'iq/permissioned-query :before
                        #(blocking-send-msg :remove-query-ok socket {:op :remove-query :q q}))
        (is (empty? (query-datoms store app-id)))
        (is (nil? (a/poll! (:ws-conn socket))) "No computation is sent for the removed query")
        (is (some? (:result (blocking-send-msg :add-query-ok socket {:op :add-query :q q}))))))))

(defn- remove-write-and-add!
  "Removes query-1987 from the session, transacts `tx-steps` and adds the
   query again. Returns the result of the add."
  [socket app-id tx-steps]
  (let [q (:kw-q query-1987)]
    (blocking-send-msg :remove-query-ok socket {:op :remove-query :q q})
    (tx/transact! (aurora/conn-pool :write)
                  (attr-model/get-by-app-id app-id)
                  app-id
                  tx-steps)
    (:result (blocking-send-msg :add-query-ok socket {:op :add-query :q q}))))

(defn- robocop-title-step [r title]
  [:add-triple
   (resolvers/->uuid r "eid-robocop")
   (resolvers/->uuid r :movie/title)
   title])

(deftest refresh-leaves-a-query-added-again-while-it-runs-as-the-add-made-it
  (with-stale-query
    (fn [store socket app-id r]
      (let [title "RoboCop 2"
            after-add (atom nil)]
        (refresh-beside store socket #'iq/permissioned-query :after
                        (fn []
                          (let [result (remove-write-and-add! socket app-id [(robocop-title-step r title)])]
                            (is (some #(= title (nth % 2))
                                      (-> result first :data :datalog-result :join-rows first))))
                          (reset! after-add (query-datoms store app-id))))
        (is (seq (get-subscriptions-for-app-id store app-id)))
        (is (= @after-add (query-datoms store app-id)))
        (is (nil? (a/poll! (:ws-conn socket))) "The result from before the add is not sent")))))

(deftest refresh-sends-no-result-from-before-a-query-was-added-again
  (with-stale-query
    (fn [store socket app-id r]
      (tx/transact! (aurora/conn-pool :write)
                    (attr-model/get-by-app-id app-id)
                    app-id
                    [(robocop-title-step r "RoboCop 2")])
      (refresh-beside store socket #'rq/collect-instaql-results-for-client :before
                      #(remove-write-and-add! socket app-id [(robocop-title-step r "RoboCop 3")]))
      (is (nil? (a/poll! (:ws-conn socket))) "The result from before the add is not sent"))))

(deftest refresh-leaves-the-topics-of-a-query-added-again-while-it-runs
  (with-stale-query
    (fn [store socket app-id r]
      (let [movie (random-uuid)
            title-attr (resolvers/->uuid r :movie/title)
            after-add (atom nil)]
        (refresh-beside store socket #'rs/record-datalog-query-finish! :before
                        (fn []
                          (remove-write-and-add! socket app-id
                                                 [[:add-triple movie (resolvers/->uuid r :movie/id) (str movie)]
                                                  [:add-triple movie (resolvers/->uuid r :movie/year) 1987]])
                          (reset! after-add (query-datoms store app-id))))
        (is (= @after-add (query-datoms store app-id)))
        (rs/mark-stale-topics! store
                               app-id
                               0
                               (instant.isn/test-isn 0)
                               [[#{:ea} #{movie} #{title-attr} #{"Wall Street"}]]
                               {})
        (is (seq (rs/get-stale-instaql-queries store app-id (:id socket)))
            "A write to the movie that the add found marks the query stale")))))

(deftest failed-refresh-leaves-a-query-added-again-while-it-runs
  (with-stale-query
    (fn [store socket app-id r]
      (let [after-add (atom nil)]
        (is (thrown-with-msg?
             Exception #"Run failed"
             (refresh-beside store socket #'iq/permissioned-query :throw
                             (fn []
                               (remove-write-and-add! socket app-id [(robocop-title-step r "RoboCop 2")])
                               (reset! after-add (query-datoms store app-id))))))
        (is (seq (get-subscriptions-for-app-id store app-id)))
        (is (= @after-add (query-datoms store app-id)))))))

(deftest failed-add-query-leaves-a-query-added-again-while-it-runs
  (with-stale-query
    (fn [store socket app-id r]
      (let [q (:kw-q query-1987)
            after-add (atom nil)]
        (blocking-send-msg :remove-query-ok socket {:op :remove-query :q q})
        (run-beside #'iq/permissioned-query :throw
                    #(send-msg socket {:op :add-query :q q})
                    (fn []
                      (remove-write-and-add! socket app-id [(robocop-title-step r "RoboCop 2")])
                      (reset! after-add (query-datoms store app-id))))
        (is (= :error (:op (read-msg socket))))
        (is (seq (get-subscriptions-for-app-id store app-id)))
        (is (= @after-add (query-datoms store app-id)))))))

(deftest refresh-throttles-when-flag-enabled
  (with-session
    (fn [_store {:keys [socket movies-app-id]}]
      ;; Init with a version that supports skip-attrs so the session caches an
      ;; attrs-hash. A refresh with no stale queries then has no computations
      ;; and an unchanged attrs hash — the conditions handle-refresh! uses to
      ;; emit a throttle return.
      (send-msg socket {:op :init
                        :app-id movies-app-id
                        :versions {session/core-version-key "0.20.5"}})
      (is (= :init-ok (:op (read-msg socket))))

      (let [refresh-times (atom [])
            release (promise)
            first-entered (promise)
            orig-handle-refresh @#'session/handle-refresh!]
        (with-redefs [flags/throttle-refresh? (constantly true)
                      flags/refresh-throttle-ms (constantly 500)
                      session/handle-refresh!
                      (fn [store sess-id event debug-info]
                        (let [n (count (swap! refresh-times conj
                                              (System/currentTimeMillis)))]
                          (when (= 1 n)
                            (deliver first-entered :go)
                            (deref release 2000 :timeout))
                          (orig-handle-refresh store sess-id event debug-info)))]
          ;; Put the first refresh; the worker polls it and blocks inside
          ;; handle-refresh! until we deliver `release`. That keeps the worker
          ;; busy so the second put lands alone in the queue rather than
          ;; combining with the first via the [:refresh :refresh] combiner.
          (receive-queue/put! {:op :refresh :session-id (:id socket)})
          (is (not= :timeout (deref first-entered 2000 :timeout))
              "worker should enter handle-refresh!")

          (receive-queue/put! {:op :refresh :session-id (:id socket)})

          ;; Release the blocked refresh. It returns a throttle map, which the
          ;; grouped queue treats as a signal to delay the next process-task
          ;; by refresh-throttle-ms.
          (let [released-at (System/currentTimeMillis)]
            (deliver release :go)

            (test-util/wait-for #(= 2 (count @refresh-times)) 2000)

            (let [[_ t2] @refresh-times
                  gap-after-release (- t2 released-at)]
              (testing "throttle delays the next refresh by ~refresh-throttle-ms"
                (is (<= 500 gap-after-release)
                    (str "expected gap-after-release >= 500ms, got "
                         gap-after-release "ms")))))

          (testing "no refresh-ok is sent when there's nothing new to report"
            (is (= :timeout (ua/<!!-timeout (:ws-conn socket) 100)))))))))

(deftest transact-requires-auth
  (with-session
    (fn [_store {:keys [socket]}]
      (is (= 400
             (:status (blocking-send-msg
                       :error
                       socket {:op :transact :tx-steps []})))))))

(deftest transact-rejects-malformed
  (with-empty-app
    (fn [{app-id :id}]
      (with-session
        (fn [_store {:keys [socket]}]
          (blocking-send-msg :init-ok socket {:op :init :app-id app-id})
          (is (= 400
                 (:status (blocking-send-msg
                           :error
                           socket {:op :transact
                                   :tx-steps [["moop" 1 2 3]]})))))))))

(deftest transact-works
  (with-movies-app
    (fn [{app-id :id :as _app} r]
      (with-session
        (fn [_store {:keys [socket]}]
          (blocking-send-msg :init-ok socket {:op :init :app-id app-id})
          (let [robocop-eid (resolvers/->uuid r "eid-robocop")
                name-attr-id (resolvers/->uuid r :movie/title)]
            (is (= :transact-ok
                   (:op (blocking-send-msg
                         :transact-ok
                         socket {:op :transact
                                 :tx-steps
                                 [["add-triple" robocop-eid name-attr-id "RoboDrizzle"]]}))))
            (let [resp (blocking-send-msg :add-query-ok
                                          socket {:op :add-query
                                                  :q (:kw-q query-1987)})]
              (is (contains? (->> resp
                                  :result
                                  (mapcat iq/data-seq)
                                  (map :datalog-result)
                                  (mapcat :join-rows)

                                  (#(apply concat %))
                                  ;; last set of rows, drop time, get value
                                  (map (comp last drop-last))
                                  set)
                             "RoboDrizzle")))))))))

(deftest join-room-works
  (with-session
    (fn [_store {:keys [socket movies-app-id]}]
      (blocking-send-msg :init-ok socket {:op :init
                                          :app-id movies-app-id})
      (let [rid (str (UUID/randomUUID))
            sess-id (:id socket)]

        (send-msg socket
                  {:op :join-room
                   :room-id rid})

        (let [msgs (read-msgs 2 socket)
              join-room-ok (ucoll/seek (fn [msg] (= :join-room-ok (:op msg))) msgs)]

          (is (= [:join-room-ok :refresh-presence]
                 (sort (map :op msgs))))

          (is join-room-ok)

          (is (= rid (:room-id join-room-ok)))
          (is (eph/in-room? movies-app-id rid sess-id)))))))

(deftest join-room-errors-on-invalid-room-id
  (with-session
    (fn [_store {:keys [socket movies-app-id]}]
      (blocking-send-msg :init-ok socket {:op :init
                                          :app-id movies-app-id})
      (send-msg socket
                {:op :join-room
                 :room-id nil})

      (let [msg (first (read-msgs 1 socket))]
        (is (= :error (:op msg)))
        (is (= :param-missing (:type msg)))))))

(deftest leave-room-works
  (with-session
    (fn [_store {:keys [socket movies-app-id]}]
      (send-msg socket {:op :init
                        :app-id movies-app-id})
      (is (= :init-ok (:op (read-msg socket))))

      (let [rid (str (UUID/randomUUID))
            sess-id (:id socket)]
        (send-msg socket {:op :join-room, :room-id rid})
        (is (= #{:join-room-ok :refresh-presence}
               (->> (read-msgs 2 socket) (map :op) set)))

        (is (eph/in-room? movies-app-id rid sess-id))

        (let [{:keys [op room-id]} (blocking-send-msg :leave-room-ok
                                                      socket
                                                      {:op :leave-room
                                                       :room-id rid})]
          ;; session is no longer in the room
          (is (= :leave-room-ok op))
          (is (= rid room-id))
          (is (not (eph/in-room? movies-app-id rid sess-id)))

          (is (empty? (:sessions @eph/room-maps)))
          (is (empty? (:rooms @eph/room-maps))))))))

(deftest patch-presence-works
  (with-session
    (fn [_store {:keys [socket movies-app-id]}]
      (let [rid     (str (UUID/randomUUID))
            sess-id (:id socket)
            d1      {:a "a" :b "b" :c "c" :d "d" :e "e"}
            d2      {:a "a" :b "b" :c "c" :e "E" :f "F"}]
        (send-msg socket {:op       :init
                          :app-id   movies-app-id
                          :versions {session/core-version-key "0.17.6"}})
        (is (= :init-ok (:op (read-msg socket))))

        (send-msg socket {:op :join-room :room-id rid})
        (is (= #{:join-room-ok :refresh-presence}
               (->> (read-msgs 2 socket) (map :op) set)))

        ;; session is in the room
        (is (eph/in-room? movies-app-id rid sess-id))

        ;; session data is empty
        (is (= {sess-id {:peer-id     sess-id
                         :instance-id "dev"
                         :user        nil
                         :data        {}}}
               (eph/get-room-data movies-app-id rid)))

        ;; set session data
        (send-msg socket {:op :set-presence, :room-id rid, :data d1})

        (is (= #{{:op :set-presence-ok, :room-id rid}
                 {:op      :patch-presence
                  :room-id rid
                  :edits   [[[sess-id :data] :r {:a "a" :b "b" :c "c" :d "d" :e "e"}]]}}
               (read-msgs 2 socket)))
        (is (= {sess-id {:peer-id     sess-id
                         :instance-id "dev"
                         :user        nil
                         :data        d1}}
               (eph/get-room-data movies-app-id rid)))

        ;; udpate session data
        (send-msg socket {:op :set-presence, :room-id rid, :data d2})
        (is (= #{{:op :set-presence-ok, :room-id rid}
                 {:op      :patch-presence
                  :room-id rid
                  :edits   [[[sess-id :data :d] :-]
                            [[sess-id :data :e] :r "E"]
                            [[sess-id :data :f] :+ "F"]]}}
               (read-msgs 2 socket)))
        (is (= {sess-id {:peer-id     sess-id
                         :instance-id "dev"
                         :user        nil
                         :data        d2}}
               (eph/get-room-data movies-app-id rid)))))))

(deftest set-presence-two-sessions
  (with-session
    (fn [_store {socket-1 :socket
                 socket-2 :socket-2
                 movies-app-id :movies-app-id}]
      (let [rid       (str (UUID/randomUUID))
            sess-id-1 (:id socket-1)
            sess-id-2 (:id socket-2)
            d1        {:a "a" :b "b" :c "c" :d "d" :e "e"}
            d2        {:a "a" :b "b" :c "c" :e "E" :f "F"}
            versions  {session/core-version-key "0.17.6"}]
        ;; socket-1 joining
        (send-msg socket-1 {:op :init, :app-id movies-app-id, :versions versions})
        (is (= :init-ok (:op (read-msg socket-1))))

        (send-msg socket-1 {:op :join-room, :room-id rid})
        (is (= #{{:op :join-room-ok, :room-id rid}
                 {:op :refresh-presence, :room-id rid, :data {sess-id-1 {:data {}, :instance-id "dev", :peer-id sess-id-1, :user nil}}}}
               (read-msgs 2 socket-1)))

        (send-msg socket-1 {:op :set-presence, :room-id rid, :data d1})
        (is (= #{{:op :set-presence-ok, :room-id rid}
                 {:op :patch-presence, :room-id rid, :edits [[[sess-id-1 :data] :r d1]]}}
               (read-msgs 2 socket-1)))

        ;; socket-2 joining
        (send-msg socket-2 {:op :init, :app-id movies-app-id, :versions versions})
        (is (= :init-ok (:op (read-msg socket-2))))

        (send-msg socket-2 {:op :join-room, :room-id rid})
        (is (= #{{:op :join-room-ok, :room-id rid}
                 {:op      :refresh-presence
                  :room-id rid
                  :data    {sess-id-1 {:data        d1
                                       :peer-id     sess-id-1
                                       :instance-id "dev"
                                       :user        nil}
                            sess-id-2 {:data        {}
                                       :peer-id     sess-id-2
                                       :instance-id "dev"
                                       :user        nil}}}}
               (read-msgs 2 socket-2)))
        (is (= {:op      :patch-presence
                :room-id rid
                :edits   [[[sess-id-2] :+ {:data        {}
                                           :peer-id     sess-id-2
                                           :instance-id "dev"
                                           :user        nil}]]}
               (read-msg socket-1)))

        ;; socket-1 updating
        (send-msg socket-1 {:op :set-presence, :room-id rid, :data d2})
        (is (= #{{:op      :set-presence-ok
                  :room-id rid}
                 {:op      :patch-presence
                  :room-id rid
                  :edits   [[[sess-id-1 :data :d] :-]
                            [[sess-id-1 :data :e] :r "E"]
                            [[sess-id-1 :data :f] :+ "F"]]}}
               (read-msgs 2 socket-1)))
        (is (= {:op      :patch-presence
                :room-id rid
                :edits   [[[sess-id-1 :data :d] :-]
                          [[sess-id-1 :data :e] :r "E"]
                          [[sess-id-1 :data :f] :+ "F"]]}
               (read-msg socket-2)))

        ;; socket-2 leaving
        (send-msg socket-2 {:op :leave-room, :room-id rid})
        (is (= {:op      :leave-room-ok
                :room-id rid}
               (read-msg socket-2)))
        (is (= {:op      :patch-presence
                :room-id rid
                :edits   [[[sess-id-2] :-]]}
               (read-msg socket-1)))

        ;; socket-1 updating
        (send-msg socket-1 {:op :set-presence, :room-id rid, :data d1})
        (is (= #{{:op      :set-presence-ok
                  :room-id rid}
                 {:op      :patch-presence
                  :room-id rid
                  :edits   [[[sess-id-1 :data :e] :r "e"]
                            [[sess-id-1 :data :f] :-]
                            [[sess-id-1 :data :d] :+ "d"]]}}
               (read-msgs 2 socket-1)))))))

(deftest set-presence-fails-when-not-in-room
  (with-session
    (fn [_store {:keys [socket movies-app-id]}]
      (blocking-send-msg :init-ok socket {:op :init :app-id movies-app-id})
      (let [rid (str (UUID/randomUUID))
            d1 {:hello "world"}
            {:keys [op status]} (blocking-send-msg :error socket {:op :set-presence :room-id rid :data d1})]
        (is (= :error op))
        (is (= 400 status))))))

(deftest broadcast-works
  (with-session
    (fn [_store {:keys [socket movies-app-id]}]
      (let [rid (str (UUID/randomUUID))
            sess-id (:id socket)
            t1 "foo"
            d1 {:hello "world"}]
        (blocking-send-msg :init-ok socket {:op :init :app-id movies-app-id})
        (is (= :join-room-ok (:op (blocking-send-msg :join-room-ok socket {:op :join-room, :room-id rid}))))
        (is (= :refresh-presence (:op (read-msg socket))))

        (let [room-data (eph/get-room-data movies-app-id rid)
              {:keys [op room-id topic data]}
              (blocking-send-msg :client-broadcast-ok
                                 socket
                                 {:op :client-broadcast
                                  :room-id rid
                                  :topic t1
                                  :data d1})]

          ;; broadcast is okay
          (is (= :client-broadcast-ok op))
          (is (= rid room-id))
          (is (= t1 topic))
          (is (= {:peer-id sess-id
                  :user nil
                  :data d1} data))

          ;; Rooms should be unchanged after broadcast
          (is (= room-data
                 (eph/get-room-data movies-app-id rid))))))))

(deftest broadcast-fails-when-not-in-room
  (with-session
    (fn [_store {:keys [socket movies-app-id]}]
      (blocking-send-msg :init-ok socket {:op :init :app-id movies-app-id})
      (let [rid (str (UUID/randomUUID))
            t1 "foo"
            d1 {:hello "world"}
            {:keys [op status]} (blocking-send-msg :error socket {:op :client-broadcast
                                                                  :room-id rid
                                                                  :topic t1
                                                                  :data d1})]
        (is (= :error op))
        (is (= 400 status))))))

;; -------
;; Streams

(defn with-file-mock [f]
  (let [files (atom {})
        upload (fn [_bucket {:keys [object-key]} file]
                 (let [ba (with-open [_in ^InputStream file
                                      out (ByteArrayOutputStream.)]
                            (io/copy file out)
                            (.toByteArray out))]
                   (swap! files assoc object-key ba)))
        get-metadata (fn [_bucket key]
                       {:size (alength ^bytes (get @files key))})]
    (test-util/with-s3-mock {:upload upload
                             :get-object-metadata get-metadata
                             :location-id-url (fn [app-id location-id]
                                                (str "https://example.com/" app-id "/" location-id))}
      (f {:slurp-file (fn [app-id location-id]
                        (-> @files
                            (get (s3/->object-key app-id location-id))
                            slurp))}))))

(defn read-full-stream
  "Reads the full stream, including any unflushed bytes from the buffer"
  [store app-id stream-id slurp-file]
  (let [stream-files (app-stream-model/get-stream-files {:app-id app-id
                                                         :stream-id stream-id})
        file-content (apply str (map (fn [{:keys [location-id]}]
                                       (slurp-file app-id location-id))
                                     stream-files))
        buffer-content (some->> (rs/get-stream-object-for-subscribe store app-id stream-id)
                                deref
                                :buffer
                                (map (fn [^bytes ba]
                                       (String. ba "UTF-8")))
                                (apply str))]
    (str file-content buffer-content)))

(deftest streams-writer
  (with-file-mock
    (fn [{:keys [slurp-file]}]
      (with-session
        (fn [store {:keys [socket movies-app-id]}]
          (rule-model/put! {:app-id movies-app-id
                            :code {:$streams {:allow {:create "true" :view "true"}}}})
          (blocking-send-msg :init-ok socket {:op :init :app-id movies-app-id})
          (let [event-id (random-uuid)
                {:keys [stream-id]}
                (blocking-send-msg :start-stream-ok socket {:op :start-stream
                                                            :client-event-id event-id
                                                            :client-id "stream-1"
                                                            :reconnect-token (str (random-uuid))})]

            (is (uuid? stream-id))

            (send-msg socket {:op :append-stream
                              :stream-id (str stream-id)
                              :chunks ["Hello"]
                              :offset 0
                              :done false})

            (is (= "Hello" (read-full-stream store movies-app-id stream-id slurp-file)))

            (send-msg socket {:op :append-stream
                              :stream-id (str stream-id)
                              :chunks [" "]
                              :offset (count (.getBytes "Hello" "UTF-8"))
                              :done false})

            (is (= "Hello " (read-full-stream store movies-app-id stream-id slurp-file)))

            (let [{:keys [offset done]}
                  (blocking-send-msg :stream-flushed socket {:op :append-stream
                                                             :stream-id (str stream-id)
                                                             :chunks ["World"]
                                                             :offset (count (.getBytes "Hello " "UTF-8"))
                                                             :done true})
                  stream (app-stream-model/get-stream {:app-id movies-app-id
                                                       :client-id "stream-1"})
                  stream-files (app-stream-model/get-stream-files {:app-id movies-app-id
                                                                   :stream-id stream-id})]

              (is (= offset (count (.getBytes "Hello World" "UTF-8"))))
              (is done)
              (is (= (:size stream) offset))
              (is (:done stream))
              (is (= 1 (count stream-files)))
              (is (= offset (:size (first stream-files))))
              (is (= "Hello World" (read-full-stream store movies-app-id stream-id slurp-file)))
              (is (= "Hello World" (slurp-file movies-app-id (:location-id (first stream-files))))))))))))

(deftest streams-writer-flushes-to-files
  (with-redefs [flags/stream-flush-byte-limit (constantly 5)]
    (with-file-mock
      (fn [{:keys [slurp-file]}]
        (with-session
          (fn [store {:keys [socket movies-app-id]}]
            (rule-model/put! {:app-id movies-app-id
                              :code {:$streams {:allow {:create "true" :view "true"}}}})
            (blocking-send-msg :init-ok socket {:op :init :app-id movies-app-id})
            (let [event-id (random-uuid)
                  {:keys [stream-id]}
                  (blocking-send-msg :start-stream-ok socket {:op :start-stream
                                                              :client-event-id event-id
                                                              :client-id "stream-1"
                                                              :reconnect-token (random-uuid)})

                  {:keys [offset done]}
                  (blocking-send-msg :stream-flushed socket {:op :append-stream
                                                             :stream-id (str stream-id)
                                                             :chunks ["Hello"]
                                                             :offset 0})

                  stream-files (app-stream-model/get-stream-files {:app-id movies-app-id
                                                                   :stream-id stream-id})
                  _ (is (= "Hello" (read-full-stream store movies-app-id stream-id slurp-file)))
                  _ (is (= offset 5))
                  _ (is (= done false))
                  _ (is (= 1 (count stream-files)))
                  _ (is (= "Hello" (slurp-file movies-app-id (:location-id (first stream-files)))))

                  {:keys [offset done]}
                  (blocking-send-msg :stream-flushed socket {:op :append-stream
                                                             :stream-id (str stream-id)
                                                             :chunks ["World"]
                                                             :offset 5})
                  stream-files (app-stream-model/get-stream-files {:app-id movies-app-id
                                                                   :stream-id stream-id})

                  _ (is (= "HelloWorld" (read-full-stream store movies-app-id stream-id slurp-file)))

                  _ (is (= offset 10))
                  _ (is (= done false))
                  _ (is (= 2 (count stream-files)))
                  _ (is (= "World" (slurp-file movies-app-id (:location-id (second stream-files)))))

                  {:keys [offset done]}
                  (blocking-send-msg :stream-flushed socket {:op :append-stream
                                                             :stream-id (str stream-id)
                                                             :chunks ["Bye!"]
                                                             :offset 10
                                                             :done true})
                  stream-files (app-stream-model/get-stream-files {:app-id movies-app-id
                                                                   :stream-id stream-id})]

              (is (= offset 14))
              (is (= done true))
              (is (= 3 (count stream-files)))
              (is (= "HelloWorldBye!" (read-full-stream store movies-app-id stream-id slurp-file)))
              (is (= "Bye!" (slurp-file movies-app-id (:location-id (last stream-files))))))))))))

(deftest streams-writer-can-restart
  (with-redefs [flags/stream-flush-byte-limit (constantly 5)]
    (with-file-mock
      (fn [{:keys [slurp-file]}]
        (with-session
          (fn [store {:keys [socket socket-2 socket-3 movies-app-id]}]
            (rule-model/put! {:app-id movies-app-id
                              :code {:$streams {:allow {:create "true" :view "true"}}}})
            (blocking-send-msg :init-ok socket {:op :init :app-id movies-app-id})
            (blocking-send-msg :init-ok socket-2 {:op :init :app-id movies-app-id})
            (blocking-send-msg :init-ok socket-3 {:op :init :app-id movies-app-id})
            (let [event-id (random-uuid)
                  reconnect-token (str (random-uuid))
                  {:keys [stream-id]}
                  (blocking-send-msg :start-stream-ok socket {:op :start-stream
                                                              :client-event-id event-id
                                                              :client-id "stream-1"
                                                              :reconnect-token reconnect-token})


                  ;; Make sure part of the buffer gets flushed to a file
                  _ (blocking-send-msg :stream-flushed socket {:op :append-stream
                                                               :stream-id (str stream-id)
                                                               :chunks ["Hello"]
                                                               :offset 0})
                  ;; Make sure we still have some data buffered
                  _ (send-msg socket {:op :append-stream
                                      :stream-id (str stream-id)
                                      :chunks ["ABC"]
                                      :offset 5})

                  _ (is (= "HelloABC" (read-full-stream store movies-app-id stream-id slurp-file)))
                  _ (is (= 1 (count (app-stream-model/get-stream-files {:app-id movies-app-id
                                                                        :stream-id stream-id}))))

                  ;; simulate client going away
                  _ (session/on-close store socket)

                  _ (test-util/wait-for (fn []
                                          (= 2 (count (app-stream-model/get-stream-files {:app-id movies-app-id
                                                                                          :stream-id stream-id}))))

                                        1000)

                  {:keys [offset]}
                  (blocking-send-msg :start-stream-ok socket-2 {:op :start-stream
                                                                :client-id "stream-1"
                                                                :reconnect-token reconnect-token})
                  _ (is (= offset 8))

                  _ (send-msg socket-2 {:op :append-stream
                                        :stream-id (str stream-id)
                                        :chunks ["DEF"]
                                        :offset 8})
                  _ (is (= "HelloABCDEF" (read-full-stream store movies-app-id stream-id slurp-file)))]

              (testing "if someone steals our session, we can't write to it"
                (blocking-send-msg :start-stream-ok socket-3 {:op :start-stream
                                                              :client-id "stream-1"
                                                              :reconnect-token reconnect-token})
                (blocking-send-msg :error socket-2 {:op :append-stream
                                                    :client-id "stream-1"
                                                    :chunks ["GHI"]
                                                    :offset 8})

                ;; socket-3 can write to it now
                (send-msg socket-3 {:op :append-stream
                                    :stream-id (str stream-id)
                                    :chunks ["DEF"]
                                    :offset 8})
                (is (= "HelloABCDEF" (read-full-stream store movies-app-id stream-id slurp-file)))))))))))

(deftest streams-writer-flushes-on-disconnect
  (with-redefs [flags/stream-flush-byte-limit (constantly 10)]
    (with-file-mock
      (fn [{:keys [slurp-file]}]
        (with-session
          (fn [store {:keys [socket socket-2 socket-3 movies-app-id]}]
            (rule-model/put! {:app-id movies-app-id
                              :code {:$streams {:allow {:create "true" :view "true"}}}})
            (blocking-send-msg :init-ok socket {:op :init :app-id movies-app-id})
            (blocking-send-msg :init-ok socket-2 {:op :init :app-id movies-app-id})
            (blocking-send-msg :init-ok socket-3 {:op :init :app-id movies-app-id})
            (let [event-id (random-uuid)
                  reconnect-token (str (random-uuid))
                  {:keys [stream-id]}
                  (blocking-send-msg :start-stream-ok socket {:op :start-stream
                                                              :client-event-id event-id
                                                              :client-id "stream-1"
                                                              :reconnect-token reconnect-token})


                  ;; Log some buffered data
                  _ (send-msg socket {:op :append-stream
                                      :stream-id (str stream-id)
                                      :chunks ["Hello"]
                                      :offset 0})

                  _ (is (= "Hello" (read-full-stream store movies-app-id stream-id slurp-file)))
                  _ (is (= 0 (count (app-stream-model/get-stream-files {:app-id movies-app-id
                                                                        :stream-id stream-id}))))

                  ;; simulate client going away
                  _ (session/on-close store socket)

                  ;; Check that the data was flushed to $files
                  _ (test-util/wait-for
                     (fn []
                       (= 1 (count (app-stream-model/get-stream-files {:app-id movies-app-id
                                                                       :stream-id stream-id}))))
                     1000)

                  _ (is (= "Hello" (read-full-stream store movies-app-id stream-id slurp-file)))])))))))

(deftest on-close-fires-when-send-hits-closed-connection
  (testing "ClosedChannelException from ws/send-json! triggers on-close"
    (with-session
      (fn [store {:keys [socket zeneca-app-id]
                  {:keys [id]} :socket}]
        (is (some? (rs/session store id)))
        (with-redefs [ws/send-json! (fn [_app-id _msg _conn]
                                      (throw (ClosedChannelException.)))]
          (send-msg socket {:op :init :app-id zeneca-app-id}))
        (test-util/wait-for #(nil? (rs/session store id)) 1000)
        (is (nil? (rs/session store id))))))

  (testing "UT002002 IOException (ws channel closed) triggers on-close"
    (with-session
      (fn [store {:keys [socket zeneca-app-id]
                  {:keys [id]} :socket}]
        (with-redefs [ws/send-json! (fn [_app-id _msg _conn]
                                      (throw (IOException. "UT002002: Channel is closed")))]
          (send-msg socket {:op :init :app-id zeneca-app-id}))
        (test-util/wait-for #(nil? (rs/session store id)) 1000)
        (is (nil? (rs/session store id))))))

  (testing "UT002027 IOException (ws connection broken) triggers on-close"
    (with-session
      (fn [store {:keys [socket zeneca-app-id]
                  {:keys [id]} :socket}]
        (with-redefs [ws/send-json! (fn [_app-id _msg _conn]
                                      (throw (IOException. "UT002027: connection broken")))]
          (send-msg socket {:op :init :app-id zeneca-app-id}))
        (test-util/wait-for #(nil? (rs/session store id)) 1000)
        (is (nil? (rs/session store id))))))

  (testing "UT000041 IOException (generic channel closed) triggers on-close"
    (with-session
      (fn [store {:keys [socket zeneca-app-id]
                  {:keys [id]} :socket}]
        (with-redefs [ws/send-json! (fn [_app-id _msg _conn]
                                      (throw (IOException. "UT000041: channel closed")))]
          (send-msg socket {:op :init :app-id zeneca-app-id}))
        (test-util/wait-for #(nil? (rs/session store id)) 1000)
        (is (nil? (rs/session store id))))))

  (testing "unrelated IOException does NOT trigger on-close"
    (with-session
      (fn [store {:keys [socket zeneca-app-id]
                  {:keys [id]} :socket}]
        (with-redefs [ws/send-json! (fn [_app-id _msg _conn]
                                      (throw (IOException. "something else went wrong")))]
          (send-msg socket {:op :init :app-id zeneca-app-id}))
        ;; Poll: session must remain present for the entire observation window.
        (let [deadline (+ (System/currentTimeMillis) 500)]
          (loop []
            (let [s (rs/session store id)]
              (cond
                (nil? s)
                (is (some? s) "session was cleaned up during observation window")

                (< (System/currentTimeMillis) deadline)
                (do (Thread/sleep 50) (recur))

                :else
                (is (some? s))))))))))

(deftest transact-combiner-collapses-redundant-events
  (with-movies-app
    (fn [{app-id :id} r]
      (with-session
        (fn [_store {:keys [socket]}]
          (blocking-send-msg :init-ok socket {:op :init :app-id app-id})
          (let [eid (resolvers/->uuid r "eid-robocop")
                attr-id (resolvers/->uuid r :movie/title)
                a-entered (promise)
                release (promise)
                call-count (atom 0)
                orig-handle session/handle-transact!
                event (fn [client-id v]
                        {:op :transact
                         :session-id (:id socket)
                         :client-event-id client-id
                         :tx-steps [["add-triple" eid attr-id v {:mode :upsert}]]})]
            (with-redefs [session/handle-transact!
                          (fn [store sess-id evt]
                            (swap! call-count inc)
                            (when (= "client-a" (:client-event-id evt))
                              (deliver a-entered :go)
                              (deref release 2000 :timeout))
                            (orig-handle store sess-id evt))]
              ;; Put client-a alone, then wait until the worker has polled it
              ;; out and is blocked inside handle-transact!. That guarantees
              ;; b and c will land in the queue together and combine.
              (receive-queue/put! (event "client-a" "value-a"))
              (is (not= :timeout (deref a-entered 2000 :timeout))
                  "worker should enter handle-transact! for client-a")

              (receive-queue/put! (event "client-b" "value-b"))
              (receive-queue/put! (event "client-c" "value-c"))
              (deliver release :go)

              (let [msgs (vec (repeatedly 3
                                          #(let [m (ua/<!!-timeout (:ws-conn socket))]
                                             (when (= :timeout m)
                                               (throw (ex-info "timeout waiting for ack" {})))
                                             m)))]
                (testing "every client-event-id received a transact-ok"
                  (is (every? #(= :transact-ok (:op %)) msgs))
                  (is (= #{"client-a" "client-b" "client-c"}
                         (set (map :client-event-id msgs)))))
                (testing "client-b and client-c share a tx-id (combined run)"
                  (let [by-id (into {} (map (juxt :client-event-id :tx-id) msgs))]
                    (is (= (get by-id "client-b") (get by-id "client-c")))
                    (is (not= (get by-id "client-a") (get by-id "client-b"))
                        "client-a ran alone, so its tx-id differs"))))

              (testing "handle-transact! ran twice (a alone, b+c combined)"
                (is (= 2 @call-count)))

              (testing "value-c (event2 of the combined batch) is what's persisted"
                (let [resp   (blocking-send-msg :add-query-ok
                                                socket {:op :add-query
                                                        :return-type :tree
                                                        :q {:movie {:$ {:where {:id (str eid)}}}}})
                      title (-> resp
                                :result
                                (get "movie")
                                first
                                (get "title"))]
                  (is (= title "value-c")))))))))))

(deftest transact-combiner-fans-out-errors
  (with-movies-app
    (fn [{app-id :id} r]
      (with-session
        (fn [_store {:keys [socket]}]
          (blocking-send-msg :init-ok socket {:op :init :app-id app-id})
          (let [eid (resolvers/->uuid r "eid-robocop")
                attr-id (resolvers/->uuid r :movie/title)
                a-entered (promise)
                release (promise)
                call-count (atom 0)
                orig-handle session/handle-transact!
                event (fn [client-id v]
                        {:op :transact
                         :session-id (:id socket)
                         :client-event-id client-id
                         :tx-steps [["add-triple" eid attr-id v {:mode :upsert}]]})]
            (with-redefs [permissioned-tx/transact!
                          (fn [_ctx _coerced]
                            (ex/throw-validation-err! :tx-steps [] [{:message "test boom"}]))

                          session/handle-transact!
                          (fn [store sess-id evt]
                            (swap! call-count inc)
                            (when (= "client-a" (:client-event-id evt))
                              (deliver a-entered :go)
                              (deref release 2000 :timeout))
                            (orig-handle store sess-id evt))]
              (receive-queue/put! (event "client-a" "value-a"))
              (is (not= :timeout (deref a-entered 2000 :timeout))
                  "worker should enter handle-transact! for client-a")
              (receive-queue/put! (event "client-b" "value-b"))
              (receive-queue/put! (event "client-c" "value-c"))
              (deliver release :go)

              ;; Three errors: one for client-a (ran alone), then two from
              ;; the combined b+c run fanned out to each client-event-id.
              (let [errs (vec (repeatedly 3
                                          #(let [m (ua/<!!-timeout (:ws-conn socket))]
                                             (when (= :timeout m)
                                               (throw (ex-info "timeout waiting for error" {})))
                                             m)))
                    by-id (into {} (map (juxt :client-event-id identity) errs))]
                (testing "each client-event-id received its own :error"
                  (is (every? #(= :error (:op %)) errs))
                  (is (= #{"client-a" "client-b" "client-c"}
                         (set (keys by-id))))
                  (is (every? #(= :validation-failed (:type %)) errs)))

                (testing "every error's :original-event matches the client that sent it"
                  (doseq [[client-id expected-value] [["client-a" "value-a"]
                                                      ["client-b" "value-b"]
                                                      ["client-c" "value-c"]]]
                    (let [oe (:original-event (get by-id client-id))]
                      (is (= client-id (:client-event-id oe))
                          (str "original-event.client-event-id mismatch for " client-id))
                      (is (= expected-value
                             (-> oe :tx-steps first (nth 3)))
                          (str "tx-steps should hold this client's own value for " client-id))
                      (is (not (contains? oe :redundant-events))
                          (str ":redundant-events must be stripped for " client-id))))))

              (testing "handle-transact! ran twice (a alone, b+c combined)"
                (is (= 2 @call-count))))))))))

(deftest transact-combiner-skips-has-many-refs
  (with-zeneca-app
    (fn [{app-id :id} r]
      (with-session
        (fn [_store {:keys [socket]}]
          (blocking-send-msg :init-ok socket {:op :init :app-id app-id})
          (let [shelf (resolvers/->uuid r "eid-the-way-of-the-gentleman")
                books-attr (resolvers/->uuid r :bookshelves/books)
                ;; Three books that are NOT already on the Worldview shelf
                book-a (resolvers/->uuid r "eid-hackers-&-painters")
                book-b (resolvers/->uuid r "eid-antifragile")
                book-c (resolvers/->uuid r "eid-fooled-by-randomness")
                a-entered (promise)
                release (promise)
                call-count (atom 0)
                orig-handle session/handle-transact!
                event (fn [client-id target]
                        {:op :transact
                         :session-id (:id socket)
                         :client-event-id client-id
                         :tx-steps [["add-triple" shelf books-attr target]]})]
            (with-redefs [session/handle-transact!
                          (fn [store sess-id evt]
                            (swap! call-count inc)
                            (when (= "client-a" (:client-event-id evt))
                              (deliver a-entered :go)
                              (deref release 2000 :timeout))
                            (orig-handle store sess-id evt))]
              (receive-queue/put! (event "client-a" book-a))
              (is (not= :timeout (deref a-entered 2000 :timeout))
                  "worker should enter handle-transact! for client-a")
              ;; b and c land in the queue together; they would combine if
              ;; matching-steps? said yes. For has-many refs, each add carries
              ;; a different target value, so combine-transact must reject.
              (receive-queue/put! (event "client-b" book-b))
              (receive-queue/put! (event "client-c" book-c))
              (deliver release :go)

              (let [msgs (vec (repeatedly 3
                                          #(let [m (ua/<!!-timeout (:ws-conn socket))]
                                             (when (= :timeout m)
                                               (throw (ex-info "timeout waiting for ack" {})))
                                             m)))
                    by-id (into {} (map (juxt :client-event-id identity) msgs))]
                (testing "every add got its own transact-ok"
                  (is (every? #(= :transact-ok (:op %)) msgs))
                  (is (= #{"client-a" "client-b" "client-c"} (set (keys by-id)))))

                (testing "tx-ids are all distinct — no two adds collapsed"
                  (is (= 3 (count (set (map :tx-id (vals by-id))))))))

              (testing "handle-transact! ran three times (no combining)"
                (is (= 3 @call-count)))

              (testing "all three refs were persisted"
                (let [resp     (blocking-send-msg
                                :add-query-ok
                                socket {:op :add-query
                                        :return-type :tree
                                        :q {:bookshelves {:$ {:where {:id (str shelf)}}
                                                          :books {}}}})
                      book-ids (->> resp
                                    :result
                                    (#(get % "bookshelves"))
                                    first
                                    (#(get % "books"))
                                    (map #(get % "id"))
                                    set)]
                  (is (contains? book-ids (str book-a)))
                  (is (contains? book-ids (str book-b)))
                  (is (contains? book-ids (str book-c))))))))))))

;; ----------------------------------------------
;; Which writes refresh a permissioned query

(defn- entity-topics
  "The topics that the invalidator produces for the stored triples of `eids`."
  [app-id eids]
  (->> (sql/select ::entity-topics
                   (aurora/conn-pool :read)
                   ["select entity_id, attr_id, cast(value as text) as value,
                            cast(checked_data_type as text) as checked_data_type,
                            ea, eav, av, ave, vae
                       from triples
                      where app_id = ?::uuid and entity_id = any(?::uuid[])"
                    app-id
                    (with-meta (vec eids) {:pgtype "uuid[]"})])
       (map (fn [row]
              [(set (filter row [:ea :eav :av :ave :vae]))
               #{(:entity_id row)}
               #{(:attr_id row)}
               #{(topics/parse-v row)}]))
       set))

(defn- transact-topics!
  "Transacts `tx-steps`, which must only change triples of `eids`, and returns
   the topics that the invalidator would produce for the change."
  [app-id eids tx-steps]
  (let [before (entity-topics app-id eids)
        _ (tx/transact! (aurora/conn-pool :write)
                        (attr-model/get-by-app-id app-id)
                        app-id
                        tx-steps)
        after (entity-topics app-id eids)]
    (set/difference (set/union before after)
                    (set/intersection before after))))

(defn- refreshed-by-write?
  "Subscribes to `q`, makes the write, and returns whether the write marked `q`
   stale. Unsubscribes afterwards so that the next check starts fresh."
  [store {:keys [socket app-id tx-counter]} q [eids tx-steps]]
  (let [sess-id (:id socket)
        _ (blocking-send-msg :add-query-ok socket {:op :add-query :q q})
        _ (is (empty? (rs/get-stale-instaql-queries store app-id sess-id)))
        topics (transact-topics! app-id eids tx-steps)
        _ (is (seq topics) "the write must change a triple")
        tx-id (swap! tx-counter inc)
        _ (rs/mark-stale-topics! store app-id tx-id (instant.isn/test-isn tx-id) topics {})
        stale? (contains? (set (map :instaql-query/query
                                    (rs/get-stale-instaql-queries store app-id sess-id)))
                          q)]
    (blocking-send-msg :remove-query-ok socket {:op :remove-query :q q})
    stale?))

(defn- refreshed-by?
  "`refreshed-by-write?` for a write to one entity, as `[e a tx-steps]`."
  [store sess q [e _a tx-steps]]
  (refreshed-by-write? store sess q [[e] tx-steps]))

(defn- with-permissioned-session [rules f]
  (with-session
    (fn [store {:keys [socket zeneca-app-id zeneca-resolver]}]
      (rule-model/put! {:app-id zeneca-app-id :code rules})
      (blocking-send-msg :init-ok socket {:op :init :app-id zeneca-app-id})
      (rs/transact! "store/reset-tx-id"
                    (rs/app-conn store zeneca-app-id)
                    [[:db.fn/call (fn [db]
                                    [[:db/add (ds/entid db [:tx-meta/app-id zeneca-app-id]) :tx-meta/processed-tx-id 0]
                                     [:db/add (ds/entid db [:tx-meta/app-id zeneca-app-id]) :tx-meta/processed-isn (instant.isn/test-isn 0)]])]])
      (f store
         {:socket socket
          :app-id zeneca-app-id
          :tx-counter (atom 0)}
         zeneca-resolver))))

(defn- set-value [r eid attr v]
  (let [e (resolvers/->uuid r eid)
        a (resolvers/->uuid r attr)]
    [e a [[:add-triple e a v]]]))

(deftest view-rule-fields-refresh-queries-with-fields
  (with-permissioned-session
    {:users {:allow {:view "data.handle != 'joe'"}}}
    (fn [store sess r]
      (let [q {:users {:$ {:fields ["fullName"]}}}
            refreshed? (partial refreshed-by? store sess q)]
        (testing "a field that the query doesn't fetch and the rule doesn't read"
          (is (not (refreshed? (set-value r "eid-alex" :users/email "alex@example.com")))))
        (testing "a field that the rule reads"
          (is (refreshed? (set-value r "eid-alex" :users/handle "alex2"))))
        (testing "a field that the query fetches"
          (is (refreshed? (set-value r "eid-alex" :users/fullName "Alex 2"))))
        (testing "an unread field of a row that the rule hides"
          (is (not (refreshed? (set-value r "eid-joe-averbukh" :users/email "joe@example.com")))))
        (testing "a rule field of a hidden row, so the row can appear"
          (is (refreshed? (set-value r "eid-joe-averbukh" :users/handle "joe2"))))))))

(deftest view-rule-fields-without-query-fields
  (with-permissioned-session
    {:users {:allow {:view "data.handle != 'joe'"}}}
    (fn [store sess r]
      (testing "a query without :fields still refreshes on any field"
        (is (refreshed-by? store sess {:users {}}
                           (set-value r "eid-alex" :users/email "alex@example.com")))))))

(deftest field-rule-fields-refresh-queries-with-fields
  (with-permissioned-session
    {:users {:allow {:view "true"}
             :fields {:email "data.fullName != 'Hidden'"}}}
    (fn [store sess r]
      (let [q {:users {:$ {:fields ["email"]}}}
            refreshed? (partial refreshed-by? store sess q)]
        (testing "a field that only the field rule reads"
          (is (refreshed? (set-value r "eid-alex" :users/fullName "Hidden"))))
        (testing "a field that no rule reads"
          (is (not (refreshed? (set-value r "eid-alex" :users/createdAt "2020-01-01 00:00:00.000000")))))))))

(deftest ref-rule-paths-refresh-queries-with-fields
  (with-permissioned-session
    {:bookshelves {:allow {:view "'alex' in data.ref('users.handle')"}}}
    (fn [store sess r]
      (let [q {:bookshelves {:$ {:fields ["order"]}}}
            refreshed? (partial refreshed-by? store sess q)
            alex (resolvers/->uuid r "eid-alex")
            stopa (resolvers/->uuid r "eid-stepan-parunashvili")
            user-shelves (resolvers/->uuid r :users/bookshelves)
            shelf (resolvers/->uuid r "eid-nonfiction")]
        (testing "a field of the row that the rule doesn't read"
          (is (not (refreshed? (set-value r "eid-nonfiction" :bookshelves/desc "changed")))))
        (testing "the field at the end of the ref path"
          (is (refreshed? (set-value r "eid-alex" :users/handle "alex2")))
          (is (refreshed? (set-value r "eid-alex" :users/handle "alex"))))
        (testing "another field of a linked entity"
          (is (not (refreshed? (set-value r "eid-alex" :users/email "alex@example.com")))))
        (testing "linking an entity on the ref path"
          (is (refreshed? [stopa user-shelves [[:add-triple stopa user-shelves shelf]]])))
        (testing "unlinking an entity on the ref path"
          (is (refreshed? [alex user-shelves [[:retract-triple alex user-shelves shelf]]])))))))

(deftest unnarrowable-rules-refresh-on-any-field
  (with-permissioned-session
    {:books {:allow {:view "data['isbn13'] != ''"}}}
    (fn [store sess r]
      (testing "a rule that indexes data falls back to every field"
        (is (refreshed-by? store sess {:books {:$ {:fields ["title"]}}}
                           (set-value r "eid-catch-and-kill" :books/pageCount 1)))))))

(defn- create-entities!
  "Creates an entity with `id-attr` for each of `eids`, plus any `tx-steps`."
  [{:keys [app-id]} r id-attr eids tx-steps]
  (tx/transact! (aurora/conn-pool :write)
                (attr-model/get-by-app-id app-id)
                app-id
                (concat (for [e eids]
                          [:add-triple e (resolvers/->uuid r id-attr) (str e)])
                        tx-steps)))

(deftest ref-rule-paths-refresh-for-partial-matches
  (testing "the first link on a path, for an entity that has no link yet"
    (with-permissioned-session
      {:bookshelves {:allow {:view "'alex' in data.ref('users.handle')"}}}
      (fn [store sess r]
        (let [alex (resolvers/->uuid r "eid-alex")
              user-shelves (resolvers/->uuid r :users/bookshelves)
              shelf (random-uuid)]
          (create-entities! sess r :bookshelves/id [shelf] [])
          (is (refreshed-by? store sess {:bookshelves {:$ {:fields ["order"]}}}
                             [alex user-shelves [[:add-triple alex user-shelves shelf]]]))))))
  (testing "a later link on a path, for an entity that has only the first link"
    (with-permissioned-session
      {:books {:allow {:view "'alex' in data.ref('bookshelves.users.handle')"}}}
      (fn [store sess r]
        (let [alex (resolvers/->uuid r "eid-alex")
              user-shelves (resolvers/->uuid r :users/bookshelves)
              shelf-books (resolvers/->uuid r :bookshelves/books)
              shelf (random-uuid)
              book (random-uuid)
              q {:books {:$ {:fields ["title"]}}}]
          (create-entities! sess r :books/id [book] [])
          (create-entities! sess r :bookshelves/id [shelf] [])
          (testing "linking the first entity"
            (is (refreshed-by? store sess q [shelf shelf-books [[:add-triple shelf shelf-books book]]])))
          (testing "linking the next entity"
            (is (refreshed-by? store sess q [alex user-shelves [[:add-triple alex user-shelves shelf]]])))
          (testing "a field that the rule doesn't read"
            (is (not (refreshed-by? store sess q (set-value r "eid-alex" :users/email "alex@example.com")))))
          (testing "a field of the row that the rule doesn't read"
            (is (not (refreshed-by? store sess q [book (resolvers/->uuid r :books/pageCount)
                                                  [[:add-triple book (resolvers/->uuid r :books/pageCount) 1]]])))))))))

;; ----------------------------------------------
;; Which writes refresh a query that names a link

(def ^:private early "2026-01-10T10:00:00Z")
(def ^:private cutoff "2026-02-01T00:00:00Z")
(def ^:private late "2026-03-10T10:00:00Z")

(defn- with-tasks-session
  "Calls `f` with a session on an app that has projects, tasks that link to
   a project, and notes that link to a task, and with:

     `store`      the session's store
     `sess`       the session, as `refreshed-by-write?` takes it
     `ids`        the attr ids by name, as `:tasks/status`
     `transact!`  transacts tx-steps
     `entity!`    creates an entity of an etype with the fields and returns its id
     `create`     the write that `entity!` makes, for an id
     `set-field`  the write that sets a field of an entity
     `refreshed?` `refreshed-by-write?` for a query and a write"
  [f]
  (with-session
    (fn [store {:keys [socket zeneca-app-id]}]
      (let [app-id zeneca-app-id
            ids (test-util/make-attrs
                 app-id
                 [[:projects/id :unique? :index?]
                  [:tasks/id :unique? :index?]
                  [:tasks/status :index? :string]
                  [:tasks/dueAt :index? :date]
                  [:tasks/doneAt :index? :date]
                  [:tasks/rank :index? :number]
                  [:tasks/title]
                  [[:tasks/project :projects/tasks]]
                  [:notes/id :unique? :index?]
                  [:notes/text :index? :string]
                  [[:notes/task :tasks/notes]]])
            sess {:socket socket :app-id app-id :tx-counter (atom 0)}
            create (fn [etype id fields]
                     [[id] (into [[:add-triple id (ids (keyword etype "id")) (str id)]]
                                 (for [[k v] fields]
                                   [:add-triple id (ids (keyword etype (name k))) v]))])
            transact! (fn [tx-steps]
                        (tx/transact! (aurora/conn-pool :write)
                                      (attr-model/get-by-app-id app-id)
                                      app-id
                                      tx-steps))
            entity! (fn [etype fields]
                      (let [id (random-uuid)]
                        (transact! (second (create etype id fields)))
                        id))]
        (blocking-send-msg :init-ok socket {:op :init :app-id app-id})
        (rs/transact! "store/reset-tx-id"
                      (rs/app-conn store app-id)
                      [[:db.fn/call (fn [db]
                                      [[:db/add (ds/entid db [:tx-meta/app-id app-id]) :tx-meta/processed-tx-id 0]
                                       [:db/add (ds/entid db [:tx-meta/app-id app-id]) :tx-meta/processed-isn (instant.isn/test-isn 0)]])]])
        (f {:store store
            :sess sess
            :ids ids
            :transact! transact!
            :entity! entity!
            :create create
            :set-field (fn [id attr v]
                         [[id] [[:add-triple id (ids attr) v]]])
            :refreshed? (fn [q write]
                          (refreshed-by-write? store sess q write))})))))

(deftest link-wheres-refresh-for-each-clause
  (with-tasks-session
    (fn [{:keys [entity! set-field refreshed?]}]
      (let [project (entity! "projects" {})
            other (entity! "projects" {})
            _ (entity! "tasks" {:project project :status "open"})
            link (str project)]
        (doseq [where [{:and [{:status "open"} {"project.id" link}]}
                       {:and [{"project.id" link} {:status "open"}]}
                       (array-map :status "open" "project.id" link)
                       (array-map "project.id" link :status "open")
                       {:status "open" :project link}]
                :let [q {:tasks {:$ {:where where}}}]]
          (testing (pr-str where)
            (testing "a task of the project comes into the list"
              (is (refreshed? q (set-field (entity! "tasks" {:project project :status "done"})
                                           :tasks/status "open"))))
            (testing "a task of another project doesn't"
              (is (not (refreshed? q (set-field (entity! "tasks" {:project other :status "done"})
                                                :tasks/status "open")))))))))))

(deftest link-wheres-refresh-for-what-enters-and-leaves
  (with-tasks-session
    (fn [{:keys [ids entity! create set-field refreshed?]}]
      (let [project (entity! "projects" {})
            other (entity! "projects" {})
            task! (fn [project fields]
                    (entity! "tasks" (assoc fields :project project)))
            held (task! project {:status "open" :dueAt early})
            q {:tasks {:$ {:where {:and [{:status {:$in ["open" "started"]}}
                                         {:dueAt {:$lt cutoff}}
                                         {"project.id" (str project)}]}}}}
            relink (fn [task project]
                     [[task] [[:add-triple task (:tasks/project ids) project]]])]
        (testing "a date moves into the range"
          (is (refreshed? q (set-field (task! project {:status "open" :dueAt late})
                                       :tasks/dueAt early)))
          (is (refreshed? {:tasks {:$ {:where {:and [{:status "open"}
                                                     {:dueAt {:$gte cutoff}}
                                                     {"project.id" (str project)}]}}}}
                          (set-field (task! project {:status "open" :dueAt early})
                                     :tasks/dueAt late))))
        (testing "a date moves out of the range"
          (is (refreshed? q (set-field (task! project {:status "open" :dueAt early})
                                       :tasks/dueAt late))))
        (testing "a task is linked to the project"
          (is (refreshed? q (relink (task! other {:status "open" :dueAt early}) project))))
        (testing "a task is linked to another project"
          (is (refreshed? q (relink (task! project {:status "open" :dueAt early}) other))))
        (testing "a task is unlinked from the project"
          (let [task (task! project {:status "open" :dueAt early})]
            (is (refreshed? q [[task] [[:retract-triple task (:tasks/project ids) project]]]))))
        (testing "a task is created in the project"
          (is (refreshed? q (create "tasks" (random-uuid) {:project project :status "open" :dueAt early}))))
        (testing "a task of the project is deleted"
          (let [task (task! project {:status "open" :dueAt early})]
            (is (refreshed? q [[task] [[:delete-entity task "tasks"]]]))))
        (testing "the project of a task is deleted"
          (let [gone (entity! "projects" {})
                task (task! gone {:status "open" :dueAt early})]
            (is (refreshed? {:tasks {:$ {:where {:and [{:status "open"} {"project.id" (str gone)}]}}}}
                            [[gone task] [[:delete-entity gone "projects"]]]))))
        (testing "a field that the where doesn't name"
          (is (refreshed? q (set-field held :tasks/title "Held")))
          (is (not (refreshed? q (set-field (task! project {:status "done" :dueAt early})
                                            :tasks/title "Not held")))))
        (testing "tasks of another project"
          (let [task (task! other {:status "open" :dueAt late})]
            (is (not (refreshed? q (set-field task :tasks/dueAt early))))
            (is (not (refreshed? q (set-field task :tasks/status "done"))))
            (is (not (refreshed? q [[task] [[:delete-entity task "tasks"]]])))
            (is (not (refreshed? q (create "tasks" (random-uuid) {:project other :status "open" :dueAt early}))))))
        (testing "$isNull"
          (let [q {:tasks {:$ {:where {:and [{:status "open"}
                                             {:doneAt {:$isNull false}}
                                             {"project.id" (str project)}]}}}}
                finish (fn [project]
                         (set-field (task! project {:status "open"}) :tasks/doneAt early))]
            (is (refreshed? q (finish project)))
            (is (not (refreshed? q (finish other))))))
        (testing "$isNull of a link from the other side"
          (let [q {:tasks {:$ {:where {:and [{:notes {:$isNull true}}
                                             {"project.id" (str project)}]}}}}
                note (entity! "notes" {:text "a"})]
            (is (refreshed? q [[note] [[:add-triple note (:notes/task ids) held]]]))))
        (testing "$in of links"
          (let [third (entity! "projects" {})
                q {:tasks {:$ {:where {:status "open"
                                       "project.id" {:$in [(str project) (str other)]}}}}}
                open (fn [project]
                       (set-field (task! project {:status "done"}) :tasks/status "open"))]
            (is (refreshed? q (open project)))
            (is (refreshed? q (open other)))
            (is (not (refreshed? q (open third))))))))))

(deftest link-wheres-refresh-with-ors
  (with-tasks-session
    (fn [{:keys [entity! set-field refreshed?]}]
      (let [project (entity! "projects" {})
            other (entity! "projects" {})
            link (str project)
            _ (entity! "tasks" {:project project :status "open" :dueAt early})
            open (fn [project]
                   (set-field (entity! "tasks" {:project project :status "done" :dueAt late})
                              :tasks/status "open"))
            due (fn [project]
                  (set-field (entity! "tasks" {:project project :status "done" :dueAt late})
                             :tasks/dueAt early))]
        (testing "an or of and-lists that each name the link"
          (let [q {:tasks {:$ {:where {:or [{:and [{:status "open"} {"project.id" link}]}
                                            {:and [{:dueAt {:$lt cutoff}} {"project.id" link}]}]}}}}]
            (is (refreshed? q (open project)))
            (is (refreshed? q (due project)))
            (is (not (refreshed? q (open other))))
            (is (not (refreshed? q (due other))))))
        (testing "an or beside the link"
          (let [q {:tasks {:$ {:where {:and [{"project.id" link}
                                             {:or [{:status "open"} {:dueAt {:$lt cutoff}}]}]}}}}]
            (is (refreshed? q (open project)))
            (is (refreshed? q (due project)))
            (is (not (refreshed? q (open other))))
            (is (not (refreshed? q (due other))))))
        (testing "an or of and-lists that name different links"
          (let [third (entity! "projects" {})
                q {:tasks {:$ {:where {:or [{:and [{:status "open"} {"project.id" link}]}
                                            {:and [{:dueAt {:$lt cutoff}} {"project.id" (str other)}]}]}}}}]
            (is (refreshed? q (open project)))
            (is (refreshed? q (due other)))
            (is (not (refreshed? q (open third))))
            (is (not (refreshed? q (due third))))))
        (testing "an or with a list that doesn't name the link is for every project"
          (let [q {:tasks {:$ {:where {:or [{:and [{:dueAt {:$lt cutoff}} {"project.id" link}]}
                                            {:status "open"}]}}}}]
            (is (refreshed? q (open other)))))))))

(deftest link-wheres-refresh-pages-children-and-paths
  (with-tasks-session
    (fn [{:keys [ids entity! create set-field refreshed?]}]
      (let [project (entity! "projects" {})
            other (entity! "projects" {})
            link (str project)]
        (testing "a list that is empty"
          (let [q {:tasks {:$ {:where {:and [{:status "started"} {"project.id" link}]}}}}
                start (fn [project]
                        (set-field (entity! "tasks" {:project project :status "done"})
                                   :tasks/status "started"))]
            (is (not (refreshed? q (start other))))
            (is (refreshed? q (start project)))))
        (testing "an ordered list with a limit"
          (let [q {:tasks {:$ {:where {"project.id" link}
                               :order {:rank :desc}
                               :limit 1}}}
                _ (entity! "tasks" {:project project :rank 5})
                _ (entity! "tasks" {:project project :rank 4})
                raise (fn [project]
                        (set-field (entity! "tasks" {:project project :rank 1})
                                   :tasks/rank 9))]
            (is (refreshed? q (raise project)))
            (is (not (refreshed? q (raise other))))))
        (testing "a list with children"
          (let [q {:tasks {:$ {:where {:and [{:status "open"} {"project.id" link}]}}
                           :notes {}}}
                held (entity! "tasks" {:project project :status "open"})]
            (is (refreshed? q (set-field (entity! "tasks" {:project project :status "done"})
                                         :tasks/status "open")))
            (is (refreshed? q (create "notes" (random-uuid) {:task held :text "a"})))
            (is (not (refreshed? q (set-field (entity! "tasks" {:project other :status "done"})
                                              :tasks/status "open"))))))
        (testing "a clause through another link"
          (let [q {:tasks {:$ {:where {:and [{"notes.text" "urgent"} {"project.id" link}]}}}}
                task (entity! "tasks" {:project project})
                note (entity! "notes" {:task task :text "later"})
                loose (entity! "notes" {:text "urgent"})]
            (is (refreshed? q (set-field note :notes/text "urgent")))
            (is (refreshed? q [[loose] [[:add-triple loose (:notes/task ids) (entity! "tasks" {:project project})]]]))
            (testing "and is for the notes of every project"
              (is (refreshed? q (set-field (entity! "notes" {:task (entity! "tasks" {:project other})
                                                             :text "later"})
                                           :notes/text "urgent"))))))))))

(deftest link-wheres-refresh-for-many-rows
  (with-tasks-session
    (fn [{:keys [ids transact! entity! create refreshed?]}]
      (let [project (entity! "projects" {})
            other (entity! "projects" {})
            q {:tasks {:$ {:where {:and [{:status "open"} {"project.id" (str project)}]}}}}
            open-all (fn [project]
                       (let [tasks (vec (repeatedly 300 random-uuid))]
                         (transact! (mapcat #(second (create "tasks" % {:project project :status "done"}))
                                            tasks))
                         [tasks (for [task tasks]
                                  [:add-triple task (:tasks/status ids) "open"])]))]
        (is (not (refreshed? q (open-all other))))
        (is (refreshed? q (open-all project)))))))

(deftest link-wheres-refresh-with-each-matcher
  (with-tasks-session
    (fn [{:keys [entity! set-field refreshed?]}]
      (let [project (entity! "projects" {})
            other (entity! "projects" {})
            q {:tasks {:$ {:where {:and [{"project.id" (str project)} {:status "open"}]}}}}
            open (fn [project]
                   (set-field (entity! "tasks" {:project project :status "done"})
                              :tasks/status "open"))]
        (doseq [[v3? v2?] [[true true] [false true] [false false]]]
          (with-redefs [flags/use-get-datalog-queries-for-topics-v3? (constantly v3?)
                        flags/use-get-datalog-queries-for-topics-v2? (constantly v2?)]
            (is (refreshed? q (open project)))
            (is (not (refreshed? q (open other))))))))))

(deftest link-wheres-refresh-through-the-invalidator
  (with-tasks-session
    (fn [{:keys [store sess ids entity! transact!]}]
      (let [{:keys [socket app-id]} sess
            project (entity! "projects" {})
            other (entity! "projects" {})
            task (entity! "tasks" {:project project :status "done"})
            other-task (entity! "tasks" {:project other :status "done"})
            q {:tasks {:$ {:where {:and [{"project.id" (str project)} {:status "open"}]}}}}
            invalidate! (var-get #'inv/invalidate!)
            handled (atom 0)
            machine-id (string/replace (str "test-" (random-uuid)) #"-" "_")
            open! (fn [task]
                    (let [before @handled]
                      (transact! [[:add-triple task (:tasks/status ids) "open"]])
                      (test-util/wait-for #(< before @handled) 5000)))
            stale? (fn []
                     (boolean (seq (rs/get-stale-instaql-queries store app-id (:id socket)))))]
        ;; The invalidator marks the session's store for the app's writes
        ;; and sends no refresh.
        (with-redefs [inv/invalidate!
                      (fn [process-id global-store wal-record]
                        (if (and (= machine-id process-id) (= app-id (:app-id wal-record)))
                          (do (invalidate! process-id store wal-record)
                              (swap! handled inc)
                              {:sockets [] :sync-subs []})
                          (invalidate! process-id global-store wal-record)))]
          (let [process (inv/start machine-id)]
            (try
              (blocking-send-msg :add-query-ok socket {:op :add-query :q q})
              (open! other-task)
              (is (not (stale?)))
              (open! task)
              (is (stale?))
              (finally
                (inv/stop process)))))))))
