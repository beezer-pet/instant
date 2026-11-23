(ns instant.model.instant-user-magic-code
  (:require
   [instant.config :as config]
   [instant.jdbc.aurora :as aurora]
   [instant.jdbc.sql :as sql]
   [instant.model.instant-user :as instant-user-model]
   [instant.util.string :refer [rand-num-str]]
   [instant.util.exception :as ex]
   [next.jdbc :as next-jdbc])
  (:import
   (java.time Instant)
   (java.time.temporal ChronoUnit)
   (java.util Date UUID)))

(defn rand-code []
  (rand-num-str 6))

(defn- take-user-lock!
  "Makes the transactions that create a code for the user take turns."
  [tx-conn user-id]
  (sql/select ::take-user-lock!
              tx-conn
              ["SELECT pg_advisory_xact_lock(?::int4, hashtext(?))"
               (config/pg-lock-ns :magic-code)
               (str user-id)]))

(defn create!
  "Creates a code for the user and deletes the user's earlier codes."
  ([params] (create! (aurora/conn-pool :write) params))
  ([conn {:keys [id code user-id]}]
   (next-jdbc/with-transaction [tx-conn conn]
     (take-user-lock! tx-conn user-id)
     (sql/do-execute! tx-conn
                      ["DELETE FROM instant_user_magic_codes WHERE user_id = ?::uuid"
                       user-id])
     (sql/execute-one! tx-conn
                       ["INSERT INTO instant_user_magic_codes (id, code, user_id) VALUES (?::uuid, ?, ?::uuid)"
                        id code user-id]))))

(defn expired?
  ([magic-code] (expired? (Instant/now) magic-code))
  ([now {created-at :created_at}]
   (> (.between ChronoUnit/MINUTES (Date/.toInstant created-at) now) 10)))

(defn consume!
  ([params] (consume! (aurora/conn-pool :write) params))
  ([conn {:keys [email code] :as params}]
   (let [m (sql/execute-one! conn
                             ["DELETE FROM instant_user_magic_codes
                                 USING instant_users
                               WHERE
                                 instant_user_magic_codes.user_id = instant_users.id AND
                                 instant_user_magic_codes.code = ? AND
                                 instant_users.email = ?
                               RETURNING instant_user_magic_codes.*"
                              code email])]
     (ex/assert-record! m :instant-user-magic-code {:args [params]})
     (when (expired? m)
       (ex/throw-expiration-err! :instant-user-magic-code {:args [params]}))
     m)))

(comment
  (def u (instant-user-model/get-by-email {:email "stopa@instantdb.com"}))
  (def m (create! {:id (UUID/randomUUID)
                   :code (rand-code)
                   :user-id (:id u)}))

  (expired? (.plus (Instant/now) 1 ChronoUnit/MINUTES) m)
  (expired? (.plus (Instant/now) 25 ChronoUnit/MINUTES) m)
  (consume! {:email "stopa@instantdb.com"
             :code (:code m)}))
