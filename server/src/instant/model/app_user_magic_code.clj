(ns instant.model.app-user-magic-code
  (:require
   [instant.config :as config]
   [instant.jdbc.aurora :as aurora]
   [instant.jdbc.sql :as sql]
   [instant.model.app :as app-model]
   [instant.model.app-user :as app-user-model]
   [instant.model.instant-user :as instant-user-model]
   [instant.system-catalog-ops :refer [update-op]]
   [instant.util.crypt :as crypt-util]
   [instant.util.exception :as ex]
   [instant.util.string :refer [rand-num-str]])
  (:import
   (java.util Date UUID)))

(def etype "$magicCodes")

(defn rand-code []
  (rand-num-str 6))

(defn expired? [app-id magic-code]
  (when magic-code
    (let [created-at ^Date (:created_at magic-code)
          ttl-ms (* 1000 60 (app-model/get-magic-code-expiry-minutes {:id app-id}))]
      (< (+ (.getTime created-at) ttl-ms) (System/currentTimeMillis)))))

(defn- take-email-lock!
  "Makes the transactions that create a code for the email take turns."
  [tx-conn app-id email]
  (sql/select ::take-email-lock!
              tx-conn
              ["select pg_advisory_xact_lock(?::int4, hashtext(?))"
               (config/pg-lock-ns :magic-code)
               (str app-id ":" email)]))

(defn create!
  "Creates a code for the email and deletes the email's earlier codes."
  ([params]
   (create! (aurora/conn-pool :write) params))
  ([conn {:keys [app-id email id code]}]
   (let [id   (or id (random-uuid))
         code (or code (rand-code))]
     (update-op
      conn
      {:app-id app-id
       :etype etype}
      (fn [{:keys [tx-conn transact! get-entity get-entities-where]}]
        (take-email-lock! tx-conn app-id email)
        (transact! (conj (mapv (fn [{:keys [id]}]
                                 [:delete-entity id etype])
                               (get-entities-where {:email email}))
                         {:id       id
                          :etype    etype
                          :codeHash (-> code
                                        crypt-util/str->sha256
                                        crypt-util/bytes->hex-string)
                          :email    email}))
        (assoc (get-entity id)
               :code code))))))

(defn consume!
  ([params]
   (consume! (aurora/conn-pool :write) params))
  ([conn {:keys [email code app-id] :as params}]
   (update-op
    conn
    {:app-id app-id
     :etype etype}
    (fn [{:keys [get-entity-where delete-entity!]}]
      (let [code-hash (-> code
                          crypt-util/str->sha256
                          crypt-util/bytes->hex-string)
            {code-id :id} (get-entity-where
                           {:codeHash code-hash
                            :email    email})]
        (ex/assert-record! code-id :app-user-magic-code {:args [params]})
        (let [code (delete-entity! code-id)]
          (ex/assert-record! code :app-user-magic-code {:args [params]})
          (when (expired? app-id code)
            (ex/throw-expiration-err! :app-user-magic-code {:args [params]}))
          code))))))

(comment
  (def instant-user (instant-user-model/get-by-email
                     {:email "stopa@instantdb.com"}))
  (def app (first (app-model/get-all-for-user {:user-id (:id instant-user)})))
  (def runtime-user (app-user-model/get-by-email {:app-id (:id app)
                                                  :email "stopa@instantdb.com"}))

  (def m (create! {:id (UUID/randomUUID)
                   :code (rand-code)
                   :user-id (:id runtime-user)}))

  (consume! {:email "stopa@instantdb.com"
             :app-id (:id app)
             :code (:code m)}))
