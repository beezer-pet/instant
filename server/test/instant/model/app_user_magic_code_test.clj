(ns instant.model.app-user-magic-code-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [instant.admin.routes :as admin-routes]
   [instant.db.permissioned-transaction :as permissioned-tx]
   [instant.fixtures :refer [with-empty-app]]
   [instant.hard-deletion-sweeper :as hard-deletion-sweeper]
   [instant.jdbc.aurora :as aurora]
   [instant.jdbc.sql :as sql]
   [instant.model.app :as app-model]
   [instant.model.app-user :as app-user-model]
   [instant.model.app-user-magic-code :as magic-code]
   [instant.system-catalog :as system-catalog]
   [instant.util.exception :as ex])
  (:import
   (clojure.lang ExceptionInfo)))

(defn- consumes? [params]
  (try
    (magic-code/consume! params)
    true
    (catch ExceptionInfo e
      (when-not (= ::ex/record-not-found (::ex/type (ex-data e)))
        (throw e))
      false)))

(defn- refusal [params]
  (try
    (magic-code/consume! params)
    nil
    (catch ExceptionInfo e
      (select-keys (ex-data e) [::ex/type ::ex/message]))))

(defn- stored-in
  "The etypes of the app that hold the email."
  [app-id email]
  (->> (sql/select (aurora/conn-pool :read)
                   ["SELECT attrs.etype
                       FROM triples
                       JOIN attrs ON attrs.id = triples.attr_id
                      WHERE triples.app_id = ?::uuid
                        AND triples.value = to_jsonb(?::text)"
                    app-id email])
       (map :etype)
       sort))

(defn- age!
  "Backdates the code's creation by that many minutes."
  [app-id code-id minutes]
  (sql/execute! (aurora/conn-pool :write)
                ["UPDATE triples SET created_at = ? WHERE app_id = ?::uuid AND entity_id = ?::uuid"
                 (- (System/currentTimeMillis) (* minutes 60 1000)) app-id code-id]))

(deftest a-new-code-replaces-the-emails-earlier-codes
  (with-empty-app
    (fn [{app-id :id}]
      (with-empty-app
        (fn [{other-app-id :id}]
          (let [create!     (fn [params]
                              (magic-code/create! params)
                              params)
                other-email (create! {:app-id app-id :email "d@e.f" :code "111111"})
                other-app   (create! {:app-id other-app-id :email "a@b.c" :code "111111"})
                earlier     (create! {:app-id app-id :email "a@b.c" :code "111111"})
                latest      (create! {:app-id app-id :email "a@b.c" :code "222222"})]
            (testing "the earlier code is gone"
              (is (not (consumes? earlier))))
            (testing "the latest code is left"
              (is (consumes? latest)))
            (testing "another email's code is left"
              (is (consumes? other-email)))
            (testing "another app's code for the email is left"
              (is (consumes? other-app)))))))))

(deftest codes-created-at-once-leave-one
  (with-empty-app
    (fn [{app-id :id}]
      (let [params {:app-id app-id :email "a@b.c"}
            codes  (->> (repeatedly 8 #(future (:code (magic-code/create! params))))
                        doall
                        (mapv deref))]
        (is (= 1 (count (filter #(consumes? (assoc params :code %)) codes))))))))

(deftest an-expired-code-is-refused-as-a-wrong-code-is
  (with-empty-app
    (fn [{app-id :id}]
      (let [params            {:app-id app-id :email "a@b.c"}
            {:keys [id code]} (magic-code/create! params)]
        (age! app-id id (* 25 60))
        (is (= ::ex/record-not-found (::ex/type (refusal (assoc params :code "wrong")))))
        (is (= (refusal (assoc params :code "wrong"))
               (refusal (assoc params :code code))))
        (testing "and is deleted"
          (is (empty? (stored-in app-id "a@b.c"))))))))

(deftest a-code-is-stored-until-it-is-used
  (with-empty-app
    (fn [{app-id :id}]
      (let [email          "a@b.c"
            {:keys [code]} (magic-code/create! {:app-id app-id :email email})]
        (is (= ["$magicCodes"] (stored-in app-id email)))
        (is (consumes? {:app-id app-id :email email :code code}))
        (is (empty? (stored-in app-id email)))))))

(deftest a-users-codes-are-deleted-with-the-user
  (with-empty-app
    (fn [{app-id :id :keys [admin-token make-ctx]}]
      (with-empty-app
        (fn [{other-app-id :id}]
          (let [email       "a@b.c"
                other-email "d@e.f"
                transact!   (fn [tx-steps]
                              (permissioned-tx/transact! (make-ctx {:admin? true}) tx-steps))
                create!     (fn []
                              (magic-code/create! {:app-id app-id :email email})
                              (app-user-model/create! {:app-id app-id :email email}))]
            (app-user-model/create! {:app-id app-id :email other-email})
            (magic-code/create! {:app-id app-id :email other-email})
            (magic-code/create! {:app-id other-app-id :email email})
            (doseq [[path delete!]
                    {"the admin api"
                     (fn [{:keys [id]}]
                       (let [deleted (-> (admin-routes/app-users-delete
                                          {:params  {:email email}
                                           :headers {"app-id"        app-id
                                                     "authorization" (str "Bearer " admin-token)}})
                                         :body
                                         :deleted)]
                         (is (= [id email] ((juxt :id :email) deleted)))))

                     "a transaction by id"
                     (fn [{:keys [id]}]
                       (transact! [[:delete-entity id "$users"]]))

                     "a transaction without an etype"
                     (fn [{:keys [id]}]
                       (transact! [[:delete-entity id]]))

                     "a transaction by lookup"
                     (fn [_]
                       (transact! [[:delete-entity
                                    [(system-catalog/get-attr-id "$users" "email") email]
                                    "$users"]]))

                     "a cascade"
                     (fn [{:keys [id]}]
                       (let [primary (app-user-model/create! {:app-id app-id})]
                         (app-user-model/link-guest {:app-id          app-id
                                                     :primary-user-id (:id primary)
                                                     :guest-user-id   id})
                         (transact! [[:delete-entity (:id primary) "$users"]])))}]
              (testing path
                (let [user (create!)]
                  (is (= ["$magicCodes" "$users"] (stored-in app-id email)))
                  (delete! user)
                  (is (empty? (stored-in app-id email))))))
            (testing "a delete that is refused leaves the code"
              (let [{:keys [id]} (create!)]
                (is (thrown? ExceptionInfo
                             (permissioned-tx/transact! (make-ctx) [[:delete-entity id "$users"]])))
                (is (= ["$magicCodes" "$users"] (stored-in app-id email)))))
            (testing "another user's code is left"
              (is (= ["$magicCodes" "$users"] (stored-in app-id other-email))))
            (testing "another app's code for the email is left"
              (is (= ["$magicCodes"] (stored-in other-app-id email))))))))))

(deftest expired-codes-are-swept
  (with-empty-app
    (fn [{app-id :id}]
      (with-empty-app
        (fn [{short-app-id :id}]
          (with-empty-app
            (fn [{read-only-app-id :id}]
              (app-model/update-magic-code-expiration! {:id                        short-app-id
                                                        :magic-code-expiry-minutes 10})
              (doseq [[app-id email minutes] [[app-id "old@b.c" (* 25 60)]
                                              [app-id "new@b.c" (* 23 60)]
                                              [short-app-id "old@b.c" 11]
                                              [short-app-id "new@b.c" 8]
                                              [read-only-app-id "old@b.c" (* 25 60)]]
                      :let [{:keys [id]} (magic-code/create! {:app-id app-id :email email})]]
                (age! app-id id minutes))
              (app-model/set-status! {:app-id read-only-app-id :status :read-only})
              (hard-deletion-sweeper/handle-sweep nil)
              (is (empty? (stored-in app-id "old@b.c")))
              (is (= ["$magicCodes"] (stored-in app-id "new@b.c")))
              (is (empty? (stored-in short-app-id "old@b.c")))
              (is (= ["$magicCodes"] (stored-in short-app-id "new@b.c")))
              (is (empty? (stored-in read-only-app-id "old@b.c"))))))))))
