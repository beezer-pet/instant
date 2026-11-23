(ns instant.model.instant-user-magic-code-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [instant.fixtures :refer [with-user]]
   [instant.jdbc.aurora :as aurora]
   [instant.jdbc.sql :as sql]
   [instant.model.instant-user-magic-code :as magic-code]
   [instant.util.exception :as ex])
  (:import
   (clojure.lang ExceptionInfo)))

(defn- create! [{user-id :id email :email} code]
  (magic-code/create! {:id (random-uuid) :code code :user-id user-id})
  {:email email :code code})

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

(deftest a-new-code-replaces-the-users-earlier-codes
  (with-user
    (fn [user]
      (with-user
        (fn [other-user]
          (let [other   (create! other-user "111111")
                earlier (create! user "111111")
                latest  (create! user "222222")]
            (testing "the earlier code is gone"
              (is (not (consumes? earlier))))
            (testing "the latest code is left"
              (is (consumes? latest)))
            (testing "another user's code is left"
              (is (consumes? other)))))))))

(deftest codes-created-at-once-leave-one
  (with-user
    (fn [user]
      (let [codes (->> (repeatedly 8 #(future (create! user (magic-code/rand-code))))
                       doall
                       (mapv deref))]
        (is (= 1 (count (filter consumes? codes))))))))

(deftest an-expired-code-is-refused-as-a-wrong-code-is
  (with-user
    (fn [{user-id :id :as user}]
      (let [params (create! user "111111")]
        (sql/execute! (aurora/conn-pool :write)
                      ["UPDATE instant_user_magic_codes SET created_at = created_at - interval '1 hour' WHERE user_id = ?::uuid"
                       user-id])
        (is (= ::ex/record-not-found (::ex/type (refusal (assoc params :code "wrong")))))
        (is (= (refusal (assoc params :code "wrong"))
               (refusal params)))
        (testing "and is deleted"
          (is (empty? (sql/select (aurora/conn-pool :read)
                                  ["SELECT 1 FROM instant_user_magic_codes WHERE user_id = ?::uuid"
                                   user-id]))))))))
