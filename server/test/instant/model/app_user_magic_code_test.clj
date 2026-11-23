(ns instant.model.app-user-magic-code-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [instant.fixtures :refer [with-empty-app]]
   [instant.jdbc.aurora :as aurora]
   [instant.jdbc.sql :as sql]
   [instant.model.app-user-magic-code :as magic-code]
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
        (sql/execute! (aurora/conn-pool :write)
                      ["UPDATE triples SET created_at = ? WHERE app_id = ?::uuid AND entity_id = ?::uuid"
                       (- (System/currentTimeMillis) (* 25 60 60 1000)) app-id id])
        (is (= ::ex/record-not-found (::ex/type (refusal (assoc params :code "wrong")))))
        (is (= (refusal (assoc params :code "wrong"))
               (refusal (assoc params :code code))))
        (testing "and is deleted"
          (is (empty? (sql/select (aurora/conn-pool :read)
                                  ["SELECT 1 FROM triples WHERE app_id = ?::uuid AND entity_id = ?::uuid"
                                   app-id id]))))))))
