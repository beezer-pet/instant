(ns instant.model.instant-user-magic-code-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [instant.fixtures :refer [with-user]]
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
