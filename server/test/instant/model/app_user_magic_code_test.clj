(ns instant.model.app-user-magic-code-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [instant.fixtures :refer [with-empty-app]]
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
