(ns instant.util.string-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [instant.util.string :as string-util]))

(deftest rand-num-str-test
  (let [strs (repeatedly 2000 #(string-util/rand-num-str 6))]
    (testing "every string is len digits"
      (is (every? #(re-matches #"[0-9]{6}" %) strs)))
    (testing "every digit is drawn"
      (is (= (set "0123456789") (set (apply str strs)))))))
