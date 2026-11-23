(ns instant.util.string
  (:import
   (java.security SecureRandom)
   (java.util.zip CRC32))
  (:require
   [clojure.string :as string]))

(defn safe-name [x]
  (condp = (type x)
    clojure.lang.Symbol (name x)
    clojure.lang.Keyword (name x)
    String x
    (str x)))

(def ^:private ^SecureRandom secure-random (SecureRandom.))

(defn rand-num-str
  "Generates a random string of len digits, each drawn from a SecureRandom"
  [len]
  (apply str (repeatedly len #(.nextInt secure-random 10))))

(defn unique-int
  "Converts a string/kw name/symbol to a unique int"
  [s]
  (let [bytes (.getBytes ^String (name s) "UTF-8")
        crc (new CRC32)]
    (.update ^CRC32 crc bytes)
    (.getValue ^CRC32 crc)))

(defn safe-trim [s]
  (when (string? s)
       (string/trim s)))

(defn coerce-non-blank-str [s]
  (cond
    (number? s) (str s)
    (string? s) (when-not (string/blank? s)
                  (string/trim s))

    :else nil))

(defn multiline->single-line
  "Helper to remove extra spaces from multiline strings. Lets you spread the
   string across multiple lines to meet line-width requirements in code."
  [s]
  (string/replace s #"\s+" (fn [_] " ")))

(defn indexes-of [^String s ^String value]
  (loop [next-idx 0
         idxes []]
    (if-let [found-idx (string/index-of s value next-idx)]
      (recur (long (inc found-idx))
             (conj idxes found-idx))
      idxes)))

(defn join-in-sentence
  "Joins items in the list in a sentence
    ['a'] => 'a'
    ['a', 'b'] => 'a and b'
    ['a', 'b', 'c'] => 'a, b, and c'"
  [ls]
  (case (count ls)
    0 ""
    1 (format "%s" (first ls))
    2 (format "%s and %s"
              (first ls)
              (second ls))
    (format "%s, and %s"
            (string/join ", " (butlast ls))
            (last ls))))
