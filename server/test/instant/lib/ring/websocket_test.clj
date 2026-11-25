(ns instant.lib.ring.websocket-test
  (:require [clojure.string :as string]
            [clojure.test :refer [deftest is testing]]
            [instant.lib.ring.undertow :as undertow]
            [instant.lib.ring.websocket :as ws]
            [instant.util.json :refer [<-json]])
  (:import (instant.lib.ring.undertow Server)
           (io.undertow Undertow$ListenerInfo)
           (java.net InetSocketAddress)
           (java.util Random)
           (java.util.concurrent LinkedBlockingQueue TimeUnit)
           (okhttp3 OkHttpClient Request$Builder WebSocket WebSocketListener)
           (org.apache.commons.codec.binary Hex)))

(deftest inactive?-needs-an-unanswered-ping
  (let [idle-timeout-ms 15000]
    (testing "no ping has been sent yet"
      (is (not (ws/inactive? 100000 0 0 idle-timeout-ms))))
    (testing "the ping was answered, even if the next check runs late"
      (is (not (ws/inactive? 40000 10100 10000 idle-timeout-ms))))
    (testing "any message after the ping counts as an answer"
      (is (not (ws/inactive? 30000 12000 10000 idle-timeout-ms))))
    (testing "an unanswered ping inside the timeout"
      (is (not (ws/inactive? 25000 5000 10000 idle-timeout-ms))))
    (testing "an unanswered ping past the timeout"
      (is (ws/inactive? 25001 5000 10000 idle-timeout-ms)))))

(deftest compressed-messages-over-1-mb
  (let [server (undertow/run-undertow
                (fn [_req]
                  {:undertow/websocket
                   {:on-message (fn [{:keys [channel data]}]
                                  ;; Runs on the io thread that sends the message
                                  (future (ws/send-json! nil data channel)))}})
                {:host "127.0.0.1" :port 0})
        ^Undertow$ListenerInfo info (first (Server/.getListenerInfo server))
        ^InetSocketAddress addr (.getAddress info)
        received (LinkedBlockingQueue.)
        client (OkHttpClient.)
        ^WebSocket socket (.newWebSocket
                           client
                           (-> (Request$Builder.)
                               (.url (str "ws://127.0.0.1:" (.getPort addr)))
                               (.build))
                           (proxy [WebSocketListener] []
                             (onMessage [_socket message]
                               (.put received message))))
        random-bytes (byte-array 2000000)
        _ (.nextBytes (Random.) random-bytes)]
    (try
      (doseq [[label ^String message] {"over 1 MB when compressed" (Hex/encodeHexString random-bytes)
                                       "over 1 MB when decompressed" (string/join (repeat 4000000 \a))}]
        (testing label
          (.send socket message)
          (let [echoed? (= message (<-json (.poll received 10 TimeUnit/SECONDS)))]
            (is echoed?))))
      (finally
        (.cancel socket)
        (.shutdown (.executorService (.dispatcher client)))
        (Server/.stop server)))))
