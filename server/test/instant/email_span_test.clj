(ns instant.email-span-test
  (:require
   [clj-http.client :as clj-http]
   [clojure.string :as string]
   [clojure.test :refer [deftest is]]
   [instant.config :as config]
   [instant.postmark :as postmark]
   [instant.sendgrid :as sendgrid]
   [instant.util.tracer :as tracer]))

(defn- span-attributes
  "Runs f and returns the attributes of the span named span-name, as a string."
  [span-name f]
  (let [spans (atom [])
        new-span! tracer/new-span!]
    (with-redefs [tracer/new-span! (fn [opts]
                                     (swap! spans conj opts)
                                     (new-span! opts))]
      (f))
    (->> @spans
         (filter #(= span-name (:name %)))
         (map :attributes)
         pr-str)))

(deftest span-of-a-sent-email-holds-neither-subject-nor-body
  (with-redefs [clj-http/post (constantly {:status 200})
                config/postmark-send-enabled? (constantly true)
                config/postmark-token (constantly "token")
                config/sendgrid-send-enabled? (constantly true)
                config/sendgrid-token (constantly "token")]
    (let [email {:subject "424242 is your verification code"
                 :html "<p>Your code is 424242</p>"
                 :text "Your code is 424242"}
          postmark-span (span-attributes
                         "postmark/send"
                         #(postmark/send! (assoc email
                                                 :from "from@example.com"
                                                 :to "to@example.com")))
          sendgrid-span (span-attributes
                         "sendgrid/send"
                         #(sendgrid/send! (assoc email
                                                 :from {:email "from@example.com"}
                                                 :to [{:email "to@example.com"}])))]
      (doseq [span [postmark-span sendgrid-span]]
        (is (string/includes? span "to@example.com"))
        (is (not (string/includes? span "424242")))))))
