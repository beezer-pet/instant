(ns instant.storage.coordinator-test
  (:require [clojure.test :as test :refer [deftest is testing]]
            [instant.storage.coordinator :as coordinator]
            [instant.fixtures :refer [with-empty-app]]
            [instant.flags :as flags]
            [instant.model.app-user :as app-user-model]
            [instant.model.rule :as rule-model]
            [instant.util.exception :as ex]
            [instant.util.test :as test-util :refer [perm-err? validation-err? instant-ex-data make-attrs insert-entities suid]])
  (:import
   (java.io ByteArrayInputStream)))

(deftest storage-permission-can-traverse-refs
  (with-empty-app
    (fn [{app-id :id}]
      (let [rules {"$files" {"allow" {"view" "'authorized' in auth.ref('$user.authorization.role')"
                                      "update" "'authorized' in auth.ref('$user.authorization.role')"
                                      "create" "'authorized' in auth.ref('$user.authorization.role')"
                                      "delete" "'authorized' in auth.ref('$user.authorization.role')"}}}
            path "/test/file.txt"
            attr->id (make-attrs app-id
                                 [[:authorizations/role]
                                  [[:authorizations/$user :$users/authorization]]])
            pass-user (app-user-model/create!
                       {:app-id app-id
                        :id (suid "1111")
                        :email "pass@example.com"})
            fail-user (app-user-model/create!
                       {:app-id app-id
                        :id (suid "2222")
                        :email "fail@example.com"})]

        (insert-entities app-id attr->id
                         [{:db/id (suid "aaaa")
                           :authorizations/role "authorized"
                           :authorizations/$user (:id pass-user)}
                          {:db/id (suid "bbbb")
                           :authorizations/role "pending"
                           :authorizations/$user (:id fail-user)}])

        (rule-model/put! {:app-id app-id :code rules})

        (let [actions ["view" "create" "update" "delete"]]
          (testing "auth.ref should pass for authorized user"
            (doseq [action actions]
              (is (coordinator/assert-storage-permission!
                   action
                   {:app-id app-id
                    :path path
                    :current-user pass-user}))))

          (testing "auth.ref should fail for unauthorized user"
            (doseq [action actions]
              (is (perm-err?
                   (coordinator/assert-storage-permission!
                    action
                    {:app-id app-id
                     :path path
                     :current-user fail-user})))))

          (testing "no permissions set should deny by default"
            (rule-model/put! {:app-id app-id :code {}})
            (doseq [action actions]
              (is (perm-err?
                   (coordinator/assert-storage-permission!
                    action
                    {:app-id app-id
                     :path path
                     :current-user pass-user}))))))))))

(deftest storage-permissions-can-mix-data-and-auth
  (with-empty-app
    (fn [{app-id :id}]
      (let [rules {"$files" {"allow" {"create" "data.path.startsWith('profiles/' + auth.ref('$user.profile.handle')[0] + '/')"}}}
            handle "moop"
            attr->id (make-attrs app-id
                                 [[:profiles/handle]
                                  [[:profiles/$user :$users/profile]]])
            user (app-user-model/create!
                  {:app-id app-id
                   :id (suid "1")
                   :email "moop@instantdb.com"})]

        (insert-entities app-id attr->id
                         [{:db/id (suid "a")
                           :profiles/handle handle
                           :profiles/$user (:id user)}])

        (rule-model/put! {:app-id app-id :code rules})

        (testing "Valid path should pass"
          (is (coordinator/assert-storage-permission!
               "create"
               {:app-id app-id
                :path (str "profiles/" handle "/some-file.txt")
                :current-user user})))

        (testing "Invalid path should fail"
          (is (perm-err?
               (coordinator/assert-storage-permission!
                "create"
                {:app-id app-id
                 :path (str "profiles/" "random" "/some-file.txt")
                 :current-user user}))))))))

(deftest create-rule-sees-the-uploads-size-and-content-type
  (with-empty-app
    (fn [{app-id :id}]
      (rule-model/put!
       {:app-id app-id
        :code {"$files" {"allow" {"create" "data.size <= 5 && data['content-type'] in ['image/png', 'application/pdf']"}}}})
      (let [uploads (atom [])
            upload! (fn [ctx]
                      (coordinator/upload-file!
                       (merge {:app-id app-id
                               :path "photo.png"
                               :content-type "image/png"
                               :content-length 5}
                              ctx)
                       (ByteArrayInputStream. (byte-array 5))))
            refused? (fn [ex-data]
                       (contains? #{::ex/permission-denied
                                    ::ex/permission-evaluation-failed}
                                  (::ex/type ex-data)))]
        (test-util/with-s3-mock {:upload (fn [_bucket ctx _file]
                                           (swap! uploads conj (:content-length ctx)))
                                 :get-object-metadata (fn [_bucket _key]
                                                        {:size 5 :content-type "image/png"})}
          (testing "an upload within the rule's limits is stored, with the length that the rule saw"
            (is (uuid? (:id (upload! {}))))
            (is (= [5] @uploads)))

          (testing "a larger upload is refused before its body is read"
            (is (refused? (instant-ex-data (upload! {:content-length 6}))))
            (is (= [5] @uploads)))

          (testing "another content type is refused"
            (is (refused? (instant-ex-data (upload! {:content-type "text/html"}))))
            (is (= [5] @uploads)))

          (testing "an upload that declares no length has no size for the rule"
            (is (refused? (instant-ex-data (upload! {:content-length nil}))))
            (is (refused? (instant-ex-data (upload! {:content-length -1}))))
            (is (= [5] @uploads)))

          (testing "an upload url has no size for the rule"
            (is (refused?
                 (instant-ex-data
                   (coordinator/create-upload-url! {:app-id app-id
                                                    :path "photo.png"}))))))))))

(deftest storage-max-upload-bytes-caps-every-upload
  (with-empty-app
    (fn [{app-id :id}]
      (let [uploads (atom 0)
            upload! (fn [content-length & {:as ctx}]
                      (coordinator/upload-file!
                       (merge {:app-id app-id
                               :path "photo.png"
                               :content-length content-length
                               :skip-perms-check? true}
                              ctx)
                       (ByteArrayInputStream. (byte-array 5))))]
        (test-util/with-s3-mock {:upload (fn [_bucket _ctx _file]
                                           (swap! uploads inc))
                                 :get-object-metadata (fn [_bucket _key]
                                                        {:size 5})}
          (testing "without the flag an upload of any length is stored"
            (binding [flags/*flag-overrides* {:storage-max-upload-bytes nil}]
              (is (uuid? (:id (upload! 6))))
              (is (uuid? (:id (upload! -1)))))
            (is (= 2 @uploads)))

          (binding [flags/*flag-overrides* {:storage-max-upload-bytes 5}]
            (testing "an upload of the largest size is stored"
              (is (uuid? (:id (upload! 5))))
              (is (= 3 @uploads)))

            (testing "a larger upload is refused before its body is read"
              (is (validation-err? (upload! 6)))
              (is (= 3 @uploads)))

            (testing "an upload that declares no length is refused"
              (is (validation-err? (upload! -1)))
              (is (validation-err? (upload! nil)))
              (is (= 3 @uploads)))

            (testing "a file that the server stores for a stream has no limit"
              (is (uuid? (:id (upload! 6 :skip-size-check? true))))
              (is (= 4 @uploads)))))))))

(comment
  (test/run-tests *ns*))
