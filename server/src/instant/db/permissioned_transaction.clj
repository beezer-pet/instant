(ns instant.db.permissioned-transaction
  (:require
   [clojure.string :as string]
   [clojure+.core :as clojure+]
   [instant.db.cel :as cel]
   [instant.db.datalog :as d]
   [instant.db.model.attr :as attr-model]
   [instant.db.model.entity :as entity-model]
   [instant.db.transaction :as tx]
   [instant.jdbc.aurora :as aurora]
   [instant.model.rule :as rule-model]
   [instant.util.exception :as ex]
   [instant.util.io :as io]
   [instant.util.json :refer [->json]]
   [instant.util.string :as string-util]
   [instant.util.tracer :as tracer]
   [instant.util.uuid :as uuid-util]
   [next.jdbc :as next-jdbc]
   [instant.util.coll :as ucoll]
   [instant.db.model.triple :as triple-model]
   [instant.system-catalog :as system-catalog]
   [instant.flags :as flags])
  (:import
   (com.zaxxer.hikari HikariDataSource)))

(defn lookup-ref? [eid]
  (sequential? eid))

(defn extract-etype [{:keys [attrs]} attr-id]
  (attr-model/fwd-etype (attr-model/seek-by-id attr-id attrs)))

(defn extract-rev-etype [{:keys [attrs]} attr-id]
  (attr-model/rev-etype (attr-model/seek-by-id attr-id attrs)))

(defn printable-check [check]
  (-> check
      (update :program :display-code)))

(defn- throw-tx-step-validation-err! [tx-step message]
  (ex/throw-validation-err!
   :tx-step
   (tx/vectorize-tx-step tx-step)
   [{:message message}]))

(defn- validate-system-delete-entity! [{:keys [admin?]} {:keys [etype] :as tx-step}]
  (when-not (or admin? (= "$files" etype))
    (throw-tx-step-validation-err! tx-step
                                   (format "%s is a system entity. You aren't allowed to delete this directly." etype))))

(defn- validate-system-triple-op! [{:keys [admin? attrs]} {:keys [aid] :as tx-step}]
  (let [attr (attr-model/seek-by-id aid attrs)
        {:keys [catalog]} attr
        [etype label :as ident-name] (attr-model/fwd-ident-name attr)

        valid-triple-op? (or
                          ;; if this isn't in a system catalog, it's allowed
                          (not= catalog :system)

                          ;; some system catalog attrs can be changed
                          (system-catalog/editable-triple-ident-name? ident-name)

                          (if (or (= etype "$files")
                                  (= etype "$streams"))
                            ;; under no circumstances do we let you change $files or $streams attrs
                            false
                            ;; for other systems namespaces, admins can change them
                            admin?))]
    (when-not valid-triple-op?
      (throw-tx-step-validation-err!
       tx-step
       (format "%s.%s is a system column. You aren't allowed to change this directly."
               etype label)))
    (when (and (= ["$files" "path"] ident-name)
               (string? (:value tx-step))
               (string/starts-with? (:value tx-step) "$stream/"))
      ;; Really, this only needs to be files linked to streams, but we
      ;; don't want to do another db lookup here to check if it's linked
      (throw-tx-step-validation-err!
       tx-step
       (format "The path for stream files can't be edited.")))))

(defn prevent-system-column-updates
  "What do we do if a user tries to run a `transact` and update a row 
   in a system table? 

   Here's what we do: 

   There are _some_ system columns that we let users edit: 
      
      1. The $files 'path' attribute can be changed. 

      2. The `id` attribute
         
         This is because the user may have their own columns defined, and 
         are running a patch. The client _always_ sends an 
         `id` triple as part of a patch.  

  For the remaining columns: 

  1. If the user is an admin, they can update system columns. 

     *Note*: we special case $files, so _even admins can't update_ those columns. 
     This is because $files includes information like `file size`, 
     which only Instant can set."
  [{:keys [attrs] :as ctx} tx-step-maps]
  (doseq [{:keys [op etype aid] :as tx-step} tx-step-maps
          :when (and etype (string/starts-with? etype "$"))
          :when (#{:add-triple :deep-merge-triple :retract-triple :delete-entity} op)]
    (if (or (= :delete-entity op)
            ;; without its id an entity drops out of its etype's listing
            (and (= :retract-triple op)
                 (= "id" (attr-model/fwd-label (attr-model/seek-by-id aid attrs)))))
      (validate-system-delete-entity! ctx  tx-step)
      (validate-system-triple-op! ctx tx-step))))

(defn coerce-value-uuids
  "Checks that all ref values are either lookup refs or UUIDs"
  [_ctx tx-steps]
  (for [{:keys [rev-etype value] :as tx-step} tx-steps]
    (if (and rev-etype (not (lookup-ref? value)))
      (if-some [value-uuid (uuid-util/coerce value)]
        (assoc tx-step :value value-uuid)
        (ex/throw-validation-err! :eid value
                                  [{:message "Expected link value to be a uuid."
                                    :hint {:tx-step (tx/vectorize-tx-step tx-step)}}]))
      tx-step)))

(defn validate-lookup-etypes
  "Checks that all lookup refs use existing attrs and match tx-step attribute etypes"
  [ctx tx-steps]
  (doseq [{:keys [eid etype] :as tx-step} tx-steps
          :when (lookup-ref? eid)
          :let [lookup-etype (extract-etype ctx (first eid))]]
    (when-not lookup-etype
      (ex/throw-validation-err! :lookup eid
                                [{:tx-step (tx/vectorize-tx-step tx-step)
                                  :message "Invalid lookup. Could not determine namespace from lookup attribute."}]))
    (when-not (= etype lookup-etype)
      (ex/throw-validation-err! :tx-step (tx/vectorize-tx-step tx-step)
                                [{:message (string-util/multiline->single-line
                                            "Invalid transaction. The namespace in the lookup attribute is
                                           different from the namespace of the attribute that is
                                           being set")}])))
  tx-steps)

(defn- id-lookup->eid
  "The entity id that a lookup on an id attribute names: the database takes
   the entity of such a lookup from its value where no triple holds it."
  [{:keys [attrs]} eid]
  (when (lookup-ref? eid)
    (let [[aid value] eid]
      (when (= "id" (attr-model/fwd-label (attr-model/seek-by-id aid attrs)))
        (uuid-util/coerce value)))))

(defn load-entities-map
  "Takes tx-steps, fetches from DB map of

     {{:eid :etype} -> <entity map>}

   A lookup on an id attribute that finds no entity is given the entity of
   that id, which can hold values of the etype's attributes with one value
   without its id."
  [{:keys [datalog-query-fn attrs] :as ctx} tx-steps]
  (let [;; An entity appears in one tx-step per attribute written, so fetch
        ;; each entity once rather than once per step.
        eids+etypes (distinct
                     (for [{:keys [eid etype value rev-etype]} tx-steps
                           :when eid
                           [eid etype] (if rev-etype
                                         [[eid etype]
                                          [value rev-etype]]
                                         [[eid etype]])]
                       {:eid eid :etype etype}))
        id-keys     (into {}
                          (for [{:keys [eid etype] :as key} eids+etypes
                                :let [id (id-lookup->eid ctx eid)]
                                :when id]
                            [key {:eid id :etype etype}]))
        fetched     (if (seq id-keys)
                      (distinct (concat eids+etypes (vals id-keys)))
                      eids+etypes)]
    (when (seq eids+etypes)
      (let [query {:children
                   {:pattern-groups
                    (for [{:keys [eid etype]} fetched]
                      {:patterns
                       [[:ea eid (attr-model/ea-ids-for-etype etype attrs)]]})}}
            ;; you might be tempted to simplify the query to [[:ea (set eids)]]
            ;; but the eid might be a lookup ref and you won't know how to get
            ;; the join rows for that lookup
            query-ctx (if (and (identical? datalog-query-fn d/query)
                               (d/scoped-permission-entity-fetch-enabled? (:app-id ctx)))
                        (assoc ctx ::d/permission-entity-fetch? true)
                        ctx)
            datalog-result (datalog-query-fn query-ctx query)
            entities (zipmap
                      fetched
                      (for [data (:data datalog-result)
                            :let [rows    (:join-rows (:result data))
                                  triples (mapcat identity rows)]]
                        (when (seq triples)
                          (-> (entity-model/triples->map ctx triples)
                              (assoc "id" (ffirst triples))))))]
        (if (seq id-keys)
          (into {}
                (for [key eids+etypes]
                  [key (or (get entities key)
                           (get entities (get id-keys key)))]))
          entities)))))

(defn resolve-lookups-tx-steps
  "Given known entities-map, resolves as much lookup-refs as possible"
  [_ctx entities-map tx-steps]
  (for [{:keys [eid etype value rev-etype] :as tx-step} tx-steps]
    (cond-> tx-step
      (lookup-ref? eid)
      (update :eid #(-> entities-map
                        (get {:eid % :etype etype})
                        (get "id")
                        (some-> uuid-util/coerce)
                        (or %)))

      (and rev-etype (lookup-ref? value))
      (update :value #(-> entities-map
                          (get {:eid % :etype rev-etype})
                          (get "id")
                          (some-> uuid-util/coerce)
                          (or %))))))

(defn resolve-lookups-entities-map
  "Updates entities-map keys that are lookup-refs"
  [_ctx entities-map]
  (into {}
        (for [[{:keys [eid etype]} entity] entities-map
              :let [id (or (some-> entity
                                   (get "id")
                                   uuid-util/coerce)
                           eid)]]
          [{:eid id :etype etype} entity])))

(defn deep-merge-and-delete
  "Applies a `merge()` patch to a record. Analogous to immutableDeepMerge in JS
   and deep_merge in Postgres"
  [orig updates]
  (if (and (map? orig) (map? updates))
    (reduce-kv
     (fn [accum key updated-value]
       (cond
         (and (map? (get accum key))
              (map? updated-value))
         (assoc accum key (deep-merge-and-delete (get accum key) updated-value))

         (nil? updated-value)
         (dissoc accum key)

         :else
         (assoc accum key updated-value)))
     orig
     updates)
    updates))

(defn update-entities-map
  "Applies tx-steps to the entities in entities-map. Given the steps in the
   order the database applies them, returns

     {:entities <the entities ~approximating the ones post tx>
      :cleared  #{[eid aid]}, the attributes with one value whose value a
                retract removes
      :remade   #{{:eid :etype}}, the entities that are deleted and
                written afterwards}

   As in the database, a retract removes a value only where it names the value
   that is held. Of an attribute with many values an entity holds the last
   value that the transaction adds and doesn't retract afterwards. An entity
   that is written after its delete holds what is written from then on."
  [{:keys [attrs]} entities-map tx-steps]
  (let [{:keys [entities many cleared remade]}
        (reduce
         (fn [acc {:keys [op eid etype aid value]}]
           (let [key   {:eid eid :etype etype}
                 ;; a write after the delete of its entity starts it anew
                 acc   (if (and (#{:add-triple :deep-merge-triple} op)
                                (contains? (:deleted acc) key))
                         (-> acc
                             (update :deleted disj key)
                             (update :remade conj key)
                             (update :entities assoc! key nil)
                             (update :many #(into {} (remove (fn [[[k]]] (= key k))) %)))
                         acc)
                 ;; apply lookup from eid as entity attr
                 acc   (if (and (lookup-ref? eid)
                                (#{:add-triple :deep-merge-triple} op))
                         (let [[lookup-aid lookup-value] eid
                               lookup-label (-> lookup-aid
                                                (attr-model/seek-by-id attrs)
                                                :forward-identity
                                                last)]
                           (update acc :entities ucoll/update! key assoc lookup-label lookup-value))
                         acc)
                 ;; apply tx-step
                 attr  (some-> aid
                               (attr-model/seek-by-id attrs))
                 label (-> attr
                           :forward-identity
                           last)
                 one?  (= :one (:cardinality attr))
                 ;; the database finds the triple to delete by the value's json
                 named? (fn [held]
                          (= (->json held) (->json value)))]
             (case op
               :add-triple
               (if one?
                 (update acc :entities ucoll/update! key assoc label value)
                 (update-in acc [:many [key label]] (fnil conj []) value))

               :deep-merge-triple
               (update acc :entities ucoll/update! key update label deep-merge-and-delete value)

               :retract-triple
               (if one?
                 (let [entity (get (:entities acc) key)]
                   (if (and (contains? entity label)
                            (named? (get entity label)))
                     (-> acc
                         (update :entities ucoll/update! key dissoc label)
                         (update :cleared conj [(str eid) (str aid)]))
                     acc))
                 (update-in acc [:many [key label]] #(into [] (remove named?) %)))

               :delete-entity
               (update acc :deleted conj key)

               acc)))
         {:entities (transient entities-map)
          :many     {}
          :cleared  #{}
          :deleted  #{}
          :remade   #{}}
         tx-steps)]
    {:entities (persistent!
                (reduce-kv
                 (fn [entities [key label] values]
                   (if (seq values)
                     (ucoll/update! entities key assoc label (peek values))
                     entities))
                 entities
                 many))
     :cleared  cleared
     :remade   remade}))

(defn get-modified-fields-for-eid
  "Returns a list of field names being modified for a given entity.
   Used for request.modifiedFields in CEL update rules."
  [target-eid tx-steps attrs]
  (->> tx-steps
       (filter (fn [{:keys [op eid rev-etype]}]
                 (and (= target-eid eid)
                      (or (#{:add-triple :deep-merge-triple} op)
                          ;; a retract of a link is an unlink, not a field
                          (and (= :retract-triple op)
                               (nil? rev-etype))))))
       (map (fn [{:keys [aid]}]
              (let [attr (attr-model/seek-by-id aid attrs)
                    [_ _ label] (:forward-identity attr)]
                label)))
       ;; (TODO) We exclude `id` because we don't consider it a modifable field
       ;; We should add a validation check though earlier in transact to prevent
       ;; the value of id being different than the eid of the entity
       (remove #(= % "id"))
       distinct
       vec))

(defn validate-retracts!
  "A retract is checked through the entity it names. One of an attribute
   that doesn't exist, or through a lookup that finds no entity, would be
   given no check, so it is refused."
  [{:keys [attrs]} tx-steps]
  (doseq [{:keys [op eid etype aid value rev-etype] :as tx-step} tx-steps
          :when (= :retract-triple op)]
    (when-not etype
      (throw-tx-step-validation-err! tx-step "Could not find the attribute for this retract"))
    (doseq [lookup [eid (when (or rev-etype
                                  (= "id" (attr-model/fwd-label (attr-model/seek-by-id aid attrs))))
                          value)]
            :when (lookup-ref? lookup)]
      (ex/throw-validation-err! :lookup lookup [{:message "Could not find the entity for this lookup"}]))))

(defn validate-retracted!
  "A retract is checked through the entity it names, as the entity was before
   the transaction, and the rules are given the entity as the retracts are
   expected to leave it. The transaction is refused where the database did
   otherwise:

   A retract deleted a triple of an entity that was not there before the
   transaction, other than a triple the transaction wrote itself, so it was
   given no check.

   The attributes with one value that lost it to a retract are not the
   `cleared` ones, for example because the entity was changed after the
   checks read it, or holds a value in another form than it is read in. A
   value that a delete of an entity took first is not expected of a retract,
   unless the transaction writes the attribute between the two."
  [{:keys [attrs] :as ctx} entities-map cleared tx-steps tx-data]
  (let [loaded   (into #{}
                       (for [[{:keys [eid etype]} entity] entities-map
                             :when entity]
                         [(str eid) etype]))
        written  (into #{}
                       (for [{:keys [op eid aid]} tx-steps
                             :when (#{:add-triple :deep-merge-triple} op)]
                         [(str eid) (str aid)]))
        results  (:results tx-data)
        removed  (into #{}
                       (for [{:keys [entity_id attr_id]} (:retract-triple results)
                             :when (= :one (:cardinality (attr-model/seek-by-id attr_id attrs)))]
                         [(str entity_id) (str attr_id)]))
        ;; tx-steps are in the order the database applies them
        between  (->> tx-steps
                      (drop-while #(not (#{:delete-entity :retract-triple} (:op %))))
                      (take-while #(not= :retract-triple (:op %))))
        deleted  (when (seq between)
                   (into #{}
                         (for [{:keys [entity_id attr_id]} (:delete-entity results)]
                           [(str entity_id) (str attr_id)])))
        rewritten (into #{}
                        (for [{:keys [op eid aid]} between
                              :when (#{:add-triple :deep-merge-triple} op)]
                          [(str eid) (str aid)]))
        expected (into #{}
                       (remove (fn [eid+aid]
                                 (and (contains? deleted eid+aid)
                                      (not (rewritten eid+aid)))))
                       cleared)]
    (doseq [{:keys [entity_id attr_id]} (:retract-triple results)
            :when (not (or (loaded [(str entity_id) (extract-etype ctx attr_id)])
                           (written [(str entity_id) (str attr_id)])))]
      (ex/throw-validation-err! :eid entity_id [{:message "Could not find the entity for this retract"}]))
    (when-some [[eid aid] (first (concat (remove removed expected)
                                         (remove expected removed)))]
      (ex/throw-validation-err! :retract
                                {:entity-id eid :attr-id aid}
                                [{:message "The retract did not do what the permission checks expected of it"}]))))

(defn validate-names!
  "The rules check an entity under the name that the steps give it, its id or
   a lookup, and a lookup that finds no entity before the transaction names a
   new one. Where two of the names that the steps write or link turn out to
   be one entity in the database, the checks would see it in parts, each with
   the steps of one name, so the transaction is refused.

   So is one that writes or links a lookup that `create-lookups-map` doesn't
   hold, as where the transaction took the value from its entity, or the
   database matched the lookup in another form than it is written in, 1.0
   for 1."
  [tx-steps create-lookups-map]
  (let [named (fn [acc name etype]
                (let [eid (if (lookup-ref? name)
                            (or (get create-lookups-map name)
                                (ex/throw-validation-err! :lookup name [{:message "Could not find the entity for this lookup"}]))
                            name)]
                  (update acc [eid etype] (fnil conj #{}) name)))
        names (reduce (fn [acc {:keys [op eid etype value rev-etype]}]
                        (if (#{:add-triple :deep-merge-triple} op)
                          (cond-> (named acc eid etype)
                            rev-etype (named value rev-etype))
                          acc))
                      {}
                      tx-steps)]
    (when-some [lookup (some (fn [[_ names]]
                               (when (next names)
                                 (some #(when (lookup-ref? %) %) names)))
                             names)]
      (ex/throw-validation-err! :lookup lookup [{:message "The transaction names the entity of this lookup in another way too"}]))))

(defn pre-checks
  "Checks that run before tx: update, delete for attrs & objects. A `remade`
   entity is deleted and written afterwards: its delete is checked on what
   it was, and its writes make a new entity, which is checked after the tx."
  [{:keys [attrs admin? rules]}
   entities-map
   updated-entities-map
   rule-params-map
   remade
   tx-steps]
  (for [tx-step tx-steps
        :let [{:keys [op eid aid etype value rev-etype]} tx-step
              key                {:eid eid :etype etype}
              entity             (when (or (= :delete-entity op)
                                           (not (remade key)))
                                   (get entities-map key))
              ref?               (some? rev-etype)
              [_ _ fwd-label]    (:forward-identity (attr-model/seek-by-id aid attrs))
              [_ _ rev-label]    (:reverse-identity (attr-model/seek-by-id aid attrs))
              rev-key            {:eid value :etype rev-etype}
              rev-entity         (when (and ref?
                                            (not (remade rev-key)))
                                   (get entities-map rev-key))
              rule-params        (get rule-params-map key)
              rev-rule-params    (get rule-params-map rev-key)
              link-program       (when ref?
                                   (rule-model/get-program! rules
                                                            {:etype etype
                                                             :action "link"
                                                             :paths [[etype "allow" "link" fwd-label]
                                                                     [etype "allow" "link" "$default"]]}))
              rev-link-program   (when ref?
                                   (rule-model/get-program! rules
                                                            {:etype etype
                                                             :action "link"
                                                             :paths [[rev-etype "allow" "link" rev-label]
                                                                     [rev-etype "allow" "link" "$default"]]}))
              unlink-program     (when ref?
                                   (rule-model/get-program! rules
                                                            {:etype etype
                                                             :action "unlink"
                                                             :paths [[etype "allow" "unlink" fwd-label]
                                                                     [etype "allow" "unlink" "$default"]]}))
              rev-unlink-program (when ref?
                                   (rule-model/get-program! rules
                                                            {:etype etype
                                                             :action "unlink"
                                                             :paths
                                                             [[rev-etype "allow" "unlink" rev-label]
                                                              [rev-etype "allow" "unlink" "$default"]]}))]
        check (clojure+/cond+
               (= :update-attr op)
               [{:scope    :attr
                 :action   :update
                 :etype    "attrs"
                 :program  {:result admin?}}]

               (= :delete-attr op)
               [{:scope    :attr
                 :action   :delete
                 :etype    "attrs"
                 :program  {:result admin?}}]

               (= :restore-attr op)
               [{:scope    :attr
                 :action   :restore
                 :etype    "attrs"
                 :program  {:result admin?}}]

                ;; if link is defined on at least one side
               (and (= :add-triple op)
                    ref?
                    (or link-program rev-link-program))
               (concat
                (when (and entity link-program)
                  [{:scope    :object
                    :action   :link
                    :etype    etype
                    :eid      eid
                    :program  link-program
                    :bindings {:data         entity
                               :new-data     (get updated-entities-map key)
                               :linked-data  rev-entity
                               :linked-etype rev-etype
                               :actions      {"data" "update"
                                              "linkedData" (if rev-entity "update" "create")}
                               :rule-params  (merge rev-rule-params rule-params)}}])
                (when (and rev-entity rev-link-program)
                  [{:scope    :object
                    :action   :link
                    :etype    rev-etype
                    :eid      value
                    :program  rev-link-program
                    :bindings {:data         (assoc rev-entity "$action" "update")
                               :new-data     (get updated-entities-map rev-key)
                               :linked-data  (get updated-entities-map key)
                               :linked-etype etype
                               :actions      {"data" "update"
                                              "linkedData" (if entity "update" "create")}
                               :rule-params  (merge rule-params rev-rule-params)}}]))

                ;; fallback when link isn´t defined on either side
               (and (= :add-triple op)
                    ref?)
               (concat
                (when entity
                  [{:scope    :object
                    :action   :update
                    :etype    etype
                    :eid      eid
                    :program  (or (rule-model/get-program! rules etype "update")
                                  {:result true})
                    :bindings {:data            entity
                               :new-data        (get updated-entities-map key)
                               :rule-params     (merge rev-rule-params rule-params)
                               :modified-fields (get-modified-fields-for-eid eid tx-steps attrs)}}])
                (when rev-entity
                  [{:scope    :object
                    :action   :view
                    :etype    rev-etype
                    :eid      value
                    :program  (or (rule-model/get-program! rules rev-etype "view")
                                  {:result true})
                    :bindings {:data        rev-entity
                               :new-data    (get updated-entities-map rev-key)
                               :rule-params (merge rule-params rev-rule-params)}}]))

                ;; if unlink is defined on at least one side
               (and (= :retract-triple op)
                    ref?
                    (or unlink-program rev-unlink-program))
               (concat
                (when (and entity unlink-program)
                  [{:scope    :object
                    :action   :unlink
                    :etype    etype
                    :eid      eid
                    :program  unlink-program
                    :bindings {:data         entity
                               :new-data     (get updated-entities-map key)
                               :linked-data  rev-entity
                               :linked-etype rev-etype
                               :rule-params  (merge rev-rule-params rule-params)}}])
                (when (and rev-entity rev-unlink-program)
                  [{:scope    :object
                    :action   :unlink
                    :etype    rev-etype
                    :eid      value
                    :program  rev-unlink-program
                    :bindings {:data         rev-entity
                               :new-data     (get updated-entities-map rev-key)
                               :linked-data  entity
                               :linked-etype etype
                               :rule-params  (merge rule-params rev-rule-params)}}]))

                ;; fallback when unlink isn´t defined on either side
               (and (= :retract-triple op)
                    ref?)
               (concat
                (when entity
                  [{:scope    :object
                    :action   :update
                    :etype    etype
                    :eid      eid
                    :program  (or (rule-model/get-program! rules etype "update")
                                  {:result true})
                    :bindings {:data            entity
                               :new-data        (get updated-entities-map key)
                               :rule-params     (merge rev-rule-params rule-params)
                               :modified-fields (get-modified-fields-for-eid eid tx-steps attrs)}}])
                (when rev-entity
                  [{:scope    :object
                    :action   :view
                    :etype    rev-etype
                    :eid      value
                    :program  (or (rule-model/get-program! rules rev-etype "view")
                                  {:result true})
                    :bindings {:data        rev-entity
                               :new-data    (get updated-entities-map rev-key)
                               :rule-params (merge rule-params rev-rule-params)}}]))

               ;; without its id an entity drops out of its etype's listing
               (and (= :retract-triple op)
                    (not ref?)
                    (= "id" fwd-label))
               [{:scope    :object
                 :action   :delete
                 :etype    etype
                 :eid      eid
                 :program  (or (rule-model/get-program! rules etype "delete")
                               {:result true})
                 :bindings {:data        entity
                            :rule-params rule-params}}]

               ;; a retract that gets here is not of a link
               (and (#{:add-triple :deep-merge-triple :retract-triple} op)
                    entity)
               [{:scope    :object
                 :action   :update
                 :etype    etype
                 :eid      eid
                 :program  (or (rule-model/get-program! rules etype "update")
                               {:result true})
                 :bindings {:data            entity
                            :new-data        (get updated-entities-map key)
                            :rule-params     rule-params
                            :modified-fields (get-modified-fields-for-eid eid tx-steps attrs)}}]

               (= :delete-entity op)
               [{:scope    :object
                 :action   :delete
                 :etype    etype
                 :eid      eid
                 :program  (or (rule-model/get-program! rules etype "delete")
                               {:result true})
                 :bindings {:data        entity
                            :rule-params rule-params}}]

               :else
               [])]
    check))

(defn post-create-checks
  "Checks for new entities created -- assumed to be run after tx so it can
   reference new data in rules"
  [{:keys [attrs rules]}
   entities-map
   updated-entities-map
   rule-params-map
   create-lookups-map
   tx-steps]
  (for [{:keys [op eid aid etype value rev-etype]} tx-steps
        :let [key                {:eid eid :etype etype}
              entity             (get entities-map key)
              updated-entity     (some-> (get updated-entities-map key)
                                         (update "id" #(or (get create-lookups-map %) (:eid key) %)))
              create?            (nil? entity)
              ref?               (some? rev-etype)
              [_ _ fwd-label]    (:forward-identity (attr-model/seek-by-id aid attrs))
              [_ _ rev-label]    (:reverse-identity (attr-model/seek-by-id aid attrs))
              rev-key            {:eid value :etype rev-etype}
              updated-rev-entity (when ref?
                                   (some-> (get updated-entities-map rev-key)
                                           (update "id" #(or (get create-lookups-map %) (:eid rev-key) %))))
              rule-params        (get rule-params-map key)
              rev-rule-params    (get rule-params-map rev-key)
              link-program       (when ref?
                                   (rule-model/get-program! rules
                                                            {:etype etype
                                                             :action "link"
                                                             :paths [[etype "allow" "link" fwd-label]
                                                                     [etype "allow" "link" "$default"]]}))
              rev-link-program   (when ref?
                                   (rule-model/get-program! rules
                                                            {:etype etype
                                                             :action "link"
                                                             :paths [[rev-etype "allow" "link" rev-label]
                                                                     [rev-etype "allow" "link" "$default"]]}))]
        check (clojure+/cond+
               (= :add-attr op)
               [{:scope    :attr
                 :action   :create
                 :etype    "attrs"
                 :program  (or (rule-model/get-program! rules "attrs" "create")
                               {:result true})
                 :bindings {:data value}}]

                ;; if link is defined on at least one side
               (and (= :add-triple op)
                    ref?
                    (or link-program rev-link-program))
               (concat
                (when (and create? link-program)
                  [{:scope    :object
                    :action   :link
                    :etype    etype
                    :eid      (get create-lookups-map eid eid)
                    :program  link-program
                    :bindings {:data         updated-entity
                               :new-data     updated-entity
                               :linked-data  updated-rev-entity
                               :linked-etype rev-etype
                               :actions      {"data" "create"
                                              "linkedData" (if (get entities-map rev-key) "update" "create")}
                               :rule-params  (merge rev-rule-params rule-params)}}])
                (when (and updated-rev-entity
                           (nil? (get entities-map rev-key))
                           rev-link-program)
                  [{:scope    :object
                    :action   :link
                    :etype    rev-etype
                    :eid      (get updated-rev-entity "id")
                    :program  rev-link-program
                    :bindings {:data         updated-rev-entity
                               :new-data     updated-rev-entity
                               :linked-data  updated-entity
                               :linked-etype etype
                               :actions      {"data" "create"
                                              "linkedData" (if create? "create" "update")}
                               :rule-params  (merge rule-params rev-rule-params)}}]))

                ;; fallback when link isn´t defined on either side
               (and (= :add-triple op)
                    ref?)
               (concat
                (when create?
                  [{:scope    :object
                    :action   :create
                    :etype    etype
                    :eid      (get create-lookups-map eid eid)
                    :program  (or (rule-model/get-program! rules etype "create")
                                  {:result true})
                    :bindings {:data            updated-entity
                               :new-data        updated-entity
                               :rule-params     (merge rev-rule-params rule-params)
                               :modified-fields (get-modified-fields-for-eid eid tx-steps attrs)}}])
                (when (and updated-rev-entity
                           (nil? (get entities-map rev-key)))
                  [{:scope    :object
                    :action   :view
                    :etype    rev-etype
                    :eid      (get updated-rev-entity "id")
                    :program  (or (rule-model/get-program! rules rev-etype "view")
                                  {:result true})
                    :bindings {:data         updated-rev-entity
                               :new-data     updated-rev-entity
                               :rule-params  (merge rule-params rev-rule-params)}}]))

               (and (#{:add-triple :deep-merge-triple} op)
                    create?)
               (do
                 (when (= "$users" etype)
                   (throw-tx-step-validation-err!
                    {:op op :eid eid :aid aid :etype etype :value value}
                    "$users is a system entity. You aren't allowed to create this directly."))
                 [{:scope    :object
                   :action   :create
                   :etype    etype
                   :eid      (get create-lookups-map eid eid)
                   :program  (or (rule-model/get-program! rules etype "create")
                                 {:result true})
                   :bindings (let [updated-entity (-> (get updated-entities-map key)
                                                      (update "id" #(get create-lookups-map % %)))]
                               {:data            updated-entity
                                :new-data        updated-entity
                                :rule-params     rule-params
                                :modified-fields (get-modified-fields-for-eid eid tx-steps attrs)})}])

               :else
               [])]
    check))

(defn run-checks!
  "Runs checks, returning results (admin-check?) or throwing"
  [ctx checks]
  (let [checks (distinct checks)
        ctx    (assoc ctx :preloaded-refs (cel/create-preloaded-refs-cache))
        _      (doseq [check checks
                       :let [{:keys [eid]} check]
                       :when (lookup-ref? eid)]
                 (ex/throw-validation-err! :lookup eid [{:message "Could not find the entity for this lookup"}]))
        results (cel/eval-programs! ctx checks)]
    (doall
     (for [{:keys [scope etype result] :as check} results]
       (do
         (when-not (:admin-check? ctx)
           (ex/assert-permitted! :perms-pass? [etype scope] result))
         (-> check
             (dissoc :result)
             (assoc
              :check-result result
              :check-pass?  (boolean result))))))))

(defn- transact-once!
  "Runs transactions alongside permission checks. The overall flow looks like this:

   1. Take a list of tx-steps (add-attr, delete-attr, add-triple, etc)
   2. Load existing triples that correspond to these steps from DB
   3. Resolve lookup-refs where possible
   4. Run update and delete checks
   5. Transact tx-steps
   6. Fetch new lookup-ref data for newly inserted data
   7. Run create checks

   We run `create` checks _after_ the transaction, so we can query off of the
   object. For example, if we created a new `post`, we may want a check that says:
   `auth.id in data.ref('creator.id')`"
  [{:keys [db app-id admin? admin-check? admin-dry-run? attrs] :as ctx} tx-step-vecs]
  (tracer/with-span! {:name "permissioned-transaction/transact!"
                      :attributes {:app-id app-id
                                   :transact-version "2"}}
    (when (flags/rate-limit-tx-based-on-conn-pool?)
      (let [^HikariDataSource pool (:conn-pool db)]
        (when (instance? HikariDataSource pool)
          (let [mx-bean (.getHikariPoolMXBean pool)
                active (.getActiveConnections mx-bean)
                total (.getTotalConnections mx-bean)
                remaining-connections (- total active)
                buffer (flags/rate-limit-tx-based-on-conn-pool-buffer)]
            (when (> buffer remaining-connections)
              (tracer/record-info! {:name "permissioned-transaction/transact-rate-limit"
                                    :attributes {:app-id app-id
                                                 :active-connections active
                                                 :total-connections total}})

              (ex/throw-rate-limited!))))))
    (next-jdbc/with-transaction [tx-conn (:conn-pool db)]
      (io/warn-io :permissioned-transact!
        (let [ops-order        (tx/tx-steps-order tx-step-vecs)
              optimistic-attrs (tx/optimistic-attrs attrs tx-step-vecs)
              tx-step-maps     (io/expect-io
                                (tx/preprocess-tx-steps tx-conn optimistic-attrs app-id tx-step-vecs))
              ;; Use the db connection we have so that we don't cause a deadlock
              ;; Also need to be able to read our own writes for the create checks
              ctx              (assoc ctx
                                      :db {:conn-pool tx-conn}
                                      :attrs optimistic-attrs)]
          (prevent-system-column-updates ctx tx-step-maps)
          (if admin?
            (let [tx-steps (tx/reorder-tx-steps ops-order tx-step-maps)]
              (io/expect-io
               (tx/transact-without-tx-conn-impl! tx-conn optimistic-attrs app-id tx-steps {})))
            (let [;; pre-processing tx
                  tx-step-maps         (->> tx-step-maps
                                            (coerce-value-uuids ctx)
                                            (validate-lookup-etypes ctx)
                                            (tx/validate-value-lookup-etypes optimistic-attrs))
                  ;; TODO somehow fetch update-delete data-ref dependencies in the same go
                  entities-map         (io/expect-io
                                        (load-entities-map ctx tx-step-maps))
                  tx-step-maps         (resolve-lookups-tx-steps ctx entities-map tx-step-maps)
                  _                    (validate-retracts! ctx tx-step-maps)
                  entities-map         (resolve-lookups-entities-map ctx entities-map)
                  ;; the rules are given the entities as the database's order
                  ;; of the steps leaves them
                  ordered-tx-step-maps (tx/reorder-tx-steps ops-order tx-step-maps)
                  {updated-entities-map :entities
                   cleared              :cleared
                   remade               :remade}
                  (update-entities-map ctx entities-map ordered-tx-step-maps)
                  rule-params-map      (persistent!
                                        (reduce
                                         (fn [acc {:keys [op eid etype value]}]
                                           (if (= :rule-params op)
                                             (ucoll/update! acc {:eid eid :etype etype} merge value)
                                             acc))
                                         (transient {})
                                         tx-step-maps))

                  ;; pre checks
                  pre-checks           (pre-checks ctx
                                                   entities-map
                                                   updated-entities-map
                                                   rule-params-map
                                                   remade
                                                   tx-step-maps)
                  pre-check-results    (io/expect-io
                                        (run-checks! ctx pre-checks))

                  ;; transact to DB
                  tx-step-maps         ordered-tx-step-maps
                  tx-data              (io/expect-io
                                        (tx/transact-without-tx-conn-impl! tx-conn (:attrs ctx) app-id tx-step-maps {}))
                  _                    (validate-retracted! ctx entities-map cleared tx-step-maps tx-data)

                  ;; post checks
                  lookups              (->> tx-step-maps
                                            (mapcat (fn [{:keys [eid value rev-etype]}]
                                                      (if rev-etype
                                                        [eid value]
                                                        [eid])))
                                            (filter lookup-ref?))
                  create-lookups-map   (io/expect-io
                                        (triple-model/fetch-lookups->eid tx-conn app-id lookups))
                  _                    (when (seq lookups)
                                         (validate-names! tx-step-maps create-lookups-map))
                  post-checks          (post-create-checks ctx
                                                           ;; a remade entity is new
                                                           (apply dissoc entities-map remade)
                                                           updated-entities-map
                                                           rule-params-map
                                                           create-lookups-map
                                                           tx-step-maps)
                  post-check-results   (io/expect-io
                                        (run-checks! ctx post-checks))

                  ;; finalizing
                  check-results        (concat pre-check-results post-check-results)
                  all-checks-ok?       (every? :check-result check-results)
                  rollback?            (and admin-check?
                                            (or admin-dry-run?
                                                (not all-checks-ok?)))]
              (when rollback?
                (io/expect-io
                 (.rollback tx-conn)))
              (assoc tx-data
                     :check-results  check-results
                     :all-checks-ok? all-checks-ok?
                     :committed?     (not rollback?)))))))))

(def max-deadlock-retries 3)

(defn- deadlock? [e]
  (= :deadlock-detected
     (some-> (ex/find-instant-exception e) ex-data ::ex/hint :condition)))

(defn transact!
  "Runs `transact-once!`, and runs it again if Postgres aborts it to break a
   deadlock. Nothing of an aborted transaction is stored."
  [ctx tx-step-vecs]
  (loop [retries 0]
    (let [res (try
                (transact-once! ctx tx-step-vecs)
                (catch Exception e
                  (if (and (< retries max-deadlock-retries)
                           (deadlock? e))
                    ::deadlock
                    (throw e))))]
      (if (= ::deadlock res)
        (recur (inc retries))
        res))))

(comment
  (do
    (def colors-app-id  #uuid "b502cabc-11ed-4534-b340-349d46548642")
    (def joe-eid #uuid "3c32701d-f4a2-40e8-b83c-077dd4cb5cec")
    (def app-attrs (attr-model/get-by-app-id colors-app-id))
    (def goal-id-attr (:id (attr-model/seek-by-fwd-ident-name ["goals" "id"] app-attrs)))
    (def goal-creator-id-attr (:id (attr-model/seek-by-fwd-ident-name ["goals" "creatorId"] app-attrs)))
    (def goal-title-attr (:id (attr-model/seek-by-fwd-ident-name ["goals" "title"] app-attrs)))
    (def goal-eid #uuid "5433bb57-a721-4130-9431-9384133b295f")
    (def tx-steps [[:add-triple goal-eid goal-id-attr goal-eid]
                   [:add-triple goal-eid goal-creator-id-attr joe-eid]
                   [:add-triple goal-eid goal-title-attr "Get a job"]]))
  (transact! {:db {:conn-pool (aurora/conn-pool :write)}
              :app-id colors-app-id
              :attrs app-attrs
              :current-user {:id joe-eid}
              :rules (rule-model/get-by-app-id {:app-id colors-app-id})
              :datalog-query-fn d/query} tx-steps)

  ;; OG transact
  (tx/transact! (aurora/conn-pool :write)
                (attr-model/get-by-app-id colors-app-id)
                colors-app-id
                tx-steps))
