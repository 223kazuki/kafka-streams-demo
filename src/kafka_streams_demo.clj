(ns kafka-streams-demo
  (:require [clojure.tools.logging :refer [info]]
            [jackdaw.client :as jc]
            [jackdaw.serdes.edn :as jse]
            [jackdaw.streams :as j]
            [jackdaw.streams.lambdas :as lambdas :refer [key-value]]))

(defn topic-config
  "Takes a topic name and returns a topic configuration map, which may
  be used to create a topic or produce/consume records."
  [topic-name]
  {:topic-name topic-name
   :partition-count 2
   :replication-factor 1
   :key-serde (jse/serde)
   :value-serde (jse/serde)})

(defn app-config
  "Returns the application config."
  []
  {"application.id" "entity-updap"
   "bootstrap.servers" "localhost:9092"
   "default.key.serde" "jackdaw.serdes.EdnSerde"
   "default.value.serde" "jackdaw.serdes.EdnSerde"
   "cache.max.bytes.buffering" "0"})

(defn build-topology
  [builder]
  (let [ktable (j/ktable builder (topic-config "entity"))]
    (-> builder
        (j/with-kv-state-store {:store-name "entity-store"})
        (j/kstream (topic-config "event"))
        (j/filter (fn [[_ {:keys [type]}]]
                    (= :update-entity type)))
        (j/left-join ktable #(assoc %1 :entity %2))
        (j/transform
         (lambdas/transformer-with-ctx
          (fn [ctx k v]
            (let [store (.getStateStore ctx "entity-store")
                  stored-entity (.get store k)]
              (key-value [k (update v :entity #(or stored-entity %))]))))
         ["entity-store"])
        (j/peek (fn [[k v]]
                  (info "Input:" {:key k :value v})))
        (j/map-values (fn [{:keys [data entity]}]
                        (Thread/sleep 3000)
                        (update entity :count (fnil + 0 0) (:inc data))))
        (j/peek (fn [[_ v]]
                  (info "Updated entity:" v)))
        (j/transform
         (lambdas/transformer-with-ctx
          (fn [ctx k v]
            (let [store (.getStateStore ctx "entity-store")]
              (.put store k v)
              (key-value [k v]))))
         ["entity-store"])
        (doto #_branch
          (j/to (topic-config "entity"))
          (-> (j/map-values (fn [v] {:type :entity-updated :data v}))
              (j/to (topic-config "event"))))))
  builder)

(defn start-app
  "Starts the Kafka Streams Application."
  [app-config]
  (let [builder (j/streams-builder)
        topology (build-topology builder)
        app (j/kafka-streams topology app-config)]
    (j/start app)
    (info "Kafka Streams application started")
    app))

(defn stop-app
  "Stops the Kafka Streams Application."
  [app]
  (j/close app)
  (info "Kafka Streams application stopped"))

(defn -main
  [& _]
  (start-app (app-config)))

(comment
  (def app (start-app (app-config)))
  (stop-app app)

  (def p (jc/producer {:bootstrap.servers "localhost:9092"}
                      {:key-serde (jse/serde)
                       :value-serde (jse/serde)}))

  @(jc/produce! p {:topic-name "event"} :id-1 {:type :update-entity
                                               :data {:inc 1}})

  (def c (jc/consumer {:bootstrap.servers "localhost:9092"
                       :group.id          "mygroup"}
                      {:key-serde (jse/serde)
                       :value-serde (jse/serde)}))

  (jc/subscribe c [{:topic-name "event"}])
  (jc/poll c 100)
  )
