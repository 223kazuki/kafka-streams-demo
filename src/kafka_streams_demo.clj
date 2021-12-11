(ns kafka-streams-demo
  (:require [clojure.tools.logging :as log]
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
  (let [event-topic (topic-config "event")
        entity-topic (topic-config "entity")
        store-name "entity-store"]
    (-> builder
        ;; Add state store to the application
        (j/with-kv-state-store {:store-name store-name})

        ;; Start streams
        (j/kstream event-topic)

        ;; Filter by message type
        (j/filter (fn [[_ {:keys [type]}]]
                    (= :update-entity type)))

        ;; Left join entity from ktable then state store
        ;; Event -> Event + Entity
        (j/left-join (j/ktable builder entity-topic) #(assoc %1 :entity %2))
        (j/transform (lambdas/transformer-with-ctx
                      (fn [ctx k v]
                        (let [store (.getStateStore ctx store-name)
                              stored-entity (.get store k)]
                          (key-value [k (update v :entity #(or stored-entity %))]))))
                     [store-name])

        ;; Process input message and existing entity to updated entity
        ;; Event + Entity -> Entity'
        (j/peek (fn [[k v]]
                  (log/info "Input:" {:key k :value v})))
        (j/map-values (fn [{:keys [data entity]}]
                        ;; Simulate long processing time that can cause race condition
                        (Thread/sleep 3000)
                        (update entity :count (fnil + 0 0) (:inc data))))
        (j/peek (fn [[_ v]]
                  (log/info "Updated entity:" v)))

        ;; Put updated entity to state store
        (j/transform  (lambdas/transformer-with-ctx
                       (fn [ctx k v]
                         (let [store (.getStateStore ctx store-name)]
                           (.put store k v)
                           (key-value [k v]))))
                      [store-name])
        (doto #_branch
          ;; Publish updated entity to Entity topic
          (j/to entity-topic)
          ;; Publish output event to Event topic
          (-> (j/map-values (fn [v] {:type :entity-updated :data v}))
              (j/to event-topic)))))
  builder)

(defn start-app
  "Starts the Kafka Streams Application."
  [app-config]
  (let [builder (j/streams-builder)
        topology (build-topology builder)
        app (j/kafka-streams topology app-config)]
    (j/start app)
    (log/info "Kafka Streams application started")
    app))

(defn stop-app
  "Stops the Kafka Streams Application."
  [app]
  (j/close app)
  (log/info "Kafka Streams application stopped"))

(defn -main
  [& _]
  (start-app (app-config)))

(comment
  ;; Start Kafka Streams Appliaction
  (def app (start-app (app-config)))
  (stop-app app)

  ;; Publish input message
  (def p (jc/producer {:bootstrap.servers "localhost:9092"}
                      {:key-serde (jse/serde)
                       :value-serde (jse/serde)}))

  @(jc/produce! p {:topic-name "event"} :id-1 {:type :update-entity
                                               :data {:inc 1}})

  ;; Subscribe output message
  (def c (jc/consumer {:bootstrap.servers "localhost:9092"
                       :group.id          "mygroup"}
                      {:key-serde (jse/serde)
                       :value-serde (jse/serde)}))

  (jc/subscribe c [{:topic-name "event"}])
  (jc/poll c 100))
