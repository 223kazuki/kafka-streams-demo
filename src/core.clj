(ns kafka-streams-demo
  (:require [clojure.tools.logging :refer [info]]
            [jackdaw.client :as jc]
            [jackdaw.serdes.edn :as jse]
            [jackdaw.streams :as j]))

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
  {"application.id" "pipe"
   "bootstrap.servers" "localhost:9092"
   "default.key.serde" "jackdaw.serdes.EdnSerde"
   "default.value.serde" "jackdaw.serdes.EdnSerde"
   "cache.max.bytes.buffering" "0"})

(defn build-topology
  [builder]
  (let [ktable (j/ktable builder (topic-config "input-ktable"))]
    (-> (j/kstream builder (topic-config "input"))
        (j/left-join ktable #(assoc %1 :joined %2))
        (j/peek (fn [[k v]]
                  (info (str {:key k :value v}))))
        (doto #_a
          (j/to (topic-config "output"))
          (j/to (topic-config "input-ktable")))))
  builder)

(defn start-app
  "Starts the stream processing application."
  [app-config]
  (let [builder (j/streams-builder)
        topology (build-topology builder)
        app (j/kafka-streams topology app-config)]
    (j/start app)
    (info "pipe is up")
    app))

(defn stop-app
  "Stops the stream processing application."
  [app]
  (j/close app)
  (info "pipe is down"))

(defn -main
  [& _]
  (start-app (app-config)))

(comment

  (def app (start-app (app-config)))
  (stop-app app)

  (def p (jc/producer {:bootstrap.servers "localhost:9092"}
                      {:key-serde (jse/serde)
                       :value-serde (jse/serde)}))

  @(jc/produce! p {:topic-name "input"} :a {:b :c})

  (def c (jc/consumer {:bootstrap.servers "localhost:9092"
                       :group.id          "mygroup"}
                      {:key-serde (jse/serde)
                       :value-serde (jse/serde)}))

  (jc/subscribe c [{:topic-name "input"}])

  (jc/poll c 100)

  )
