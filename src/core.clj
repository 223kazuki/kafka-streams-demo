(ns kafka-streams-demo
  (:require [clojure.tools.logging :refer [info]]
            [jackdaw.streams :as j]
            [kinsky.client :as client]
            [jackdaw.serdes.edn :as jse]))

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
  (-> (j/kstream builder (topic-config "input"))
      (j/peek (fn [[k v]]
                (info (str {:key k :value v}))))
      (j/to (topic-config "output")))
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

  (info 1)
  (def app (start-app (app-config)))
  (stop-app app)

  (def p (client/producer {:bootstrap.servers "localhost:9092"}
                          (client/keyword-serializer)
                          (client/edn-serializer)))

  (def c (client/consumer {:bootstrap.servers "localhost:9092"
                           :group.id          "mygroup"}
                          (client/keyword-deserializer)
                          (client/edn-deserializer)))

  (client/send! p "input" :account-a {:action :login})

  (client/subscribe! c "input")
  (client/subscribe! c "output")
  (client/poll! c 100)

  (-main))
