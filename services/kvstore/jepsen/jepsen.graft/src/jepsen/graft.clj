(ns jepsen.graft
  "Jepsen tests for Graft kvstore - a Raft-based key-value store."
  (:require [jepsen.cli :as cli]
            [jepsen.generator :as gen]
            [jepsen.checker :as checker]
            [jepsen.checker.timeline :as timeline]
            [jepsen.nemesis :as nemesis]
            [jepsen.os :as os]
            [jepsen.os.debian :as debian]
            [jepsen.tests :as tests]
            [knossos.model :as model]
            [jepsen.graft.db :as db]
            [jepsen.graft.client :as client]))

;; ============================================================================
;; Generators
;; ============================================================================

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 10)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 10) (rand-int 10)]})

;; ============================================================================
;; Test
;; ============================================================================

(defn graft-test
  "Constructs a Jepsen test map for Graft kvstore (remote/Docker mode)."
  [opts]
  (merge tests/noop-test
         opts
         {:name       "graft-kvstore"
          :os         debian/os
          :db         (db/remote-db)
          :client     (client/client)
          :nemesis    (nemesis/partition-random-halves)
          :checker    (checker/compose
                       {:perf     (checker/perf)
                        :timeline (timeline/html)
                        :linear   (checker/linearizable
                                   {:model     (model/cas-register)
                                    :algorithm :wgl})})
          :generator  (->> (gen/mix [r w cas])
                           (gen/stagger 1/50)
                           (gen/nemesis
                            (cycle [(gen/sleep 5)
                                    {:type :info, :f :start}
                                    (gen/sleep 5)
                                    {:type :info, :f :stop}]))
                           (gen/time-limit (:time-limit opts 30)))}))

(defn local-test
  "Constructs a Jepsen test map for local testing (no Docker/SSH).
   Runs kvstore processes directly on the local machine."
  [opts]
  (merge tests/noop-test
         opts
         {:name       "graft-kvstore-local"
          :local?     true
          :nodes      ["n1" "n2" "n3"]
          :ssh        {:dummy? true}
          :os         os/noop
          :db         (db/local-db)
          :client     (client/client)
          :nemesis    nemesis/noop
          :checker    (checker/compose
                       {:timeline (timeline/html)
                        :linear   (checker/linearizable
                                   {:model     (model/cas-register)
                                    :algorithm :wgl})})
          :generator  (->> (gen/mix [r w cas])
                           (gen/stagger 1/50)
                           (gen/clients)
                           (gen/time-limit (:time-limit opts 30)))}))

(def cli-opts
  "Additional CLI options for Graft tests."
  [[nil "--local" "Run in local mode (no Docker/SSH)"
    :default false]])

(defn -main
  "CLI entry point."
  [& args]
  (cli/run! (merge (cli/single-test-cmd
                    {:test-fn (fn [opts]
                                (if (:local opts)
                                  (local-test opts)
                                  (graft-test opts)))
                     :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
