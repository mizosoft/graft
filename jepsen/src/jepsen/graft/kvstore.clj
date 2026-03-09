(ns jepsen.graft.kvstore
  "Jepsen tests for Graft kvstore - a linearizable key-value store."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.client :as client]
            [jepsen.checker :as checker]
            [jepsen.checker.timeline :as timeline]
            [jepsen.generator :as gen]
            [jepsen.nemesis :as nemesis]
            [jepsen.os :as os]
            [jepsen.tests :as tests]
            [knossos.model :as model]
            [jepsen.graft.db :as db]))

;; ============================================================================
;; Client
;; ============================================================================

(defn parse-get-output
  "Parse output from 'get' command. Returns [exists? value]."
  [out]
  (let [trimmed (str/trim out)]
    (if (= trimmed "(nil)")
      [false nil]
      [true trimmed])))

(defn parse-cas-output
  "Parse output from 'cas' command. Returns true if successful."
  [out]
  (= (str/trim out) "OK"))

(defrecord Client [service-config servers client-id]
  client/Client
  (open! [this test node]
    (let [local? (get test :local? true)
          nodes (:nodes test)]
      (assoc this
             :service-config db/kvstore-config
             :servers (db/servers-string nodes local?)
             :client-id (str "jepsen-" (name node) "-" (rand-int 100000)))))

  (setup! [this test])

  (invoke! [this test op]
    (try
      (case (:f op)
        :read
        (let [{:keys [exit out err]} (db/run-cli service-config servers client-id "get" "register")]
          (if (zero? exit)
            (let [[exists? value] (parse-get-output out)]
              (assoc op
                     :type :ok
                     :value (when exists? (Long/parseLong value))))
            (assoc op :type :info :error (keyword (str/trim err)))))

        :write
        (let [{:keys [exit out err]} (db/run-cli service-config servers client-id
                                                  "put" "register" (str (:value op)))]
          (if (zero? exit)
            (assoc op :type :ok)
            (assoc op :type :info :error (keyword (str/trim err)))))

        :cas
        (let [[expected new] (:value op)
              {:keys [exit out err]} (db/run-cli service-config servers client-id
                                                  "cas" "register"
                                                  (str expected) (str new))]
          (if (zero? exit)
            (if (parse-cas-output out)
              (assoc op :type :ok)
              (assoc op :type :fail))
            (assoc op :type :info :error (keyword (str/trim err))))))

      (catch Exception e
        (assoc op :type :info :error [:exception (str e)]))))

  (teardown! [this test])

  (close! [this test]))

(defn client []
  (map->Client {}))

;; ============================================================================
;; Generators
;; ============================================================================

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 10)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 10) (rand-int 10)]})

;; ============================================================================
;; Test
;; ============================================================================

(defn test
  "Constructs a Jepsen test for kvstore linearizability."
  [opts]
  (merge tests/noop-test
         opts
         {:name       "graft-kvstore"
          :local?     true
          :nodes      ["n1" "n2" "n3"]
          :ssh        {:dummy? true}
          :os         os/noop
          :db         (db/local-db db/kvstore-config)
          :client     (client)
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
