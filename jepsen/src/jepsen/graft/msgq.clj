(ns jepsen.graft.msgq
  "Jepsen tests for Graft msgq - a distributed message queue."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.client :as client]
            [jepsen.checker :as checker]
            [jepsen.checker.timeline :as timeline]
            [jepsen.generator :as gen]
            [jepsen.nemesis :as nemesis]
            [jepsen.os :as os]
            [jepsen.tests :as tests]
            [jepsen.graft.db :as db]))

;; ============================================================================
;; Client
;; ============================================================================

(def topic "jepsen-queue")

(defn parse-enqueue-output
  "Parse output from 'enqueue' command. Returns message ID if successful."
  [out]
  (let [trimmed (str/trim out)]
    (when (str/starts-with? trimmed "OK")
      ;; Extract id from "OK (id: xxx)"
      (when-let [match (re-find #"id: ([^\)]+)" trimmed)]
        (second match)))))

(defn parse-dequeue-output
  "Parse output from 'dequeue' command. Returns [found? data] or nil on error."
  [out]
  (let [trimmed (str/trim out)]
    (cond
      (= trimmed "(empty)") [false nil]
      (str/starts-with? trimmed "id:")
      (when-let [match (re-find #"data: (.+)" trimmed)]
        [true (second match)])
      :else nil)))

(defrecord Client [service-config servers client-id]
  client/Client
  (open! [this test node]
    (let [local? (get test :local? true)
          nodes (:nodes test)]
      (assoc this
             :service-config db/msgq-config
             :servers (db/servers-string nodes local?)
             :client-id (str "jepsen-" (name node) "-" (rand-int 100000)))))

  (setup! [this test])

  (invoke! [this test op]
    (try
      (case (:f op)
        :enqueue
        (let [{:keys [exit out err]} (db/run-cli service-config servers client-id
                                                  "enqueue" topic (str (:value op)))]
          (if (and (zero? exit) (parse-enqueue-output out))
            (assoc op :type :ok)
            (assoc op :type :info :error (keyword (str/trim (or err out))))))

        :dequeue
        (let [{:keys [exit out err]} (db/run-cli service-config servers client-id
                                                  "dequeue" topic)]
          (if (zero? exit)
            (if-let [[found? data] (parse-dequeue-output out)]
              (if found?
                (assoc op :type :ok :value (Long/parseLong data))
                (assoc op :type :fail :error :empty))
              (assoc op :type :info :error :parse-error))
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

(defn enqueue [_ _] {:type :invoke, :f :enqueue, :value (rand-int 1000)})
(defn dequeue [_ _] {:type :invoke, :f :dequeue, :value nil})

;; ============================================================================
;; Test
;; ============================================================================

(defn test
  "Constructs a Jepsen test for msgq total ordering."
  [opts]
  (merge tests/noop-test
         opts
         {:name       "graft-msgq"
          :local?     true
          :nodes      ["n1" "n2" "n3"]
          :ssh        {:dummy? true}
          :os         os/noop
          :db         (db/local-db db/msgq-config)
          :client     (client)
          :nemesis    nemesis/noop
          :checker    (checker/compose
                       {:timeline (timeline/html)
                        :queue    (checker/total-queue)})
          :generator  (->> (gen/mix [enqueue dequeue])
                           (gen/stagger 1/50)
                           (gen/clients)
                           (gen/time-limit (:time-limit opts 30)))}))
