(ns jepsen.graft.dlock
  "Jepsen tests for Graft dlock - a distributed lock service."
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
            [jepsen.graft.db :as db])
  (:import [knossos.model Model]))

;; ============================================================================
;; Lock Model for Linearizability
;; ============================================================================

;; A lock can be in one of these states:
;; - :free - no one holds it
;; - {:holder token} - someone holds it with the given token

(defrecord Lock [state]
  Model
  (step [this op]
    (case (:f op)
      :acquire
      (if (= state :free)
        ;; Lock acquired - value is the token
        (Lock. {:holder (:value op)})
        ;; Lock already held - operation fails
        (if (= (:type op) :fail)
          this
          (model/inconsistent "Lock acquired but was already held")))

      :release
      (if (= state :free)
        ;; Can't release a free lock
        (if (= (:type op) :fail)
          this
          (model/inconsistent "Released a free lock"))
        ;; Check token matches
        (if (= (:holder state) (:value op))
          (Lock. :free)
          (if (= (:type op) :fail)
            this
            (model/inconsistent "Released with wrong token")))))))

(defn lock
  "A lock model - starts in free state."
  []
  (Lock. :free))

;; ============================================================================
;; Client
;; ============================================================================

(def resource "jepsen-lock")
(def ttl-ms "30000") ; 30 second TTL

(defn parse-lock-output
  "Parse output from 'lock' command. Returns [success? token]."
  [out]
  (let [trimmed (str/trim out)]
    (cond
      (str/starts-with? trimmed "OK")
      (when-let [match (re-find #"token: (\d+)" trimmed)]
        [true (Long/parseLong (second match))])

      (str/starts-with? trimmed "FAILED")
      [false nil]

      :else nil)))

(defn parse-unlock-output
  "Parse output from 'unlock' command. Returns success?."
  [out]
  (let [trimmed (str/trim out)]
    (cond
      (= trimmed "OK") true
      (str/starts-with? trimmed "FAILED") false
      :else nil)))

(defrecord Client [service-config servers client-id token]
  client/Client
  (open! [this test node]
    (let [local? (get test :local? true)
          nodes (:nodes test)]
      (assoc this
             :service-config db/dlock-config
             :servers (db/servers-string nodes local?)
             :client-id (str "jepsen-" (name node) "-" (rand-int 100000))
             :token (atom nil))))

  (setup! [this test])

  (invoke! [this test op]
    (try
      (case (:f op)
        :acquire
        (let [{:keys [exit out err]} (db/run-cli service-config servers client-id
                                                  "lock" resource ttl-ms)]
          (if (zero? exit)
            (if-let [[success? new-token] (parse-lock-output out)]
              (if success?
                (do
                  (reset! token new-token)
                  (assoc op :type :ok :value new-token))
                (assoc op :type :fail :error :already-locked))
              (assoc op :type :info :error :parse-error))
            (assoc op :type :info :error (keyword (str/trim err)))))

        :release
        (if-let [t @token]
          (let [{:keys [exit out err]} (db/run-cli service-config servers client-id
                                                    "unlock" resource (str t))]
            (if (zero? exit)
              (if-let [success? (parse-unlock-output out)]
                (if success?
                  (do
                    (reset! token nil)
                    (assoc op :type :ok :value t))
                  (assoc op :type :fail :error :invalid-token :value t))
                (assoc op :type :info :error :parse-error))
              (assoc op :type :info :error (keyword (str/trim err)))))
          ;; No token held - can't release
          (assoc op :type :fail :error :no-token)))

      (catch Exception e
        (assoc op :type :info :error [:exception (str e)]))))

  (teardown! [this test])

  (close! [this test]))

(defn client []
  (map->Client {}))

;; ============================================================================
;; Generators
;; ============================================================================

(defn acquire [_ _] {:type :invoke, :f :acquire, :value nil})
(defn release [_ _] {:type :invoke, :f :release, :value nil})

;; ============================================================================
;; Test
;; ============================================================================

(defn test
  "Constructs a Jepsen test for dlock mutual exclusion."
  [opts]
  (merge tests/noop-test
         opts
         {:name       "graft-dlock"
          :local?     true
          :nodes      ["n1" "n2" "n3"]
          :ssh        {:dummy? true}
          :os         os/noop
          :db         (db/local-db db/dlock-config)
          :client     (client)
          :nemesis    nemesis/noop
          :checker    (checker/compose
                       {:timeline (timeline/html)
                        :linear   (checker/linearizable
                                   {:model     (lock)
                                    :algorithm :wgl})})
          :generator  (->> (gen/mix [acquire release])
                           (gen/stagger 1/50)
                           (gen/clients)
                           (gen/time-limit (:time-limit opts 30)))}))
