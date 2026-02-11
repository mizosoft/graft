(ns jepsen.graft.client
  "Client for Graft kvstore using the CLI binary."
  (:require [clojure.java.shell :as shell]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.client :as client]
            [jepsen.graft.db :as db]))

(defn cli-binary-path
  "Find the kvstore-cli binary."
  []
  (let [candidates ["./bin/kvstore-cli"
                    "../bin/kvstore-cli"
                    "../../bin/kvstore-cli"]]
    (or (first (filter #(.exists (clojure.java.io/file %)) candidates))
        (throw (ex-info "kvstore-cli binary not found"
                        {:candidates candidates})))))

(defn servers-string
  "Build the --servers argument string for the CLI."
  [nodes local?]
  (->> nodes
       (map (fn [n]
              (let [node-name (name n)
                    port (db/service-port n)]
                (if local?
                  (str node-name "=localhost:" port)
                  (str node-name "=" node-name ":" port)))))
       (str/join ",")))

(defn run-cli
  "Run kvstore-cli with given arguments. Returns {:exit, :out, :err}."
  [binary servers client-id & args]
  (apply shell/sh binary
         "--servers" servers
         "--id" client-id
         args))

(defn parse-get-output
  "Parse output from 'get' command. Returns [exists? value]."
  [out]
  (let [trimmed (str/trim out)]
    (if (= trimmed "(nil)")
      [false nil]
      [true trimmed])))

(defn parse-put-output
  "Parse output from 'put' command. Returns true if successful."
  [out]
  (str/starts-with? (str/trim out) "OK"))

(defn parse-cas-output
  "Parse output from 'cas' command. Returns true if successful."
  [out]
  (= (str/trim out) "OK"))

(defrecord Client [binary servers client-id local?]
  client/Client
  (open! [this test node]
    (let [local? (get test :local? false)
          nodes (:nodes test)]
      (assoc this
             :binary (cli-binary-path)
             :servers (servers-string nodes local?)
             :client-id (str "jepsen-" (name node) "-" (rand-int 100000))
             :local? local?)))

  (setup! [this test])

  (invoke! [this test op]
    (try
      (case (:f op)
        :read
        (let [{:keys [exit out err]} (run-cli binary servers client-id "get" "register")]
          (if (zero? exit)
            (let [[exists? value] (parse-get-output out)]
              (assoc op
                     :type :ok
                     :value (when exists? (Long/parseLong value))))
            (assoc op :type :info :error (keyword (str/trim err)))))

        :write
        (let [{:keys [exit out err]} (run-cli binary servers client-id
                                              "put" "register" (str (:value op)))]
          (if (zero? exit)
            (assoc op :type :ok)
            (assoc op :type :info :error (keyword (str/trim err)))))

        :cas
        (let [[expected new] (:value op)
              {:keys [exit out err]} (run-cli binary servers client-id
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

(defn client
  "Create a new kvstore CLI client."
  []
  (map->Client {}))
