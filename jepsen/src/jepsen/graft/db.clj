(ns jepsen.graft.db
  "Database setup and teardown for Graft services."
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.java.shell :as shell]
            [jepsen.db :as db]))

;; ============================================================================
;; Configuration
;; ============================================================================

;; We run from jepsen/, so graft root is 1 level up
(def graft-root (.getCanonicalPath (io/file "..")))
(def bin-dir (str graft-root "/jepsen/bin"))

;; Port assignments for each node (when running locally)
(def node-ports
  {"n1" {:service 8001 :raft 9001}
   "n2" {:service 8002 :raft 9002}
   "n3" {:service 8003 :raft 9003}
   "n4" {:service 8004 :raft 9004}
   "n5" {:service 8005 :raft 9005}})


(defn service-port [node]
  (get-in node-ports [(name node) :service] 8001))

(defn raft-port [node]
  (get-in node-ports [(name node) :raft] 9001))

(defn node-id [node]
  (name node))

;; ============================================================================
;; Binary Building
;; ============================================================================

(defn build-binary!
  "Build a Go binary if it doesn't exist.
   service-path is relative to graft root (e.g. './kvstore/service/cmd')"
  [binary-name service-path]
  (let [binary-path (str bin-dir "/" binary-name)]
    (.mkdirs (io/file bin-dir))
    (when-not (.exists (io/file binary-path))
      (info "Building" binary-name "...")
      (let [{:keys [exit err]} (shell/sh "go" "build" "-o" binary-path service-path
                                         :dir graft-root)]
        (when-not (zero? exit)
          (throw (ex-info (str "Failed to build " binary-name ": " err) {:exit exit})))))
    binary-path))

(defn binary-path
  "Get the path to a binary."
  [binary-name]
  (str bin-dir "/" binary-name))

;; ============================================================================
;; Service Configuration
;; ============================================================================

(defrecord ServiceConfig
  [name              ; Service name (e.g. "kvstore")
   server-binary     ; Server binary name (e.g. "kvstore")
   client-binary     ; Client binary name (e.g. "kvstore-cli")
   server-build-path ; Go build path for server (e.g. "./kvstore/service/cmd")
   client-build-path ; Go build path for client (e.g. "./kvstore/client/cmd")
   ])

(def kvstore-config
  (->ServiceConfig "kvstore"
                   "kvstore"
                   "kvstore-cli"
                   "./kvstore/service/cmd"
                   "./kvstore/client/cmd"))

(def msgq-config
  (->ServiceConfig "msgq"
                   "msgq"
                   "msgq-cli"
                   "./msgq/service/cmd"
                   "./msgq/client/cmd"))

(def dlock-config
  (->ServiceConfig "dlock"
                   "dlock"
                   "dlock-cli"
                   "./dlock/service/cmd"
                   "./dlock/client/cmd"))

(defn get-service-config [service-name]
  (case service-name
    "kvstore" kvstore-config
    "msgq" msgq-config
    "dlock" dlock-config
    (throw (ex-info (str "Unknown service: " service-name)
                    {:service service-name}))))

;; ============================================================================
;; Process Management
;; ============================================================================

(defn local-data-dir [service-name node]
  (str "/tmp/jepsen-" service-name "/" (node-id node)))

(defn local-log-file [service-name node]
  (str (local-data-dir service-name node) "/server.log"))

(defn local-pid-file [service-name node]
  (str (local-data-dir service-name node) "/server.pid"))

(defn local-join-string
  "Build the --join argument for local nodes."
  [nodes]
  (->> nodes
       (map (fn [n] (str (node-id n) "=localhost:" (raft-port n))))
       (str/join ",")))

(defn build-service-binaries!
  "Build server and client binaries for a service."
  [service-config]
  (build-binary! (:server-binary service-config) (:server-build-path service-config))
  (build-binary! (:client-binary service-config) (:client-build-path service-config)))

(defn start-local-server!
  "Start a service server process locally."
  [service-config test node]
  (let [service-name (:name service-config)
        dir (local-data-dir service-name node)
        log-file (local-log-file service-name node)
        pid-file (local-pid-file service-name node)
        binary (binary-path (:server-binary service-config))
        port (service-port node)
        join-str (local-join-string (:nodes test))]

    ;; Create data directory
    (.mkdirs (io/file dir))

    (info "Starting" service-name (node-id node) "on port" port)

    ;; Start the process with output redirected to log file
    (let [pb (ProcessBuilder.
              [binary
               "--id" (node-id node)
               "--service-addr" (str ":" port)
               "--join" join-str
               "--wal-dir" (str dir "/wal")
               "--log-file" log-file])
          proc (.start pb)]
      ;; Write PID file
      (spit pid-file (.pid proc))

      ;; Give it time to start
      (Thread/sleep 1000))))

(defn stop-local-server!
  "Stop a service server process locally."
  [service-name node]
  (let [pid-file (local-pid-file service-name node)]
    (when (.exists (io/file pid-file))
      (let [pid (str/trim (slurp pid-file))]
        (info "Stopping" service-name (node-id node) "pid" pid)
        (try
          (shell/sh "kill" "-TERM" pid)
          (Thread/sleep 500)
          ;; Force kill if still running
          (shell/sh "kill" "-9" pid)
          (catch Exception e
            (warn "Error stopping process:" (.getMessage e)))))
      (io/delete-file pid-file true))))

(defn cleanup-local-data!
  "Clean up local data directory."
  [service-name node]
  (let [dir (io/file (local-data-dir service-name node))]
    (when (.exists dir)
      (doseq [f (reverse (file-seq dir))]
        (io/delete-file f true)))))

;; ============================================================================
;; Database Implementation
;; ============================================================================

(defn local-db
  "Create a Jepsen DB for local testing of a Graft service."
  [service-config]
  (let [service-name (:name service-config)]
    (reify db/DB
      (setup! [_ test node]
        (info "Setting up local" service-name "on" node)
        (build-service-binaries! service-config)
        (cleanup-local-data! service-name node)
        (start-local-server! service-config test node)
        ;; Wait for cluster to elect a leader and stabilize
        (Thread/sleep 10000))

      (teardown! [_ test node]
        (info "Tearing down local" service-name "on" node)
        (stop-local-server! service-name node)
        (cleanup-local-data! service-name node))

      db/LogFiles
      (log-files [_ test node]
        [(local-log-file service-name node)]))))

;; ============================================================================
;; CLI Helpers
;; ============================================================================

(defn servers-string
  "Build the --servers argument string for the CLI."
  [nodes local?]
  (->> nodes
       (map (fn [n]
              (let [node-name (name n)
                    port (service-port n)]
                (if local?
                  (str node-name "=localhost:" port)
                  (str node-name "=:" port)))))
       (str/join ",")))

(defn run-cli
  "Run a service CLI with given arguments. Returns {:exit, :out, :err}."
  [service-config servers client-id & args]
  (let [binary (binary-path (:client-binary service-config))]
    (apply shell/sh binary
           "--servers" servers
           "--id" client-id
           args)))
