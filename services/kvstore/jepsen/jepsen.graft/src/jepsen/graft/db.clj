(ns jepsen.graft.db
  "Database setup and teardown for Graft kvstore."
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.java.shell :as shell]
            [jepsen.control :as c]
            [jepsen.db :as db]))

;; ============================================================================
;; Configuration
;; ============================================================================

(def binary-src "/opt/kvstore/kvstore")
(def binary-dest "/usr/local/bin/kvstore")
(def data-dir "/var/lib/kvstore")
(def log-file "/var/log/kvstore.log")
(def pid-file "/var/run/kvstore.pid")

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
;; Local Database (runs processes directly on the machine)
;; ============================================================================

;; We run from kvstore/jepsen/jepsen.graft/, so graft root is 3 levels up
(def graft-root (.getCanonicalPath (io/file "../../..")))
(def bin-dir (str graft-root "/kvstore/jepsen/jepsen.graft/bin"))

(defn build-binaries!
  "Build kvstore and kvstore-cli binaries if they don't exist."
  []
  (let [kvstore-bin (str bin-dir "/kvstore")
        cli-bin (str bin-dir "/kvstore-cli")]

    ;; Create bin directory
    (.mkdirs (io/file bin-dir))

    ;; Build kvstore server if missing
    (when-not (.exists (io/file kvstore-bin))
      (info "Building kvstore binary...")
      (let [{:keys [exit err]} (shell/sh "go" "build" "-o" kvstore-bin "./kvstore/service/cmd"
                                         :dir graft-root)]
        (when-not (zero? exit)
          (throw (ex-info (str "Failed to build kvstore: " err) {:exit exit})))))

    ;; Build kvstore-cli if missing
    (when-not (.exists (io/file cli-bin))
      (info "Building kvstore-cli binary...")
      (let [{:keys [exit err]} (shell/sh "go" "build" "-o" cli-bin "./kvstore/client/cmd"
                                         :dir graft-root)]
        (when-not (zero? exit)
          (throw (ex-info (str "Failed to build kvstore-cli: " err) {:exit exit})))))))

(defn local-data-dir [node]
  (str "/tmp/jepsen-kvstore/" (node-id node)))

(defn local-log-file [node]
  (str (local-data-dir node) "/kvstore.log"))

(defn local-pid-file [node]
  (str (local-data-dir node) "/kvstore.pid"))

(defn local-join-string
  "Build the --join argument for local nodes."
  [nodes]
  (->> nodes
       (map (fn [n] (str (node-id n) "=localhost:" (raft-port n))))
       (str/join ",")))

(defn local-binary-path []
  (str bin-dir "/kvstore"))

(defn cli-binary-path []
  (str bin-dir "/kvstore-cli"))

(defn start-local-kvstore!
  "Start kvstore process locally."
  [test node]
  (let [dir (local-data-dir node)
        log-file (local-log-file node)
        pid-file (local-pid-file node)
        binary (local-binary-path)
        port (service-port node)
        join-str (local-join-string (:nodes test))]

    ;; Create data directory
    (.mkdirs (io/file dir))

    (info "Starting kvstore" (node-id node) "on port" port)

    ;; Start the process with output redirected to log file
    (let [pb (ProcessBuilder.
              [(str binary)
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

(defn stop-local-kvstore!
  "Stop kvstore process locally."
  [node]
  (let [pid-file (local-pid-file node)]
    (when (.exists (io/file pid-file))
      (let [pid (str/trim (slurp pid-file))]
        (info "Stopping kvstore" (node-id node) "pid" pid)
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
  [node]
  (let [dir (io/file (local-data-dir node))]
    (when (.exists dir)
      (doseq [f (reverse (file-seq dir))]
        (io/delete-file f true)))))

(defn local-db
  "Graft kvstore database for local testing (no Docker/SSH)."
  []
  (reify db/DB
    (setup! [_ test node]
      (info "Setting up local kvstore on" node)
      (build-binaries!)
      (cleanup-local-data! node)
      (start-local-kvstore! test node)
      ;; Wait for cluster to elect a leader and stabilize (5 nodes need time)
      (Thread/sleep 10000))

    (teardown! [_ test node]
      (info "Tearing down local kvstore on" node)
      (stop-local-kvstore! node)
      (cleanup-local-data! node))

    db/LogFiles
    (log-files [_ test node]
      [(local-log-file node)])))

;; ============================================================================
;; Remote Database (original Docker/SSH version)
;; ============================================================================

(defn join-string
  "Build the --join argument string for remote nodes."
  [nodes]
  (->> nodes
       (map (fn [n] (str (node-id n) "=" (name n) ":" (raft-port n))))
       (str/join ",")))

(defn remote-db
  "Graft kvstore database for remote testing (Docker/SSH)."
  []
  (reify db/DB
    (setup! [_ test node]
      (info "Setting up kvstore on" node)
      (c/exec :cp binary-src binary-dest)
      (c/exec :chmod :+x binary-dest)
      (c/exec :mkdir :-p data-dir)
      ;; Start kvstore
      (c/exec :start-stop-daemon
              :--start
              :--background
              :--make-pidfile
              :--pidfile pid-file
              :--exec binary-dest
              :--
              :--id (node-id node)
              :--service-addr (str ":" (service-port node))
              :--join (join-string (:nodes test))
              :--wal-dir data-dir
              :> log-file
              (c/lit "2>&1"))
      (Thread/sleep 3000))

    (teardown! [_ test node]
      (info "Tearing down kvstore on" node)
      (c/exec :start-stop-daemon
              :--stop
              :--pidfile pid-file
              :--retry "TERM/5/KILL/5"
              :|| :true)
      (c/exec :rm :-rf data-dir)
      (c/exec :rm :-f log-file pid-file))

    db/LogFiles
    (log-files [_ test node]
      [log-file])))

;; Default to local for easier debugging
(def db local-db)
