(ns jepsen.graft
  "Jepsen tests for Graft services - Raft-based distributed systems."
  (:require [jepsen.cli :as cli]
            [jepsen.graft.kvstore :as kvstore]
            [jepsen.graft.msgq :as msgq]
            [jepsen.graft.dlock :as dlock]))

(def cli-opts
  "Additional CLI options for Graft tests."
  [[nil "--service SERVICE" "Service to test (kvstore, msgq, dlock)"
    :default "kvstore"
    :validate [#{"kvstore" "msgq" "dlock"} "Must be kvstore, msgq, or dlock"]]])

(defn -main
  "CLI entry point."
  [& args]
  (cli/run! (merge (cli/single-test-cmd
                    {:test-fn (fn [opts]
                                (case (:service opts)
                                  "kvstore" (kvstore/test opts)
                                  "msgq" (msgq/test opts)
                                  "dlock" (dlock/test opts)))
                     :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
