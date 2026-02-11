(defproject jepsen.graft "0.1.0-SNAPSHOT"
  :description "Jepsen tests for kvstore"
  :url "https://github.com/mizosoft/graft"
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [jepsen "0.3.5"]
                 [clj-http "3.12.3"]
                 [cheshire "5.11.0"]]
  :jvm-opts ["-Xmx8g" "-Xms2g" "-server"]
  :main jepsen.graft
  :repl-options {:init-ns jepsen.graft})
