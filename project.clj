(defproject lwtclient "0.3.0"
  :description "Performs LWT operations against a Cassandra cluster"
  :url "https://github.com/jkni/lwtclient"
  :license {:name "The Apache Software License, Version 2.0"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/tools.cli "0.3.3"]
                 [cc.qbits/alia "4.0.1"]
                 [ch.qos.logback/logback-classic "1.1.5"]]
  :main ^:skip-aot lwtclient.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
