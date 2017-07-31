(ns lwtclient.core
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [qbits.alia :as alia])
  (:import (com.datastax.driver.core.exceptions UnavailableException
                                                WriteTimeoutException
                                                ReadTimeoutException
                                                NoHostAvailableException))
  (:gen-class))

(def cli-options
  [["-r" "--register-start ID" "Starting register id"
    :default 1
    :parse-fn #(Integer/parseInt %)
    :validate [#(< 0 %) "Must be greater than 0"]]
   ["-t" "--thread-count COUNT" "Number of LWT threads to run concurrently"
    :default 1
    :parse-fn #(Integer/parseInt %)
    :validate [#(< 0 %) "Must be greater than 0"]]
   ["-s" "--start-time TIME" "Starting relative time in nanoseconds"
    :default 0
    :parse-fn #(Long/parseLong %)]
   ["-n" "--operation-count COUNT" "Number of operations to perform"
    :default 10000
    :parse-fn #(Integer/parseInt %)
    :validate [#(< 0 %) "Must be greater than 0"]]
   ["-u" "--upper-bound BOUND" "Upper bound of values a register can hold"
    :default 5
    :parse-fn #(Integer/parseInt %)
    :validate [#(< -1 %) "Must be greater than or equal to 0"]]
   ["-p" "--print-schema" "Schema to use for cluster under load"]
   ["-H" "--hosts HOSTS" "Hosts to contact"
    :default "localhost"
    :parse-fn #(str/split % #",")
    :validate [#(seq %) "Must provide at least one host"]]
   ["-h" "--help"]])

(def ak (keyword "[applied]"))

(defn ^Long linear-time-nanos
  "A linear time source in nanoseconds."
  []
  (System/nanoTime))

(defn cas
  [session upper-bound pst op]
  (try
    (let [[v v'] [(rand-int upper-bound) (rand-int upper-bound)]
          result (alia/execute session pst {:values [v' (:register op) v]})]
      (if (-> result first ak)
        (assoc op :type :ok :value [v v'])
        (assoc op :type :fail :value [v v'])))
    (catch UnavailableException e
      (assoc op :type :fail :value :unavailable))
    (catch ReadTimeoutException e
      (assoc op :type :info :value :read-timed-out))
    (catch WriteTimeoutException e
      (assoc op :type :info :value :write-timed-out))
    (catch NoHostAvailableException e
      (Thread/sleep 1000)
      (assoc op :type :fail :value :nohost))))

(defn write
  [session upper-bound pst pst-non-existent op]
  (try
    (let [v (rand-int upper-bound)
          register (:register op)
          result (alia/execute session pst {:values [v register]})]
      (if (-> result first ak)
        (assoc op :type :ok :value v)
        (let [result' (alia/execute session pst-non-existent {:values [register v]})]
          (if (-> result' first ak)
            (assoc op :type :ok :value v)
            (assoc op :type :fail :value v)))))
    (catch UnavailableException e
      (assoc op :type :fail))
    (catch ReadTimeoutException e
      (assoc op :type :info :value :read-timed-out))
    (catch WriteTimeoutException e
      (assoc op :type :info :value :write-timed-out))
    (catch NoHostAvailableException e
      (Thread/sleep 1000)
      (assoc op :type :fail))))

(defn rread
  [session pst op]
  (try
    (let [value (-> (alia/execute session pst {:values [(:register op)]
                                               :serial-consistency :serial} )
                    first :contents)]
      (assoc op :type :ok :value value))
    (catch UnavailableException e
      (assoc op :type :fail))
    (catch ReadTimeoutException e
      (assoc op :type :fail))
    (catch WriteTimeoutException e
      (assoc op :type :fail))
    (catch NoHostAvailableException e
      (Thread/sleep 1000)
      (assoc op :type :fail))))

(defn print-schema
  "Prints schema for use"
  []
  (println (slurp (io/resource "lwtclient_schema.cql"))))

(defn- lprintln
  "Println that locks *out*"
  [& args]
  (locking *out*
    (apply println args)))

(defn do-lwt-writes
  "Run LWT operations against a cluster"
  [hosts time-base register upper-bound count]
  (try
    (let [cluster (alia/cluster {:contact-points hosts})
          session (alia/connect cluster)
          _ (alia/execute session "USE lwtclient;")
          relative-time-base (linear-time-nanos)
          corrected-time (fn [] (+ time-base (- (linear-time-nanos) relative-time-base)))
          prepared-read (alia/prepare session
                                      "SELECT * FROM registers WHERE ID = ?")
          prepared-write (alia/prepare session
                                       "UPDATE registers SET contents=? WHERE id=? IF EXISTS")
          prepared-write-not-exists (alia/prepare session
                                                  "INSERT INTO registers (id, contents) VALUES (?, ?) IF NOT EXISTS")
          prepared-cas (alia/prepare session
                                     "UPDATE registers SET contents=? WHERE id=? IF contents=?")
          process (atom 0)]
      (dotimes [n count]
        (let [f (rand-nth [:cas :write :read])
              op {:type :invoke :f f
                  :time (corrected-time)
                  :process @process
                  :register register}
              new-op (assoc (case f
                              :cas (cas session upper-bound prepared-cas op)
                              :write (write session upper-bound prepared-write
                                            prepared-write-not-exists op)
                              :read (rread session prepared-read op))
                            :time (corrected-time))]
          (when (= :info (:type new-op))
            (swap! process inc))
          (lprintln op)
          (lprintln new-op)))
      (alia/shutdown session)
      (alia/shutdown cluster))
    (catch Exception e
      (println e)
      (System/exit 1))))

(defn -main
  "Entry point for CLI app"
  [& args]
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)]
    (cond
      (:help options) (println summary)
      (:print-schema options) (print-schema)
      errors (do (println summary) (println errors) (System/exit 1))
      :else
      (let [{:keys [hosts start-time upper-bound register-start
                    operation-count thread-count]} options
            count (quot operation-count thread-count)]
        (doseq [n (range register-start (inc (+ register-start thread-count)))]
          (future (do-lwt-writes hosts start-time (int n) upper-bound count)))
        (shutdown-agents)))))
