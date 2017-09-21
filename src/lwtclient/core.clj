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
  [["-r" "--register-set SET" "Set of registers to operate against, as comma-separated list like 3,4,5"
    :default [(int 1)]
    :parse-fn (fn [arg]
                  (as-> arg v
                        (str/split v #",")
                        (map #(Integer/parseInt %) v)
                        (set v)
                        (vec v)))
    :validate [#(seq %) "Must provide at least one register"]]
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
    :default ["localhost"]
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
    (let [[v v'] (:value op)
          result (alia/execute session pst {:values [v' (:register op) v]})]
      (if (-> result first ak)
        (assoc op :type :ok)
        (assoc op :type :fail)))
    (catch Exception e
      (let [driver-exception (.getCause e)]
        (cond
          (instance? UnavailableException driver-exception) (assoc op :type :fail :cause :unavailable)
          (instance? ReadTimeoutException driver-exception) (assoc op :type :info :cause :read-timed-out)
          (instance? WriteTimeoutException driver-exception) (assoc op :type :info :cause :write-timed-out)
          (instance? NoHostAvailableException driver-exception) (do (Thread/sleep 1000) (assoc op :type :fail :cause :nohost))
          :else (assoc op :type :error :cause :unhandled-exception :details (.toString e)))))))

(defn write
  [session upper-bound pst pst-non-existent op]
  (try
    (let [v (:value op)
          register (:register op)
          result (alia/execute session pst {:values [v register]})]
      (if (-> result first ak)
        (assoc op :type :ok)
        (let [result' (alia/execute session pst-non-existent {:values [register v]})]
          (if (-> result' first ak)
            (assoc op :type :ok)
            (assoc op :type :fail)))))
    (catch Exception e
      (let [driver-exception (.getCause e)]
        (cond
          (instance? UnavailableException driver-exception) (assoc op :type :fail :cause :unavailable)
          (instance? ReadTimeoutException driver-exception) (assoc op :type :info :cause :read-timed-out)
          (instance? WriteTimeoutException driver-exception) (assoc op :type :info :cause :write-timed-out)
          (instance? NoHostAvailableException driver-exception) (do (Thread/sleep 1000) (assoc op :type :fail :cause :nohost))
          :else (assoc op :type :error :cause :unhandled-exception :details (.toString e)))))))

(defn rread
  [session pst op]
  (try
    (let [value (-> (alia/execute session pst {:values [(:register op)]
                                               :serial-consistency :serial
                                               :consistency :serial})
                    first :contents)]
      (assoc op :type :ok :value value))
    (catch Exception e
      (let [driver-exception (.getCause e)]
        (cond
          (instance? UnavailableException driver-exception) (assoc op :type :fail :cause :unavailable)
          (instance? ReadTimeoutException driver-exception) (assoc op :type :fail :cause :read-timed-out)
          (instance? WriteTimeoutException driver-exception) (assoc op :type :fail :cause :write-timed-out)
          (instance? NoHostAvailableException driver-exception) (do (Thread/sleep 1000) (assoc op :type :fail :cause :nohost))
          :else (assoc op :type :error :cause :unhandled-exception :details (.toString e)))))))

(defn print-schema
  "Prints schema for use"
  []
  (println (slurp (io/resource "lwtclient_schema.cql"))))

(defn- lprintln
  "Println that locks *out*"
  [& args]
  (locking *out*
    (apply println args)))

(let [make-op-lock (Object.)]
  (defn- make-op
    "Build op, timestamp it, print it, and return it"
    [op time-fn]
    (locking make-op-lock
      (let [timestamped-op (assoc op :time (time-fn))]
        (println timestamped-op)
        timestamped-op))))
  
(defn do-lwt-writes
  "Run LWT operations against a cluster"
  [hosts time-base register-set upper-bound count process-counter]
  (try
    (let [cluster (alia/cluster {:contact-points hosts})
          session (alia/connect cluster)
          _ (alia/execute session "USE lwtclient;")
          corrected-time (fn [] (+ time-base (linear-time-nanos)))
          prepared-read (alia/prepare session
                                      "SELECT * FROM registers WHERE ID = ?")
          prepared-write (alia/prepare session
                                       "UPDATE registers SET contents=? WHERE id=? IF EXISTS")
          prepared-write-not-exists (alia/prepare session
                                                  "INSERT INTO registers (id, contents) VALUES (?, ?) IF NOT EXISTS")
          prepared-cas (alia/prepare session
                                     "UPDATE registers SET contents=? WHERE id=? IF contents=?")]
      (loop [n 0
             my-process (swap! process-counter inc)]
        (let [f (rand-nth [:cas :write :read])
              value (case f
                      :cas [(rand-int upper-bound) (rand-int upper-bound)]
                      :write (rand-int upper-bound)
                      :read nil)
              op (make-op {:type :invoke :f f
                           :lwtprocess my-process
                           :value value
                           :register (rand-nth register-set)}
                          corrected-time)
              new-op (make-op (case f
                                :cas (cas session upper-bound prepared-cas op)
                                :write (write session upper-bound prepared-write prepared-write-not-exists op)
                                :read (rread session prepared-read op))
                              corrected-time)]
          (when (< n count)
            (recur (inc n) (if (= :info (:type new-op))
                               (swap! process-counter inc)
                               my-process)))))
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
      (let [{:keys [hosts start-time upper-bound register-set
                    operation-count thread-count]} options
            count (quot operation-count thread-count)
            process-counter (atom 0)
            modified-start-time (- start-time (linear-time-nanos))]
        (dotimes [_ thread-count]
          (future (do-lwt-writes hosts modified-start-time register-set upper-bound count process-counter)))
        (shutdown-agents)))))