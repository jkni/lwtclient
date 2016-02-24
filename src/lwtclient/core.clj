(ns lwtclient.core
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.string :as str]
            [qbits.alia :as alia]
            [qbits.hayt :as h])
  (:import (com.datastax.driver.core.exceptions UnavailableException
                                                WriteTimeoutException
                                                ReadTimeoutException
                                                NoHostAvailableException))
  (:gen-class))

(def cli-options
  [["-r" "--registers COUNT" "Number of registers to use"
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

(defn -main
  "Entry point for CALI app"
  [& args]
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)]
    (cond
      (:help options) (println summary)
      errors (do (println summary) (println errors) (System/exit 1)))
    (try
      (let [cluster (alia/cluster {:contact-points (:hosts options)})
            session (alia/connect cluster)
            _ (alia/execute session (h/use-keyspace "lwtclient"))
            provided-time-base (:start-time options)
            relative-time-base (linear-time-nanos)
            corrected-time (fn [] (+ provided-time-base (- (linear-time-nanos) relative-time-base)))
            upper-bound (:upper-bound options)
            register-count (:registers options)
            prepared-read (alia/prepare session "SELECT * FROM REGISTERS2 WHERE ID = ?")
            prepared-write (alia/prepare session "UPDATE REGISTERS2 SET contents=? WHERE id=? IF EXISTS")
            prepared-cas (alia/prepare session "UPDATE REGISTERS2 SET contents=? WHERE id=? IF contents=?")]
        (dotimes [n (:operation-count options)]
          (println n)
          (let [f (rand-nth [:write :cas :read])
                register (rand-int register-count)
                op {:type :invoke :f f
                    :time (corrected-time)
                    :register register}]
            (println op)
            (case f
              :cas (try
                     (let [[v v'] [(rand-int upper-bound) (rand-int upper-bound)]
                           result (alia/execute session prepared-cas {:values [v' register v]
                                                                      :consistency :all})]
                       (println [v v'])
                       (if (-> result first ak)
                         (println (assoc op :type :ok :value [v v'] :time (corrected-time)))
                         (do
                           (println (-> result first))
                           (println (assoc op :type :fail :value (-> result first :value) :time (corrected-time))))))
                     (catch UnavailableException e
                       (println (assoc op :type :fail :time (corrected-time))))
                     (catch ReadTimeoutException e
                       (println (assoc op :type :info :value :read-timed-out :time (corrected-time))))
                     (catch WriteTimeoutException e
                       (println (assoc op :type :info :value :write-timed-out :time (corrected-time))))
                     (catch NoHostAvailableException e
                       (Thread/sleep 1000)
                       (println (assoc op :type :fail :time (corrected-time)))))
              :write (try
                       (let [v (rand-int upper-bound)
                             result (alia/execute session prepared-write {:values [v register]})]
                         (if (-> result first ak)
                           (println (assoc op :type :ok :value v))
                           (let [result' (alia/execute session "INSERT INTO registers2 (id, contents) VALUES (register, v) IF NOT EXISTS")]
                             (println (-> result' first))
                             (if (-> result' first ak)
                               (println (assoc op :type :ok :value v :time (corrected-time)))
                               (println (assoc op :type :fail :value v :time (corrected-time)))))))
                       (catch UnavailableException e
                         (println (assoc op :type :fail :time (corrected-time))))
                       (catch ReadTimeoutException e
                         (println (assoc op :type :info :value :read-timed-out :time (corrected-time))))
                       (catch WriteTimeoutException e
                         (println (assoc op :type :info :value :write-timed-out :time (corrected-time))))
                       (catch NoHostAvailableException e
                         (Thread/sleep 1000)
                         (println (assoc op :type :fail :time (corrected-time)))))
              :read (try
                      (let [value (-> (alia/execute session prepared-read {:values [register]
                                                                           :serial-consistency :serial} )
                                      first :contents)]
                        (println (assoc op :type :ok :value value :time (corrected-time))))
                      (catch UnavailableException e
                        (println (assoc op :type :fail :time (corrected-time))))
                      (catch ReadTimeoutException e
                        (println (assoc op :type :fail :time (corrected-time))))
                      (catch WriteTimeoutException e
                        (println (assoc op :type :fail :time (corrected-time))))
                      (catch NoHostAvailableException e
                        (Thread/sleep 1000)
                        (println (assoc op :type :fail :time (corrected-time))))))))
        (alia/shutdown session)
        (alia/shutdown cluster))
      (catch Exception e
        (println e)
        (System/exit 1)))))
