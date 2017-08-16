(ns stress-test-carmine-tasks.core
  (:require [clojure.pprint :refer [pprint]]
            [taoensso.carmine :as r]
            [taoensso.carmine.message-queue :as mq]
            [taoensso.encore :as enc]
            [environ.core :refer [env]]
            [clojure.java.shell :refer [sh]])
  (:gen-class))



(def redis-conn
  (let [url "redis://127.0.0.1:6379"]
    (println "Connecting to Redis at:" url)
    {:pool {} :spec {:uri url}}))

(defmacro wcar* [& body] `(r/wcar redis-conn ~@body))

(defn test-handler [{:keys [message attempt]}]
  (try
    (/ 0 0)
    ;; (doseq [i (range 10000)]
    ;;     ;; (println "Writing key in " message)
    ;;     (wcar* (r/set (str "key-" (rand-int 100)) "yes"))
    ;;     (Thread/sleep 100))
    (println "SUCCESSFULLY FINISHED HANDLER (SHOULD NOT HAPPEN)")
    {:status :success}
    (catch Exception e
      (println "Exception catched!")
      ;; XXX TO TEST, ADD OR REMOVE THIS LINE
      ;; (throw e)
      {:status :retry})))


(defn get-tasks []
  (mq/queue-status redis-conn "tasks"))

(defn get-tasks-ids []
  (map first (:messages (get-tasks))))

(defn get-tasks-status []
  (map (fn [mid]
         [mid
          (r/wcar redis-conn
                  (mq/message-status "tasks" mid))])
       (get-tasks-ids)))

(def successfully-added (atom 0))

(defn make-stressed-worker [& [poll-ms]]
  (mq/worker redis-conn
             "tasks"
             {:handler test-handler
              :monitor (fn [{:keys [mid-circle-size]}]
                         (let [tasks (get-tasks)
                               done (:done tasks)]
                           (when-not (empty? done)
                             (println "FOUND DONE TAKS:")
                             (pprint done))))}
             :eoq-backoff-ms (or poll-ms 100)))

(defn sh-with-sudo [cmd]
  (sh "bash" "-c" (format "echo %s | sudo -S %s" (env :tmp-pass) cmd)))


(defn -main
  [& args]
  (wcar* (r/flushall))
  (make-stressed-worker 10)
  (future
    (doseq [i (range 1 20)]
      (Thread/sleep 200)
      (try
        (wcar*
         (mq/enqueue "tasks"
                     (str "task ID " i)
                     (enc/uuid-str)
                     "true"))
        (println "Added task" i)
        (swap! successfully-added inc)
        (catch Exception e
          (pprint e)))))
  #_(future
    ;; This needs to add up to 20 secs max
    (doseq [i (range 1 50)]
      (Thread/sleep (+ 100 (rand-int 100)))
      (println "STOPPING REDIS...")
      (sh-with-sudo "/etc/init.d/redis stop")
      (Thread/sleep (+ 1000 (rand-int 100)))
      (println "STARTING REDIS...")
      (sh-with-sudo "/etc/init.d/redis start")))
  ;; Wait until tasks are created and Redis has been restarted
  (Thread/sleep 5000)
  ;; At this point all tasks should be added (or at least tried), and in `successfully-added`,
  ;; and none should be done, since we waited less than 20 seconds
  (println "Test 1:"
           (= (count (get-tasks-ids))
              @successfully-added))
  (println "Number of tasks in Carmine's queue:" (count (get-tasks-ids)))
  (println "Number of tasks added:" @successfully-added)
  (println "Test 2:"
           (every? (fn [[task-id status]]
                     (or (= status :queued)
                         (= status :locked)))
                   (get-tasks-status))))
