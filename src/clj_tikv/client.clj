(ns clj-tikv.client
  (:refer-clojure :exclude [get])
  (:import (org.tikv.common TiConfiguration TiSession)
           (org.tikv.common.util ConcreteBackOffer)
           (org.tikv.shade.com.google.protobuf ByteString)
           (org.tikv.txn KVClient TwoPhaseCommitter)))

;; operations needed
;; put
;; delete
;; get
;; scan with limit
;; prefix scan

(comment
  (def config (TiConfiguration/createDefault "127.0.0.1:2379"))
  (def session (TiSession/create config)))

(def empty-value ByteString/EMPTY)

(defn ->session
  "Creates a TiSession from `url`"
  [url]
  (TiSession/create (TiConfiguration/createDefault url)))

(defn- get-timestamp
  "Get a timestamp from a TiSession."
  [^TiSession session]
  (.. session (getTimestamp) (getVersion)))

(defn commit-kv
  "Commit a key/value pair using the given TiSession.
  Returns the commit timestamp."
  ([^TiSession session kv] (commit-kv session (first kv) (second kv) {}))
  ([^TiSession session ^bytes k ^bytes v {:keys [max-sleep] :or {max-sleep 1000} :as _opts}]
   (let [start-ts (get-timestamp session)
         committer (TwoPhaseCommitter. session start-ts)
         back-offer (ConcreteBackOffer/newCustomBackOff max-sleep)
         _ (.prewritePrimaryKey committer back-offer k v)
         commit-ts (get-timestamp session)]
     (.commitPrimaryKey committer back-offer k commit-ts)
     commit-ts)))

(defn commit-kvs
  "Commits a list of key/value pairs using the given TiSession.
  Returns the commit timestamp."
  ([^TiSession session kvs] (commit-kvs session kvs {}))
  ([^TiSession session kvs {:keys [max-sleep] :or {max-sleep 1000} :as _opts}]
   (let [start-ts (get-timestamp session)
         committer (TwoPhaseCommitter. session start-ts)
         back-offer (ConcreteBackOffer/newCustomBackOff max-sleep)
         _ (doseq [[k v] kvs]
             (.prewritePrimaryKey committer back-offer k v))
         commit-ts (get-timestamp session)]
     (doseq [[k _] kvs]
       (.commitPrimaryKey committer back-offer k commit-ts))
     commit-ts)))

(comment
  (def session (->session "127.0.0.1:2379"))
  (commit-kv session [(.getBytes "foo") (.getBytes "bar")])

  (doseq [i (range 10)]
    (commit-kv session [(.getBytes (str "foo" i)) (.getBytes "bar")]))
  )

(defrecord Client [^KVClient kv-client ^TiSession session])

(defn ->client
  "Creates a Client from TiSession."
  [^TiSession session]
  (->Client (.createKVClient session) session))

(defn get
  "Get value for key if existent. Returns `empty-value` if there is none.
  Value returned as `BtyeString`."
  [^Client {:keys [kv-client session] :as _client} key]
  (let [timestamp (get-timestamp session)]
    {:key (.get kv-client key timestamp) :ts timestamp}))

(comment
  (def client (->client session))
  (def res (get client (ByteString/copyFromUtf8 "foo")))
  (-> res :key (.toStringUtf8)))

(defn- byte-string [key]
  (cond
    (instance? ByteString key) key
    (bytes? key) (ByteString/copyFrom key)
    :else (throw (ex-info "Don't know how to create byte string for key!" {:key key :type (type key)}))))

(defn- kv-pair->pair [kv]
  [(.getKey kv) (.getValue kv)])

(defn get-kv
  "Get first key/value if existent that matches prefix of the given key"
  [^Client {:keys [kv-client session] :as _client} key]
  (let [timestamp (get-timestamp session)
        [k _v :as kv] (kv-pair->pair (first (.scan kv-client (byte-string key) timestamp 111)))]
    (if (.startsWith k key)
      {:kv kv :ts timestamp}
      {:kv [empty-value empty-value] :ts timestamp})))

(defn client-scan
  "Scan up to `limit` keys from `from` (optionally up to `to`)."
  ([^Client {:keys [kv-client session] :as _client} from limit]
   (let [timestamp (get-timestamp session)]
     (.scan kv-client (byte-string from) timestamp limit)))
  ([^KVClient {:keys [kv-client session] :as _client} from to limit]
   (let [timestamp (get-timestamp session)]
     (take limit (.scan kv-client (byte-string from) (byte-string to) timestamp)))))

(comment
  (def kv-pairs (client-scan client (ByteString/copyFromUtf8 "fo") (ByteString/copyFromUtf8 "foob") 13))

  (defn kv-pair->strings [kv]
    [(.toStringUtf8 (.getKey kv)) (.toStringUtf8 (.getValue kv))])

  (map kv-pair->strings kv-pairs)

  (def client (.createKVClient session))
  (def timestamp (get-timestamp session))

  (->> (.scan client (ByteString/copyFromUtf8 "foo") (ByteString/copyFromUtf8 "foo5") timestamp)
       (map kv-pair->strings))
  ;; => (["foo" "bar"] ["foo0" "bar"] ["foo1" "bar"] ["foo2" "bar"] ["foo3" "bar"] ["foo4" "bar"])

  (.scan client (ByteString/copyFromUtf8 "foo") timestamp 5)
  ;; => Execution error (IndexOutOfBoundsException) at org.tikv.common.operation.iterator.ScanIterator/cacheLoadFails (ScanIterator.java:100).
  ;;    current cache size = 11, larger than 5


  (.scan client (ByteString/copyFromUtf8 "foo") timestamp 12)
  ;; => []
  (.scan client (ByteString/copyFromUtf8 "foo") timestamp)
;; => []



  )
