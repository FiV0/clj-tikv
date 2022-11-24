(ns clj-tikv.client
  (:import (org.tikv.common TiConfiguration TiSession)
           (org.tikv.common.util ConcreteBackOffer)
           (org.tikv.shade.com.google.protobuf ByteString)
           (org.tikv.txn KVClient TwoPhaseCommitter)))

(def config (TiConfiguration/createDefault "127.0.0.1:2379"))
(def session (TiSession/create config))

(defn- get-timestamp [^TiSession session]
  (.. session (getTimestamp) (getVersion)))

(defn commit-kv [^TiSession session ^bytes k ^bytes v]
  (let [start-ts (get-timestamp session)
        committer (TwoPhaseCommitter. session start-ts)
        back-offer (ConcreteBackOffer/newCustomBackOff 1000)]
    (.prewritePrimaryKey committer back-offer k v)
    (.commitPrimaryKey committer back-offer k (get-timestamp session))))

(comment
  (commit-kv session (.getBytes "foo") (.getBytes "bar"))


  (doseq [i (range 10)]
    (commit-kv session (.getBytes (str "foo" i)) (.getBytes "bar")))
  )

(defn ->client [^TiSession session]
  (.createKVClient session))

(def client (->client session))
(def version (get-timestamp session))

(defn client-get [^KVClient client k]
  (.get client k version))

(def res (client-get client (ByteString/copyFromUtf8 "foo")))
(.toStringUtf8 res)

(defn client-scan
  ([^KVClient client from]
   (.scan client from version))
  ([^KVClient client from to]
   (.scan client from to version)))

(def kv-pairs (client-scan client (ByteString/copyFromUtf8 "foo2") (ByteString/copyFromUtf8 "foo5")))

(defn kv-pair->pair [kv]
  [(.getKey kv) (.getValue kv)])

(defn kv-pair->strings [kv]
  [(.toStringUtf8 (.getKey kv)) (.toStringUtf8 (.getValue kv))])

(->> kv-pairs
     (map kv-pair->strings))
