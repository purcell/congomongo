(ns somnium.congomongo.coerce
  (:use [somnium.congomongo.util :only [defunk]]
        [clojure.contrib.json read write]
        [clojure.contrib.def :only [defvar]]
        [clojure.contrib.core :only [seqable?]])
  (:import [clojure.lang IPersistentMap IPersistentVector Keyword]
           [java.util Map List]
           [com.mongodb DBObject BasicDBObject]
           [com.mongodb.gridfs GridFSFile]
           [com.mongodb.util JSON]))

(defvar *keywordize* true
  "Set this to false to prevent ClojureDBObject from setting string keys to keywords")


;;; Converting data from mongo into Clojure data objects

(defn- mongo->clojure-dispatch [o keywordize]
  (class o))

(defmulti mongo->clojure mongo->clojure-dispatch)

(defn- assocs->clojure [kvs keywordize]
  ;; Taking the keywordize test out of the fn reduces derefs
  ;; dramatically, which was the main barrier to matching pure-Java
  ;; performance for this marshalling
  (reduce (if keywordize
            (fn [m [#^String k v]] (assoc m (keyword k) (mongo->clojure v true)))
            (fn [m [#^String k v]] (assoc m k           (mongo->clojure v false))))
          {} kvs))

(defmethod mongo->clojure Map
  [#^Map m keywordize]
  (assocs->clojure (.entrySet m) keywordize))

(defmethod mongo->clojure List
  [#^List l keywordize]
  (vec (map #(mongo->clojure % keywordize) l)))

(defmethod mongo->clojure :default
  [o keywordize]
  o)

(defmethod mongo->clojure DBObject
  [#^DBObject f keywordize]
  ;; DBObject provides .toMap, but the implementation in
  ;; subclass GridFSFile unhelpfully throws
  ;; UnsupportedOperationException
  (assocs->clojure (for [k (.keySet f)] [k (.get f k)]) keywordize))

(prefer-method mongo->clojure DBObject Map)


;;; Converting data from Clojure into data objects suitable for Mongo

(defmulti clojure->mongo class)

(defmethod clojure->mongo IPersistentMap
  [#^IPersistentMap m]
  (let [dbo (BasicDBObject.)]
    (doseq [[k v] m]
      (.put dbo
            (if (keyword? k) (.getName #^Keyword k) k)
            (clojure->mongo v)))
    dbo))

(defmethod clojure->mongo Keyword
  [#^Keyword o]
  (.getName o))

(defmethod clojure->mongo List
  [#^List o]
  (map clojure->mongo o))

(defmethod clojure->mongo :default
  [o]
  o)



(defunk coerce
  {:arglists '([obj [:from :to] {:many false}])
   :doc
   "takes an object, a vector of keywords:
    from [ :clojure :mongo :json ]
    to   [ :clojure :mongo :json ],
    and an an optional :many keyword parameter which defaults to false"}
  [obj from-to :many false]
  (if (= (from-to 0) (from-to 1))
      obj
      (let [fun
            (condp = from-to
              [:clojure :mongo  ] clojure->mongo
              [:clojure :json   ] json-str
              [:mongo   :clojure] #(mongo->clojure #^DBObject % #^Boolean/TYPE *keywordize*)
              [:mongo   :json   ] #(.toString #^DBObject %)
              [:gridfs  :clojure] #(mongo->clojure #^GridFSFile % *keywordize*)
              [:json    :clojure] #(binding [*json-keyword-keys* *keywordize*] (read-json %))
              [:json    :mongo  ] #(JSON/parse %)
              :else               (throw (RuntimeException.
                                          "unsupported keyword pair")))]
        (if many (map fun (if (seqable? obj)
                            obj
                            (iterator-seq obj))) (fun obj)))))

(defn coerce-fields
  "only used for creating argument object for :only"
  [fields]
  (clojure-map->mongo-map #^IPersistentMap (zipmap fields (repeat 1))))

