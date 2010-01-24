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


(declare obj->clojure)

(defn- assocs->clojure [kvs keywordize]
  ;; Taking the keywordize test out of the fn reduces derefs
  ;; dramatically, which was the main barrier to matching pure-Java
  ;; performance for this marshalling
  (reduce (if keywordize
            (fn [m [#^String k v]] (assoc m (keyword k) (obj->clojure v true)))
            (fn [m [#^String k v]] (assoc m k           (obj->clojure v false))))
          {} kvs))

(defn- map->clojure [#^Map m keywordize]
  (assocs->clojure (.entrySet m) keywordize))

(defn- list->clojure [#^List l keywordize]
  (vec (map #(obj->clojure % keywordize) l)))

(defn- obj->clojure [o keywordize]
  (cond
   (.isInstance Map o)  (map->clojure o keywordize)
   (.isInstance List o) (list->clojure o keywordize)
   true                 o))

(defn- dbobject->clojure
  [#^DBObject f keywordize]
  ;; DBObject provides .toMap, but the implementation in
  ;; subclass GridFSFile unhelpfully throws
  ;; UnsupportedOperationException
  (assocs->clojure (for [k (.keySet f)] [k (.get f k)]) keywordize))


(declare clojure-obj->mongo-obj)

(defn- clojure-map->mongo-map [#^IPersistentMap m]
  (let [dbo (BasicDBObject.)]
    (doseq [[k v] m]
      (.put dbo
            (if (keyword? k) (.getName #^Keyword k) k)
            (clojure-obj->mongo-obj v)))
    dbo))

(defn- clojure-obj->mongo-obj [o]
  (cond
   (keyword? o) (.getName #^Keyword o)
   (map? o) (clojure-map->mongo-map #^IPersistentMap o)
   (.isInstance List o) (map clojure-obj->mongo-obj #^List o)
   true o))


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
              [:clojure :mongo  ] clojure-map->mongo-map
              [:clojure :json   ] json-str
              [:mongo   :clojure] #(dbobject->clojure #^DBObject % #^Boolean/TYPE *keywordize*)
              [:mongo   :json   ] #(.toString #^DBObject %)
              [:gridfs  :clojure] #(dbobject->clojure #^GridFSFile % *keywordize*)
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

