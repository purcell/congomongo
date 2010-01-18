(ns somnium.congomongo.coerce
  (:use [somnium.congomongo.util :only [defunk]]
        [clojure.contrib.json read write]
        [clojure.contrib.def :only [defvar]]
        [clojure.contrib.core :only [seqable?]])
  (:import [somnium.congomongo ClojureDBObject]
           [clojure.lang IPersistentMap]
           [com.mongodb DBObject]
           [com.mongodb.gridfs GridFSFile]
           [com.mongodb.util JSON]))

(defvar *keywordize* true
  "Set this to false to prevent ClojureDBObject from setting string keys to keywords")


(defn- dbobject->clojure
  "Not every DBObject returned from Mongo is a ClojureDBObject,
   since we can't setObjectClass on the collections used to back GridFS;
   those collections have GridFSDBFile as their object class.

   This function uses ClojureDBObject to marshal such DBObjects
   into Clojure structures; in practice, this applies only to GridFSFile
   and its subclasses."
  [#^DBObject f keywordize]
  (ClojureDBObject/toClojureMap
   (try (.toMap f)
        ;; DBObject provides .toMap, but the implementation in
        ;; subclass GridFSFile unhelpfully throws
        ;; UnsupportedOperationException
        (catch UnsupportedOperationException e
          (let [keys (.keySet f)]
            (zipmap keys (map #(.get f %) keys)))))
   keywordize))


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
              [:clojure :mongo  ] #(ClojureDBObject. #^IPersistentMap %)
              [:clojure :json   ] #(json-str %)
              [:mongo   :clojure] #(.toClojure #^ClojureDBObject %
                                               #^Boolean/TYPE *keywordize*)
              [:mongo   :json   ] #(.toString #^ClojureDBObject %)
              [:gridfs  :clojure] #(dbobject->map #^GridFSFile %)
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
  (ClojureDBObject. #^IPersistentMap (zipmap fields (repeat 1))))

