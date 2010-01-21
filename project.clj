(defproject org.clojars.somnium/congomongo
  "0.1.1-SNAPSHOT"
  :description "clojure-friendly api for MongoDB"
  :java-source-path "src/java"
  :javac-fork "true"
  :dependencies [[org.clojure/clojure "1.1.0-master-SNAPSHOT"]
                 [org.clojure/clojure-contrib "1.1.0-master-SNAPSHOT"]
                 [org.clojars.somnium/mongo-java-driver "1.1.0-SNAPSHOT"]]
  :dev-dependencies [[swank-clojure "1.1.0-SNAPSHOT"]
                     [org.clojars.somnium/user "0.1.0-SNAPSHOT"]
                     [lein-javac "0.0.1-SNAPSHOT"]]
  :source-path "src/clj")
