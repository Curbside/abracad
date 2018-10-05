(ns abracad.logical-test
  (:require [abracad.avro :as avro]
            [abracad.helpers.clojure]
            [abracad.helpers.schema :as schema]
            [midje.sweet :refer :all])
  (:import (java.util UUID)))

(defn- encode-decode [schema x]
  (->> (avro/binary-encoded schema x)
       (avro/decode-seq schema)
       (first)))

(fact "we can encode/decode unions using logical types"
      (let [schema (avro/parse-schema [:null schema/uuid])
            uuid (UUID/randomUUID)]
        (encode-decode schema uuid) => uuid))

(fact "we can encode/decode records using logical types"
      (let [schema (avro/parse-schema {:type :record
                                       :name :test
                                       :fields [{:name :uuid :type schema/uuid}
                                                {:name :key :type schema/keyword}
                                                {:name :sym :type schema/symbol}]})
            record {:uuid (UUID/randomUUID)
                    :key :hello/world
                    :sym 'hello/world}]
        (encode-decode schema record) => record))
