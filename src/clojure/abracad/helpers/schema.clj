(ns abracad.helpers.schema
  (:refer-clojure :exclude [keyword symbol time])
  (:require [abracad.avro :as avro]
            [cheshire.generate :as json-gen])
  (:import (com.fasterxml.jackson.core JsonGenerator)))

(deftype LogicalTypeWrapper [logical-type]
  Object
  (toString [_] logical-type))

(defn- encode-logical-type [^LogicalTypeWrapper t ^JsonGenerator gen]
  (.writeString gen (.toString t)))

(json-gen/add-encoder LogicalTypeWrapper encode-logical-type)

(def keyword (avro/parse-schema {:type :string
                                 :logicalType :keyword}))

(def symbol (avro/parse-schema {:type :string
                                :logicalType :symbol}))

(def uuid (avro/parse-schema {:type :fixed
                              :size 16
                              :name :UUID
                              :logicalType :uuid}))

(def date (avro/parse-schema {:type :int
                              :logicalType :date}))

(def time (avro/parse-schema {:type :long
                              :logicalType (LogicalTypeWrapper. "time-millis")}))

(def timestamp (avro/parse-schema {:type :long
                                   :logicalType (LogicalTypeWrapper. "timestamp-millis")}))
