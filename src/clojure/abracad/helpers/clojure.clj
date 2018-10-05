(ns abracad.helpers.clojure
  (:require [abracad.avro :as avro])
  (:import (clojure.lang Keyword Symbol)
           (java.nio ByteBuffer)
           (java.util UUID)))

(avro/deflogical Keyword "keyword"
  ([_ _ ^String datum]
   (keyword datum))
  ([_ _ ^Keyword datum]
   (str (.sym datum))))

(avro/deflogical Symbol "symbol"
  ([_ _ ^String datum]
   (symbol datum))
  ([_ _ ^Symbol datum]
   (str datum)))

(avro/deflogical UUID "uuid"
  ([_ _ ^bytes datum]
   (let [b (ByteBuffer/wrap datum)]
     (UUID. (.getLong b) (.getLong b))))
  ([_ _ ^UUID datum]
   (let [b (ByteBuffer/allocate 16)]
     (.putLong b (.getMostSignificantBits datum))
     (.putLong b (.getLeastSignificantBits datum))
     (.array b))))
