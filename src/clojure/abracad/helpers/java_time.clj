(ns abracad.helpers.java-time
  (:require [abracad.avro :as avro])
  (:import (java.time Instant LocalDate LocalTime)))

(def ^:const ^:private nanos-in-milli 1000000)

(avro/deflogical LocalDate "date"
  ([_ _ datum]
   (LocalDate/ofEpochDay (int datum)))
  ([_ _ ^LocalDate datum]
   (.toEpochDay datum)))

(avro/deflogical LocalTime "time-millis"
  ([_ _ datum]
   (LocalTime/ofNanoOfDay (long (* datum nanos-in-milli))))
  ([_ _ ^LocalTime datum]
   (/ (.toNanoOfDay datum) nanos-in-milli)))

(avro/deflogical Instant "timestamp-millis"
  ([_ _ datum]
   (Instant/ofEpochMilli (long datum)))
  ([_ _ ^Instant datum]
   (.toEpochMilli datum)))
