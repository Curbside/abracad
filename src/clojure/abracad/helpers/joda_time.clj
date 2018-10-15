(ns abracad.helpers.joda-time
  (:require [abracad.avro :as avro])
  (:import [org.joda.time DateTime DateTimeZone LocalDate LocalTime]))

(def ^:const ^:private millis-in-second 1000)

(avro/deflogical LocalDate "date"
  ([_ _ datum]
   (LocalDate. (int (* datum millis-in-second)) DateTimeZone/UTC))
  ([_ _ ^LocalDate datum]
   (-> (.toDateTimeAtStartOfDay datum DateTimeZone/UTC)
       (.getMillis)
       (/ 1000)
       (int))))

(avro/deflogical LocalTime "time-millis"
  ([_ _ datum]
   (LocalTime. (long datum) DateTimeZone/UTC))
  ([_ _ ^LocalTime datum]
   (long (.getMillisOfDay datum))))

(avro/deflogical DateTime "timestamp-millis"
  ([_ _ datum]
   (DateTime. (long datum)))
  ([_ _ ^DateTime datum]
   (.getMillis datum)))
