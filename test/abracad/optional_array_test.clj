(ns abracad.optional-array-test
  (:require
    [abracad.avro :as avro]
    [clojure.test :refer [are deftest is testing]]))

(defn- encode-decode [schema-parsed data]
  (->> data
       (avro/binary-encoded schema-parsed)
       (avro/decode schema-parsed)))

(deftest optional-array-nilable-item-test
  (testing "Given a field is optional array of an optional data type, nil value is accepted like the following"
    (let [schema-parsed (avro/parse-schema {:name "ad-opt-array"
                                            :namespace "ad"
                                            :type "record"
                                            :fields [{:name "opt-array" :type ["null" {:type "array" :items ["null" "long"]}]}]})]
      (are [result arg] (= result (encode-decode schema-parsed arg))
                        {:opt-array nil} {}
                        {:opt-array nil} {:opt-array nil}
                        {:opt-array []} {:opt-array []}
                        {:opt-array [nil Long/MIN_VALUE nil Long/MAX_VALUE]} {:opt-array [nil Long/MIN_VALUE nil Long/MAX_VALUE]}
                        {:opt-array [Long/MIN_VALUE nil Long/MAX_VALUE]} {:opt-array [Long/MIN_VALUE nil Long/MAX_VALUE]}))))

(deftest optional-array-nilable-item-boolean-test
  (testing "Given a field is an optional array of the optional boolean data type, nil value is accepted like the following"
    (let [schema-parsed (avro/parse-schema {:name "ad-opt-array"
                                            :namespace "ad"
                                            :type "record"
                                            :fields [{:name "opt-array" :type ["null" {:type "array" :items ["null" "boolean"]}]}]})]
      (are [result arg] (= result (encode-decode schema-parsed arg))
                        {:opt-array nil} {}
                        {:opt-array nil} {:opt-array nil}
                        {:opt-array []} {:opt-array []}
                        {:opt-array [nil false nil true]} {:opt-array [nil false nil true]}
                        {:opt-array [false nil true]} {:opt-array [false nil true]})))

  (testing "Given a field is an optional array of the boolean data type, nil value is accepted like the following"
    (let [schema-parsed (avro/parse-schema {:name "ad-opt-array"
                                            :namespace "ad"
                                            :type "record"
                                            :fields [{:name "opt-array" :type ["null" {:type "array" :items "boolean"}]}]})]
      (are [result arg] (= result (encode-decode schema-parsed arg))
                        {:opt-array nil} {}
                        {:opt-array nil} {:opt-array nil}
                        {:opt-array []} {:opt-array []}
                        {:opt-array [false false false true]} {:opt-array [nil false nil true]}
                        {:opt-array [false false true]} {:opt-array [false nil true]})))

  (testing "Given a field is an array of the boolean data type, nil value is accepted like the following"
    (let [schema-parsed (avro/parse-schema {:name "ad-opt-array"
                                            :namespace "ad"
                                            :type "record"
                                            :fields [{:name "opt-array" :type {:type "array" :items "boolean"}}]})]
      (are [result arg] (= result (encode-decode schema-parsed arg))
                        {:opt-array []} {}
                        {:opt-array []} {:opt-array nil}
                        {:opt-array []} {:opt-array []}
                        {:opt-array [false false false true]} {:opt-array [nil false nil true]}
                        {:opt-array [false false true]} {:opt-array [false nil true]})))

  (testing "Given a field is an array of the optional boolean data type, nil value is accepted like the following"
    (let [schema-parsed (avro/parse-schema {:name "ad-opt-array"
                                            :namespace "ad"
                                            :type "record"
                                            :fields [{:name "opt-array" :type {:type "array" :items ["null" "boolean"]}}]})]
      (are [result arg] (= result (encode-decode schema-parsed arg))
                        {:opt-array []} {}
                        {:opt-array []} {:opt-array nil}
                        {:opt-array []} {:opt-array []}
                        {:opt-array [nil false nil true]} {:opt-array [nil false nil true]}
                        {:opt-array [false nil true]} {:opt-array [false nil true]}))))
