package com.ksc.wordcount.datasourceapi.writer;

import org.apache.avro.Schema;

public class AvroSchema {
    public static final String SCHEMA_JSON = "{\"type\":\"record\",\"name\":\"KeyValue\",\"fields\":[{\"name\":\"key\",\"type\":\"string\"},{\"name\":\"value\",\"type\":\"string\"}]}";

    public static Schema getSchema() {
        return new Schema.Parser().parse(SCHEMA_JSON);
    }
}
