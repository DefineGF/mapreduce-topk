package com.ksc.wordcount.datasourceapi.writer;

import com.ksc.wordcount.datasourceapi.TempResult;
import com.ksc.wordcount.task.KeyValue;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.stream.Stream;

public class AvroPartionWriter implements PartionWriter<KeyValue>, Serializable {
    private final Logger log = LoggerFactory.getLogger(AvroPartionWriter.class);

    private final String destDir;
    private final int partId;

    private String resultFilePath = null;

    public AvroPartionWriter(String destDir, int partId) {
        this.destDir = destDir;
        this.partId = partId;
    }

    public String padLeft(int partionId, int length) {
        StringBuilder partionIdStr = new StringBuilder(String.valueOf(partionId));
        int len = partionIdStr.length();
        if (len < length) {
            for (int i = 0; i < length - len; i++) {
                partionIdStr.insert(0, "0");
            }
        }
        return partionIdStr.toString();
    }

    @Override
    public void write(Stream<KeyValue> stream) throws IOException {
        String fileName = destDir + File.separator + "part_" + padLeft(partId, 3);
        try {
            writeToAvroFile(stream, fileName);
        } catch (IOException e) {
            log.error("avro 文件写入失败: {}", e.getMessage(), e);
        }
    }

    public static Schema getKeyValueSchema() {
        String schemaString = "{\"type\":\"record\",\"name\":\"KeyValue\",\"fields\":[" +
                "{\"name\":\"key\",\"type\":\"string\"}," +
                "{\"name\":\"value\",\"type\":\"int\"}]}";
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(schemaString);
    }

    private void writeToAvroFile(Stream<KeyValue> stream, String filePath) throws IOException {
        File file = new File( filePath + ".avro");
        resultFilePath = file.getAbsolutePath();

        Schema schema = getKeyValueSchema();
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(schema, file);
        stream.forEach(kv -> {
            GenericRecord record = new GenericData.Record(schema);
            record.put("key", new Utf8(kv.getKey().toString()));
            record.put("value", kv.getValue());
            try {
                dataFileWriter.append(record);
            } catch (IOException e) {
                log.error("avro 写入失败: {}", e.getMessage(), e);
            }
        });
        dataFileWriter.close();
    }

    public String getResultFilePath() {
        return resultFilePath;
    }
}
