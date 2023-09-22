package com.ksc.wordcount.datasourceapi.writer;

import com.ksc.wordcount.task.KeyValue;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

public class TextPartionWriter implements PartionWriter<KeyValue>, Serializable {

    private final String destDest;
    private final int partionId;

    public TextPartionWriter(String destDest, int partionId) {
        this.destDest = destDest;
        this.partionId = partionId;
    }

    //把partionId 前面补0，补成length位
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
        File file = new File(destDest + File.separator + "part_" + padLeft(partionId, 3) + ".txt");

        try (FileOutputStream fos = new FileOutputStream(file)) {
            stream.forEach(keyValue -> {
                byte[] bytes = (keyValue.getKey() + "\t" + keyValue.getValue() + "\n").getBytes(StandardCharsets.UTF_8);
                try {
                    fos.write(bytes);
                } catch (IOException e) {
                    System.err.println("写入失败: " + e.getMessage());
                    e.printStackTrace();
                }
            });
        }
    }
}
