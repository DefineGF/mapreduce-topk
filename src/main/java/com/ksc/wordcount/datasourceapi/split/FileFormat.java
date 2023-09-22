package com.ksc.wordcount.datasourceapi.split;


import com.ksc.wordcount.datasourceapi.reader.PartionReader;
import com.ksc.wordcount.datasourceapi.writer.PartionWriter;

public interface FileFormat {

    boolean isSplitable(String filePath);

    PartionFile[] getSplits(String filePath, long size);

    PartionReader createReader();

    PartionWriter createWriter(String destPath, int partionId);
}
