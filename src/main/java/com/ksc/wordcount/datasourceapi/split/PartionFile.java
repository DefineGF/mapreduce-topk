package com.ksc.wordcount.datasourceapi.split;

import java.io.Serializable;
import java.util.Arrays;

public class PartionFile implements Serializable {
    private final int partionId;
    private final FileSplit[] fileSplits;

    public PartionFile(int partionId, FileSplit[] fileSplits) {
        this.partionId = partionId;
        this.fileSplits = fileSplits;
    }

    public int getPartionId() {
        return partionId;
    }

    public FileSplit[] getFileSplits() {
        return fileSplits;
    }

    @Override
    public String toString() {
        return "PartionFile{" +
                "partionId=" + partionId +
                ", fileSplits=" + Arrays.toString(fileSplits) +
                '}';
    }
}
