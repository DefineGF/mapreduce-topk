package com.ksc.wordcount.task.map;

import com.ksc.wordcount.datasourceapi.split.PartionFile;
import com.ksc.wordcount.datasourceapi.reader.PartionReader;
import com.ksc.wordcount.shuffle.ShuffleWriter;
import com.ksc.wordcount.task.TaskContext;

public class MapTaskContext extends TaskContext {

    PartionFile partionFile;
    PartionReader partionReader; // 读取原始文件
    ShuffleWriter shuffleWriter; // 写入 shuffle
    int reduceTaskNum;
    MapFunction mapFunction;


    public MapTaskContext(String applicationId, String stageId, int taskId, int partionId,
                          PartionFile partionFile, PartionReader partionReader, ShuffleWriter writer,
                          int reduceTaskNum, MapFunction mapFunction) {
        super(applicationId, stageId, taskId, partionId);
        this.partionFile = partionFile;
        this.partionReader = partionReader;
        this.shuffleWriter = writer;
        this.reduceTaskNum = reduceTaskNum;
        this.mapFunction = mapFunction;
    }

    public static class Builder {
        String applicationId;
        String stageId;
        int taskId;
        int partionId;

        PartionFile partionFile;
        PartionReader partionReader;
        ShuffleWriter shuffleWriter;
        int reduceTaskNum;
        MapFunction mapFunction;

        public Builder taskContext(String appId, String stageId, int taskId, int partionId) {
            this.applicationId = appId;
            this.stageId = stageId;
            this.taskId = taskId;
            this.partionId = partionId;
            return this;
        }

        public Builder partionFile(PartionFile file) {
            this.partionFile = file;
            return this;
        }

        public Builder reader(PartionReader reader) {
            this.partionReader = reader;
            return this;
        }
        public Builder writer(ShuffleWriter writer) {
            this.shuffleWriter = writer;
            return this;
        }
        public Builder reduceTaskNum(int num) {
            this.reduceTaskNum = num;
            return this;
        }

        public Builder mapFunc(MapFunction func) {
            this.mapFunction = func;
            return this;
        }

        public MapTaskContext build() {
            return new MapTaskContext(applicationId, stageId, taskId, partionId,
                    partionFile, partionReader, shuffleWriter, reduceTaskNum, mapFunction);
        }

    }

    public PartionFile getPartionFile() {
        return partionFile;
    }

    public PartionReader getPartionReader() {
        return partionReader;
    }

    public int getReduceTaskNum() {
        return reduceTaskNum;
    }

    public MapFunction getMapFunction() {
        return mapFunction;
    }

    public ShuffleWriter getShuffleWriter() {
        return shuffleWriter;
    }
}
