package com.ksc.wordcount.task.reduce;

import com.ksc.wordcount.shuffle.ShuffleBlockId;
import com.ksc.wordcount.shuffle.ShuffleWriter;
import com.ksc.wordcount.task.TaskContext;

public class ReduceTaskContext extends TaskContext {

    private final ShuffleBlockId[] shuffleBlockId; // 用来读取数据的文件信息
    //String destDir;
    ReduceFunction reduceFunction;

    private final ShuffleWriter writer;


    public ReduceTaskContext(String applicationId, String stageId, int taskId, int partionId, ShuffleWriter writer,
                             ShuffleBlockId[] shuffleBlockId, ReduceFunction reduceFunction) {
        super(applicationId, stageId, taskId, partionId);
        this.writer = writer;
        this.shuffleBlockId = shuffleBlockId;
        this.reduceFunction = reduceFunction;
    }

    /**
     * builder模式
     */
    public static class Builder {
        String applicationId;
        String stageId;
        int taskId;
        int partionId;
        ShuffleWriter writer;
        ShuffleBlockId[] shuffleBlockIds;
        ReduceFunction reduceFunction;

        public Builder taskContext(String appId, String stageId, int taskId, int partionId) {
            this.applicationId = appId;
            this.stageId = stageId;
            this.taskId = taskId;
            this.partionId = partionId;
            return this;
        }

        public Builder shuffleBlockIds(ShuffleBlockId[] blockIds) {
            this.shuffleBlockIds = blockIds;
            return this;
        }

        public Builder reduceFunc(ReduceFunction func) {
            this.reduceFunction = func;
            return this;
        }

        public Builder writer(ShuffleWriter writer) {
            this.writer = writer;
            return this;
        }

        public ReduceTaskContext build() {
            return new ReduceTaskContext(applicationId, stageId, taskId, partionId,
                    writer, shuffleBlockIds, reduceFunction);
        }
    }

    public ShuffleBlockId[] getShuffleBlockId() {
        return shuffleBlockId;
    }

    public ReduceFunction getReduceFunction() {
        return reduceFunction;
    }

    public ShuffleWriter getWriter() {
        return writer;
    }
}
