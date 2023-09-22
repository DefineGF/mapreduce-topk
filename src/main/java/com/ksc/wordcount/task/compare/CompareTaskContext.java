package com.ksc.wordcount.task.compare;

import com.ksc.wordcount.datasourceapi.writer.PartionWriter;
import com.ksc.wordcount.shuffle.ShuffleBlockId;
import com.ksc.wordcount.task.TaskContext;

/**
 * 网络流中读取数据 -> reduce 处理 -> 写入文件 -> 返回状态
 */
public class CompareTaskContext extends TaskContext {
    private final ShuffleBlockId[] shuffleBlockIds; // 用来读取数据的文件信息
    CompReduceFunction<String, Integer> compReduceFunc;
    PartionWriter partionWriter;
    int topN;

    public CompareTaskContext(String appId, String stageId, int taskId, int partionId, ShuffleBlockId[] shuffleBlockIds,
                              CompReduceFunction<String,Integer> compReduceFunc, PartionWriter writer, int topN) {
        super(appId, stageId, taskId, partionId);
        this.shuffleBlockIds = shuffleBlockIds;
        this.partionWriter = writer;
        this.compReduceFunc = compReduceFunc;
        this.topN = topN;
    }

    public static class Builder {
        String appId, stageId;
        int taskId, partionId;
        ShuffleBlockId[] shuffleBlockIds;
        CompReduceFunction<String,Integer> compReduceFunc;
        PartionWriter writer;
        int topN;

        public Builder taskContext(String appId, String stageId, int taskId, int partionId) {
            this.appId = appId;
            this.stageId = stageId;
            this.taskId = taskId;
            this.partionId = partionId;
            return this;
        }

        public Builder shuffleBlockIds(ShuffleBlockId[] ids) {
            this.shuffleBlockIds = ids;
            return this;
        }

        public Builder writer(PartionWriter writer) {
            this.writer = writer;
            return this;
        }

        public Builder comReduceFunc(CompReduceFunction<String, Integer> func) {
            this.compReduceFunc = func;
            return this;
        }

        public Builder topN(int topN) {
            this.topN = topN;
            return this;
        }

        public CompareTaskContext build() {
            return new CompareTaskContext(appId, stageId, taskId, partionId, shuffleBlockIds, compReduceFunc, writer, topN);
        }
    }

    public ShuffleBlockId[] getShuffleBlockIds() {
        return shuffleBlockIds;
    }

    public PartionWriter getPartionWriter() {
        return partionWriter;
    }

    public CompReduceFunction<String,Integer> getReduceFunction() {
        return compReduceFunc;
    }

    public int getTopN() {return topN;}
}
