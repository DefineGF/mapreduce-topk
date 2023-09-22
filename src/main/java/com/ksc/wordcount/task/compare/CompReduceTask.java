package com.ksc.wordcount.task.compare;

import com.ksc.wordcount.datasourceapi.writer.AvroPartionWriter;
import com.ksc.wordcount.datasourceapi.writer.PartionWriter;
import com.ksc.wordcount.shuffle.ShuffleBlockId;
import com.ksc.wordcount.shuffle.nettyimpl.client.ShuffleClient;
import com.ksc.wordcount.task.KeyValue;
import com.ksc.wordcount.task.Task;
import com.ksc.wordcount.task.TaskStatus;
import com.ksc.wordcount.task.TaskStatusEnum;

import java.util.stream.Stream;

public class CompReduceTask extends Task<CompReduceStatus> {
    private final ShuffleBlockId[] inputShuffleBlockIds;
    private final CompReduceFunction compReduceFunc;
    PartionWriter partionWriter;

    private final int topN;


    public CompReduceTask(CompareTaskContext taskContext) {
        super(taskContext);
        this.inputShuffleBlockIds = taskContext.getShuffleBlockIds();
        this.compReduceFunc = taskContext.getReduceFunction();
        this.partionWriter = taskContext.getPartionWriter();
        this.topN = taskContext.getTopN();
    }

    @Override
    public TaskStatus runTask() throws Exception {
        Stream<KeyValue> stream = Stream.empty();
        for (ShuffleBlockId shuffleBlockId : inputShuffleBlockIds) {
            stream = Stream.concat(stream, new ShuffleClient().fetchShuffleData(shuffleBlockId));
        }
        stream = compReduceFunc.reduce(stream, topN); // reduce 计算
        partionWriter.write(stream);

        CompReduceStatus status = new CompReduceStatus(taskId, TaskStatusEnum.FINISHED);
        if (partionWriter instanceof AvroPartionWriter) {
            status.setResultFilePath(((AvroPartionWriter)partionWriter).getResultFilePath());
        }
        return status;
    }
}
