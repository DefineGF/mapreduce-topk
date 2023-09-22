package com.ksc.wordcount.task.reduce;

import com.ksc.wordcount.conf.AppConfig;
import com.ksc.wordcount.shuffle.DirectShuffleWriter;
import com.ksc.wordcount.shuffle.KryoShuffleWriter;
import com.ksc.wordcount.shuffle.ShuffleBlockId;
import com.ksc.wordcount.shuffle.nettyimpl.client.ShuffleClient;
import com.ksc.wordcount.task.KeyValue;
import com.ksc.wordcount.task.Task;

import java.io.IOException;
import java.util.UUID;
import java.util.stream.Stream;

public class ReduceTask extends Task {
    ShuffleBlockId[] shuffleBlockId;      // 输入数据：需要处理的 map-shuffle 的中间数据
    final ReduceFunction reduceFunction;
    KryoShuffleWriter writer;

    public ReduceTask(ReduceTaskContext reduceTaskContext) {
        super(reduceTaskContext);
        this.shuffleBlockId = reduceTaskContext.getShuffleBlockId();
        this.reduceFunction = reduceTaskContext.getReduceFunction();
        this.writer = (KryoShuffleWriter) reduceTaskContext.getWriter();
    }


    public ReduceStatus runTask() throws InterruptedException, IOException {
        Stream<KeyValue> stream = Stream.empty();
        for(ShuffleBlockId shuffleBlockId : shuffleBlockId) {
            // 利用 ShuffleClient 从文件中获取 stream
            stream = Stream.concat(stream, new ShuffleClient().fetchShuffleData(shuffleBlockId));
        }

        stream = reduceFunction.reduce(stream); // reduce 计算
        // 直接在本地写入 shuffle 中间文件
//        DirectShuffleWriter shuffleWriter = new DirectShuffleWriter(AppConfig.shuffleTempDir, UUID.randomUUID().toString(),
//                applicationId, partionId, 1);
//        shuffleWriter.write(stream);
//        shuffleWriter.commit();
        writer.write(stream);
        return writer.getReduceTaskStatus(taskId);
    }
}
