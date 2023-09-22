package com.ksc.wordcount.task.map;

import com.ksc.wordcount.datasourceapi.TempResult;
import com.ksc.wordcount.datasourceapi.reader.PartionReader;
import com.ksc.wordcount.datasourceapi.split.PartionFile;
import com.ksc.wordcount.shuffle.KryoShuffleWriter;
import com.ksc.wordcount.task.KeyValue;
import com.ksc.wordcount.task.Task;

import java.io.IOException;
import java.util.stream.Stream;

public class ShuffleMapTask extends Task<MapStatus> {

    PartionFile partionFile;
    final PartionReader partionReader;
    int reduceTaskNum;
    MapFunction mapFunction;

    KryoShuffleWriter writer;

    public ShuffleMapTask(MapTaskContext mapTaskContext) {
        super(mapTaskContext);
        this.partionFile = mapTaskContext.getPartionFile();
        this.partionReader = mapTaskContext.getPartionReader();
        this.reduceTaskNum = mapTaskContext.getReduceTaskNum();
        this.mapFunction = mapTaskContext.getMapFunction();
        this.writer = (KryoShuffleWriter) mapTaskContext.getShuffleWriter();
    }


    /**
     * PartionReader 从本地文件中读取原始数据，写入 Stream<String> 流中;
     * 执行 Map 运算后，通过 KryoShuffleWriter 将结果序列化保存到 shuffle 文件中;
     */
    public MapStatus runTask() throws IOException {
        Stream<String> stream = partionReader.toStream(partionFile);
        Stream<KeyValue> kvStream = mapFunction.map(stream);

        // 将task执行结果写入shuffle文件中; 每个 MapTask 会写入 ReduceNum 个文件
//        DirectShuffleWriter shuffleWriter = new DirectShuffleWriter(AppConfig.shuffleTempDir,
//                UUID.randomUUID().toString(), applicationId, partionId, reduceTaskNum);
//        shuffleWriter.write(kvStream);
//        shuffleWriter.commit();
        writer.write(kvStream);
        return writer.getMapTaskStatus(taskId);
    }
}
