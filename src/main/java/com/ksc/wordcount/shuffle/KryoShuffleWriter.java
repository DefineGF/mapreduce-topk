package com.ksc.wordcount.shuffle;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.ksc.wordcount.conf.AppConfig;
import com.ksc.wordcount.task.KeyValue;
import com.ksc.wordcount.task.TaskStatusEnum;
import com.ksc.wordcount.task.map.MapStatus;
import com.ksc.wordcount.task.reduce.ReduceStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.UUID;
import java.util.stream.Stream;

public class KryoShuffleWriter implements ShuffleWriter<KeyValue>, Serializable {
    private final static Logger logger = LoggerFactory.getLogger(KryoShuffleWriter.class);
    String baseDir;
    String shuffleId;
    String appId;
    int stageId;

    int reduceTaskNum;

    ShuffleBlockId[] shuffleBlockIds; // 数据 dest

    public KryoShuffleWriter(String appId, int stageId, int reduceTaskNum) {
        this(AppConfig.shuffleTempDir, UUID.randomUUID().toString(), appId, stageId, reduceTaskNum);
    }

    public KryoShuffleWriter(String baseDir, String shuffleId, String appId, int stageId, int reduceTaskNum) {
        this.baseDir = baseDir;
        this.shuffleId = shuffleId;
        this.appId = appId;
        this.stageId = stageId;
        this.reduceTaskNum = reduceTaskNum;
    }

    @Override
    public void write(Stream<KeyValue> stream) throws IOException {
        Kryo kryo = new Kryo();
        kryo.register(KeyValue.class);

        Output[] kryOutputs = new Output[reduceTaskNum];
        shuffleBlockIds = new ShuffleBlockId[reduceTaskNum];
        for (int i = 0; i < reduceTaskNum; i++) {
            try {
                shuffleBlockIds[i] = new ShuffleBlockId(baseDir, appId, shuffleId, stageId, i);
                new File(shuffleBlockIds[i].getShuffleParentPath()).mkdirs();
                kryOutputs[i] = new Output(new FileOutputStream(shuffleBlockIds[i].getShufflePath(".kryo")));
            } catch (IOException e) {
                logger.error("创建 shuffle 文件失败: {}", e.getMessage(), e);
            }
        }

        Iterator<KeyValue> iterator = stream.iterator();
        while (iterator.hasNext()) {
            KeyValue next = iterator.next();
            int index = Math.abs(next.getKey().hashCode()) % reduceTaskNum;
            kryo.writeObject(kryOutputs[index], next);
        }
        for (Output output : kryOutputs) {
            output.close();
        }
    }

    @Override
    public void commit() {
    }

    // map 阶段完成时调用
    public MapStatus getMapTaskStatus(int taskId) {
        return new MapStatus(taskId, TaskStatusEnum.FINISHED, shuffleBlockIds);
    }

    // 第一 reduce 阶段完成时调用
    public ReduceStatus getReduceTaskStatus(int taskId) {
        return new ReduceStatus(taskId, TaskStatusEnum.FINISHED, shuffleBlockIds);
    }
}
