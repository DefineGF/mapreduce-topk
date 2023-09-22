package com.ksc.wordcount.shuffle;

import akka.stream.impl.fusing.Reduce;
import com.ksc.wordcount.task.KeyValue;
import com.ksc.wordcount.task.TaskStatus;
import com.ksc.wordcount.task.TaskStatusEnum;
import com.ksc.wordcount.task.map.MapStatus;
import com.ksc.wordcount.task.reduce.ReduceStatus;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Iterator;
import java.util.stream.Stream;

public class DirectShuffleWriter implements ShuffleWriter<KeyValue> {

    String baseDir;

    int reduceTaskNum;

    ObjectOutputStream[] fileWriters;

    ShuffleBlockId[] shuffleBlockIds; // 数据源

    /**
     * @param baseDir: shuffle 的根目录
     * @param shuffleId: UUID 生成的全局唯一字符串
     * @param applicationId: application 标记
     * @param partionId: 一个文件有唯一 partionId, 用于标记同一文件的不同分片
     * @param reduceTaskNum: reduceTaskNum 个数, 用于写入 reduceTaskNum 个文件
     */
    public DirectShuffleWriter(String baseDir, String shuffleId, String applicationId, int partionId, int reduceTaskNum) {
        this.baseDir = baseDir;
        this.reduceTaskNum = reduceTaskNum;
        fileWriters = new ObjectOutputStream[reduceTaskNum];
        shuffleBlockIds = new ShuffleBlockId[reduceTaskNum];

        for (int i = 0; i < reduceTaskNum; i++) {
            try {
                shuffleBlockIds[i] = new ShuffleBlockId(baseDir, applicationId, shuffleId, partionId, i);
                new File(shuffleBlockIds[i].getShuffleParentPath()).mkdirs();
                fileWriters[i] = new ObjectOutputStream(new FileOutputStream(shuffleBlockIds[i].getShufflePath(".data")));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //todo 学生实现 将maptask的处理结果写入shuffle文件中 - finish
    @Override
    public void write(Stream<KeyValue> entryStream) throws IOException {
        Iterator<KeyValue> iterator = entryStream.iterator();
        while (iterator.hasNext()) {
            KeyValue next = iterator.next();
            int index = Math.abs(next.getKey().hashCode()) % reduceTaskNum;
            fileWriters[index].writeObject(next);
        }
    }

    @Override
    public void commit() {
        for (int i = 0; i < reduceTaskNum; i++) {
            try {
                fileWriters[i].close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public MapStatus getMapTaskStatus(int mapTaskId) {
        return new MapStatus(mapTaskId, TaskStatusEnum.FINISHED, shuffleBlockIds);
    }

    public ReduceStatus getReduceTaskStatus(int mapTaskId) {
        return new ReduceStatus(mapTaskId, TaskStatusEnum.FINISHED, shuffleBlockIds);
    }
}
