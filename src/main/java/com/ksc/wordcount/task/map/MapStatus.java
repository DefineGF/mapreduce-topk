package com.ksc.wordcount.task.map;

import com.ksc.wordcount.shuffle.ShuffleBlockId;
import com.ksc.wordcount.task.TaskStatus;
import com.ksc.wordcount.task.TaskStatusEnum;

public class MapStatus extends TaskStatus {

    public MapStatus(int taskId, ShuffleBlockId[] shuffleBlockIds) {
        super(taskId, TaskStatusEnum.FINISHED);
        this.shuffleBlockIds = shuffleBlockIds; // 本阶段共产生 length = reduceTaskNum
    }

    public MapStatus(int taskId, TaskStatusEnum taskStatus, ShuffleBlockId[] shuffleBlockIds) {
        super(taskId,taskStatus);
        this.shuffleBlockIds = shuffleBlockIds;
    }

    public MapStatus(int taskId,TaskStatusEnum taskStatus, String errorMsg, String errorStackTrace) {
        super(taskId,taskStatus, errorMsg, errorStackTrace);
    }

    public ShuffleBlockId[] getShuffleBlockIds() {
        return shuffleBlockIds;
    }


}
