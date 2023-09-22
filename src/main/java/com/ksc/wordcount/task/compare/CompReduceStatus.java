package com.ksc.wordcount.task.compare;

import com.ksc.wordcount.shuffle.ShuffleBlockId;
import com.ksc.wordcount.task.TaskStatus;
import com.ksc.wordcount.task.TaskStatusEnum;

public class CompReduceStatus extends TaskStatus {
    private String resultFilePath;


    public CompReduceStatus(int taskId, TaskStatusEnum taskStatus) {
        super(taskId, taskStatus);
    }

    public CompReduceStatus(int taskId, TaskStatusEnum taskStatus, ShuffleBlockId[] result) {
        this(taskId, taskStatus);
        this.shuffleBlockIds = result;
    }

    public String getResultFilePath() {
        return resultFilePath;
    }

    public void setResultFilePath(String resultFilePath) {
        this.resultFilePath = resultFilePath;
    }
}
