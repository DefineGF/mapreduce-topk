package com.ksc.wordcount.task;

import com.ksc.wordcount.shuffle.ShuffleBlockId;

public class TaskStatus implements java.io.Serializable{
    protected int taskId;

    protected TaskStatusEnum taskStatus ;
    protected String errorMsg;
    protected String errorStackTrace;

    protected ShuffleBlockId[] shuffleBlockIds; // 用于记录本阶段的结果

    public TaskStatus(int taskId, TaskStatusEnum taskStatus) {
        this.taskStatus = taskStatus;
        this.taskId = taskId;
    }

    public TaskStatus(int taskId, TaskStatusEnum taskStatus, ShuffleBlockId[] shuffleBlockIds) {
        this(taskId, taskStatus);
        this.shuffleBlockIds = shuffleBlockIds;
    }

    public TaskStatus(int taskId, TaskStatusEnum taskStatus, String errorMsg, String errorStackTrace) {
        this.taskId = taskId;
        this.taskStatus = taskStatus;
        this.errorMsg = errorMsg;
        this.errorStackTrace = errorStackTrace;
    }

    public int getTaskId() {
        return taskId;
    }

    public TaskStatusEnum getTaskStatus() {
        return taskStatus;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public String getErrorStackTrace() {
        return errorStackTrace;
    }

    public void setShuffleBlockHostAndPort(String host,int port){
        if (shuffleBlockIds != null) {
            for(ShuffleBlockId shuffleBlockId : shuffleBlockIds){
                shuffleBlockId.setHostAndPort(host,port);
            }
        }
    }

    @Override
    public String toString() {
        return "TaskStatus{" +
                "taskId=" + taskId +
                ", taskStatus=" + taskStatus +
                '}';
    }
}
