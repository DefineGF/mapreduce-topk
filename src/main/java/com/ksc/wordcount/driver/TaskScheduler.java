package com.ksc.wordcount.driver;

import com.ksc.wordcount.rpc.Driver.DriverRpc;
import com.ksc.wordcount.task.TaskContext;
import com.ksc.wordcount.task.TaskStatus;
import com.ksc.wordcount.task.TaskStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class TaskScheduler {
    private final Logger log = LoggerFactory.getLogger(TaskScheduler.class);

    private final TaskManager taskManager;
    private final ExecutorManager executorManager;

    // taskId 与 ExecutorUrl 的映射
    private final Map<Integer, String> taskIdToExecURLMap = Collections.synchronizedMap(new HashMap<>());

    public TaskScheduler(TaskManager taskManager, ExecutorManager executorManager) {
        this.taskManager = taskManager;
        this.executorManager = executorManager;
    }

    public void clear() {
        taskIdToExecURLMap.clear();
    }

    public void logInfo() {
        System.out.println("########################### task -> url #################################");
        for (Map.Entry<Integer, String> entry : taskIdToExecURLMap.entrySet()) {
            System.out.println("taskId: " + entry.getKey() + " : " + entry.getValue());
        }
        executorManager.logInfo();
    }

    public void submitTask(int stageId) {
        BlockingQueue<TaskContext> taskQueue = taskManager.getBlockingQueue(stageId);
        while (!taskQueue.isEmpty()) {
            executorManager.getExecutorAvailableCoresMap().forEach((executorUrl, availCores) -> {
                // 搜索可用的 executor
                if (availCores > 0 && !taskQueue.isEmpty()) {
                    TaskContext taskContext = taskQueue.poll(); // 取出任务
                    taskIdToExecURLMap.put(taskContext.getTaskId(), executorUrl);
                    executorManager.updateExecutorCores(executorUrl, -1);
                    DriverRpc.submit(executorUrl, taskContext);
                }
            });

            try {
                Map<String, Integer> map = executorManager.getExecutorAvailableCoresMap();
                StringBuilder sb = new StringBuilder();
                map.forEach((key, value) -> {
                    sb.append("key: ").append(key).append(",value = ").append(value).append("\n");
                });
                log.info("stageId: {}, queue_size: {}, AvailableCoresMap: {}; sleep 1000", stageId, taskQueue.size(), sb.toString());

                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void waitStageFinish(int stageId) {
        StageStatusEnum stageStatusEnum = taskManager.getStageTaskStatus(stageId);
        while (stageStatusEnum == StageStatusEnum.RUNNING) {
            try {
                System.out.println("TaskScheduler waitStageFinish stageId:" + stageId + ",sleep 1000");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            stageStatusEnum = taskManager.getStageTaskStatus(stageId);
        }
        if (stageStatusEnum == StageStatusEnum.FAILED) {
            System.err.println("stageId:" + stageId + " failed");
            System.exit(1);
        }
    }

    public void updateTaskStatus(TaskStatus taskStatus) {
        if (taskStatus.getTaskStatus().equals(TaskStatusEnum.FINISHED) ||
                taskStatus.getTaskStatus().equals(TaskStatusEnum.FAILED)) {

            String executorUrl = taskIdToExecURLMap.get(taskStatus.getTaskId());
            executorManager.updateExecutorCores(executorUrl, 1);
        }
    }
}
