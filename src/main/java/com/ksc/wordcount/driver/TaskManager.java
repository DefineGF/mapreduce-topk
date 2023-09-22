package com.ksc.wordcount.driver;

import com.ksc.wordcount.shuffle.ShuffleBlockId;
import com.ksc.wordcount.task.TaskStatusEnum;
import com.ksc.wordcount.task.compare.CompReduceStatus;
import com.ksc.wordcount.task.map.MapStatus;
import com.ksc.wordcount.task.TaskContext;
import com.ksc.wordcount.task.TaskStatus;
import com.ksc.wordcount.task.reduce.ReduceStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class TaskManager {
    private final static Logger log = LoggerFactory.getLogger(TaskManager.class);

    // stageId 与 task(map 或 reduce) 之间映射;
    private final Map<Integer,BlockingQueue<TaskContext>> stageIdToBlockingQueueMap = Collections.synchronizedMap(new HashMap<>());

    // stageId 与 taskId 映射： 同一 stageId 有多个 mapTask 或者 reduceTask
    private final Map<Integer, List<Integer>> stageToTaskIds = Collections.synchronizedMap(new HashMap<>());

    // taskId 和 task 状态的映射
    private final Map<Integer, TaskStatus> taskIdToStatusMap = Collections.synchronizedMap(new HashMap<>());

    public void clear() {
        stageIdToBlockingQueueMap.clear();
        stageToTaskIds.clear();
        taskIdToStatusMap.clear();
    }



    public BlockingQueue<TaskContext> getBlockingQueue(int stageId) {
        return stageIdToBlockingQueueMap.get(stageId);
    }

    public void registerBlockingQueue(int stageId, BlockingQueue<TaskContext> blockingQueue) {
        stageIdToBlockingQueueMap.put(stageId, blockingQueue);
    }

    /**
     * - 添加 stateId 与 taskQueue 关系
     * - 添加 stateId 与 taskIdList 关系
     */
    public void addTaskContext(int stageId, TaskContext taskContext) {
        stageIdToBlockingQueueMap.get(stageId).offer(taskContext);

        //建立stageId和任务id的映射
        List<Integer> taskIdLs = stageToTaskIds.computeIfAbsent(stageId, k -> new ArrayList<>());
        taskIdLs.add(taskContext.getTaskId());
    }


    /**
     * 获取当前阶段的运行状态：
     * 1. 首先获取之前阶段的状态：如果失败 或者 正在运行，则直接返回（因为按照顺序，前一阶段正在运行或者失败时，当前阶段不可能完成）
     * 2. 前面所有阶段都finish，则判断当前阶段状态：
     * - 某个task 为 RUNNING 或者 FAILED，则当前 stage确定；
     * - 所有task 为 FAILED，则当前 stage 为 FAILED；
     */
    public StageStatusEnum getStageTaskStatus(int stageId){
        // 首先判断之前阶段状态
        for (int i = WordCountDriver.mapStageId; i < stageId; ++i) {
            StageStatusEnum curStageStatus = getCurStageTaskStatus(i);
            if (curStageStatus == StageStatusEnum.FAILED || curStageStatus == StageStatusEnum.RUNNING) {
                return curStageStatus;
            }
        }
        log.info("之前所有的任务都finish");
        // 之前所有阶段都 finished
        return getCurStageTaskStatus(stageId);
    }

    private StageStatusEnum getCurStageTaskStatus(int stageId) {
        List<Integer> taskIds = stageToTaskIds.get(stageId);
        if (taskIds == null) {
            log.warn("当前阶段 {} 未注册", stageId);
            return StageStatusEnum.RUNNING;
        }

        for (Integer taskId : taskIds) {
            TaskStatus taskStatus = taskIdToStatusMap.get(taskId);
            if (taskStatus == null) {
                // 暂时没有该任务的状态信息
                return StageStatusEnum.RUNNING;
            }

            TaskStatusEnum status = taskStatus.getTaskStatus();
            if (status == TaskStatusEnum.FAILED) {
                return StageStatusEnum.FAILED;
            }
            if (status == TaskStatusEnum.RUNNING) {
                return StageStatusEnum.RUNNING;
            }
        }
        // 所有task都是finished状态
        return StageStatusEnum.FINISHED;
    }

    /**
     * 根据 MapStateId 获取所有 对应的 MapTask，根据 MapTask id  获取各自对应的 MapStatus;
     * 从 MapStatus 获取 ShuffleBlockId[]: 由于每个 MapTask 对应 ReduceNum 个 ShuffleBlockId
     * 因此需要根据 ReduceId 获取每个 MapStatus 中对应索引的 ShuffleBlockId
     */
    public ShuffleBlockId[] getShuffleIdByMapId(int stageId, int reduceId){
        List<ShuffleBlockId> shuffleBlockIds = new ArrayList<>();
        for(int taskId : stageToTaskIds.get(stageId)){
            ShuffleBlockId shuffleBlockId = ((MapStatus) taskIdToStatusMap.get(taskId)).getShuffleBlockIds()[reduceId];
            shuffleBlockIds.add(shuffleBlockId);
        }
        return shuffleBlockIds.toArray(new ShuffleBlockId[0]);
    }

    public ShuffleBlockId[] getShuffleIdsByReduceId(int stageId) {
        List<ShuffleBlockId> shuffleBlockIds = new ArrayList<>();
        for (int taskId : stageToTaskIds.get(stageId)) {
            // 本项目共启动 ReduceNum 个 ReduceTask， 每个 ReduceTask
            ShuffleBlockId shuffleBlockId = ((ReduceStatus) taskIdToStatusMap.get(taskId)).getShuffleBlockIds()[0];
            shuffleBlockIds.add(shuffleBlockId);
        }
        return shuffleBlockIds.toArray(new ShuffleBlockId[0]);
    }

    public String getFinalFilePath() {
        int stageId = WordCountDriver.compareStageId;
        List<Integer> taskIds = stageToTaskIds.get(stageId);
        if (taskIds != null && taskIds.size() > 0) {
            int taskId = taskIds.get(0);
            TaskStatus status = taskIdToStatusMap.get(taskId);
            if (status instanceof CompReduceStatus) {
                return ((CompReduceStatus) status).getResultFilePath();
            }
        }
        return null;
    }

    public void updateTaskStatus(TaskStatus taskStatus) {
        log.info("更新任务 {} 的状态 {}", taskStatus.getTaskId(), taskStatus.getTaskStatus());
        taskIdToStatusMap.put(taskStatus.getTaskId(), taskStatus);
    }


    private AtomicInteger taskId = new AtomicInteger(0);

    public int generateTaskId() {
        return taskId.getAndAdd(1);
    }

    public void logStatusInfo() {
        System.out.println("###################################### stage - tasks ############################################");
        for (Map.Entry<Integer, List<Integer>> entry : stageToTaskIds.entrySet()) {
            System.out.println(entry.getKey() + " 中任务情况: " + Arrays.toString(entry.getValue().toArray(new Integer[0])));

            for (Integer i : entry.getValue()) {
                if (taskIdToStatusMap.get(i) == null) {
                    System.out.println("\tnull");
                } else {
                    System.out.println("\t" + taskIdToStatusMap.get(i).getTaskStatus());
                }
            }
        }
        System.out.println("###################################### stage - tasks ############################################");
    }
}
