package com.ksc.wordcount.rpc.Driver;

import akka.actor.AbstractActor;
import com.ksc.wordcount.driver.DriverEnv;
import com.ksc.wordcount.driver.ExecutorManager;
import com.ksc.wordcount.driver.TaskManager;
import com.ksc.wordcount.driver.TaskScheduler;
import com.ksc.wordcount.rpc.ExecutorRegister;
import com.ksc.wordcount.task.TaskStatus;
import com.ksc.wordcount.task.TaskStatusEnum;
import com.ksc.wordcount.rpc.RegisterSuccess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DriverActor extends AbstractActor {
    private final Logger logger = LoggerFactory.getLogger(DriverActor.class);

    private final TaskManager manager;
    private final TaskScheduler scheduler;
    private final ExecutorManager executorManager;


    public DriverActor() {
        this.manager = DriverEnv.getTaskManager();
        this.scheduler = DriverEnv.getTaskScheduler();
        this.executorManager = DriverEnv.getExecutorManager();
        logger.info("创建 DriverActor");
    }


    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(TaskStatus.class, status -> {
                    // 获取 Executor 执行 map 或 reduce 结果
                    logger.info("DriverActor receive TaskStatus: id = {}, status =  {}", status.getTaskId(), status.getTaskStatus());
                    if(status.getTaskStatus() == TaskStatusEnum.FAILED) {
                        logger.error("task failed: id = {}, msg = {}, stackTrace = {}", status.getTaskId(), status.getErrorMsg(), status.getErrorStackTrace());
                    }
                    manager.updateTaskStatus(status);  // 添加 taskId 与 task 结果的映射
                    scheduler.updateTaskStatus(status);// 更新 taskId 对应的 ExecURL 相关参数
                })
                .match(ExecutorRegister.class, executorRegister -> {
                    // 接收 ExecutorRpc.register 发送的注册信息
                    logger.info("DriverActor 接收到注册消息: {}", executorRegister.getExecutorUrl());
                    executorManager.updateExecutorRegister(executorRegister);
                    sender().tell(new RegisterSuccess(), self()); // 回复注册成功消息
                })
                .match(Object.class, message -> {
                    logger.warn("DriverActor receive unknown msg: {}", message);//处理不了的消息
                }).build();
    }
}
