package com.ksc.wordcount.rpc.Executor;

import akka.actor.AbstractActor;
import com.ksc.wordcount.task.compare.CompReduceTask;
import com.ksc.wordcount.task.compare.CompareTaskContext;
import com.ksc.wordcount.task.map.MapTaskContext;
import com.ksc.wordcount.task.map.ShuffleMapTask;
import com.ksc.wordcount.task.reduce.ReduceTask;
import com.ksc.wordcount.task.reduce.ReduceTaskContext;
import com.ksc.wordcount.worker.ExecutorThreadPoolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutorActor extends AbstractActor {
    private final Logger log = LoggerFactory.getLogger(ExecutorActor.class);

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(MapTaskContext.class, taskContext -> {
                    log.info("收到 MapTaskContext");
                    // 通过线程池执行 map 和 shuffle 运算
                    ExecutorThreadPoolFactory.getExecutorService().submit(new ShuffleMapTask(taskContext));
                })
                .match(ReduceTaskContext.class, taskContext -> {
                    log.info("收到 ReduceTaskContext");
                    ExecutorThreadPoolFactory.getExecutorService().submit(new ReduceTask(taskContext));
                })
                .match(CompareTaskContext.class, taskContext -> {
                    log.info("收到 CompareTaskContext");
                    ExecutorThreadPoolFactory.getExecutorService().submit(new CompReduceTask(taskContext));
                })
                .match(Object.class, message -> {
                    //处理不了的消息
                    log.warn("无法识别任务");
                })
                .build();
    }
}
