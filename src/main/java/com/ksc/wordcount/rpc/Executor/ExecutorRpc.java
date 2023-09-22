package com.ksc.wordcount.rpc.Executor;

import akka.actor.ActorRef;
import akka.pattern.Patterns;
import com.ksc.wordcount.rpc.ExecutorRegister;
import com.ksc.wordcount.rpc.RegisterSuccess;
import com.ksc.wordcount.task.TaskStatus;
import com.ksc.wordcount.task.TaskStatusEnum;
import com.ksc.wordcount.worker.ExecutorEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

public class ExecutorRpc {

    private final static Logger log = LoggerFactory.getLogger(ExecutorRpc.class);

    /**
     * 向 Driver 报告状态信息
     */
    public static void updateTaskMapStatue(TaskStatus taskStatus) {
        // 为结果中的 ShuffleBlockId[] 设置 ip 和 端口号，便于 netty 文件流传输
        if (taskStatus.getTaskStatus() == TaskStatusEnum.FINISHED) {
            taskStatus.setShuffleBlockHostAndPort(ExecutorEnv.host, ExecutorEnv.shufflePort);
        }
        ExecutorSystem.getDriverRef().tell(taskStatus, ActorRef.noSender());
    }

    public static void register(ExecutorRegister executorRegister){
        // ExecutorSystem.getDriverRef().tell(executorRegister, ActorRef.noSender());

        for (int i = 0; i < 3; ++i) {
            Future<Object> replyFuture = Patterns.ask(ExecutorSystem.getDriverRef(), executorRegister, 3000);
            try {
                Object reply = Await.result(replyFuture, Duration.create(3, TimeUnit.SECONDS));
                if (reply instanceof RegisterSuccess) {
                    log.info("注册成功!");
                    break;
                }
            } catch (Exception e) {
                log.error("注册失败: {}", e.getMessage(), e);
            }
        }
    }
}
