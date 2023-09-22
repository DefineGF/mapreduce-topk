package com.ksc.wordcount.worker;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.ksc.wordcount.rpc.Executor.ExecutorActor;
import com.ksc.wordcount.rpc.Executor.ExecutorRpc;
import com.ksc.wordcount.rpc.Executor.ExecutorSystem;
import com.ksc.wordcount.rpc.ExecutorRegister;
import com.ksc.wordcount.shuffle.nettyimpl.server.ShuffleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Executor {
    private final static Logger log = LoggerFactory.getLogger(Executor.class);

    /**
     * 传入参数：
     * 本机 ip  & master_ip & akka_port & rpc(netty)_port  & master_port
     */
    public static void main(String[] args) throws InterruptedException {
        String selfIp = "127.0.0.1", masterIp = "127.0.0.1";
        int akkaPort = 5050, nettyPort = 7448, masterAkkaPort = 4040;
        int coreNum = 2;

        if (args.length == 6) {
            log.info("启动参数: {} {} {} {} {} {}", args[0], args[1], args[2], args[3], args[4], args[5]);
            selfIp = args[0];
            masterIp = args[1];
            akkaPort = Integer.parseInt(args[2]);
            nettyPort = Integer.parseInt(args[3]);
            masterAkkaPort = Integer.parseInt(args[4]);
            coreNum = Integer.parseInt(args[5]);
        } else {
            log.error("启动参数个数不匹配!");
            // System.exit(0);
        }

        ExecutorEnv.host = selfIp;
        ExecutorEnv.port = akkaPort;
        ExecutorEnv.memory = "512m";
        ExecutorEnv.driverUrl = "akka.tcp://DriverSystem@" + masterIp + ":" + masterAkkaPort + "/user/driverActor";
        ExecutorEnv.core = coreNum;
        ExecutorEnv.executorUrl = "akka.tcp://ExecutorSystem@" + ExecutorEnv.host + ":" + ExecutorEnv.port + "/user/executorActor";
        ExecutorEnv.shufflePort = nettyPort;

        new Thread(() -> {
            try {
                new ShuffleService(ExecutorEnv.shufflePort).start(); // 启动 netty，处理 nio 数据流
            } catch (InterruptedException e) {
                log.error("netty 连接失败!");
            }
        }).start();

        ActorSystem executorSystem = ExecutorSystem.getExecutorSystem();
        ActorRef clientActorRef = executorSystem.actorOf(Props.create(ExecutorActor.class), "executorActor");

        log.info("ServerActor started at: {}", clientActorRef.path().toString());
        ExecutorRpc.register(new ExecutorRegister(ExecutorEnv.executorUrl, ExecutorEnv.memory, ExecutorEnv.core));
    }

}
