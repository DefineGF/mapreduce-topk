package com.ksc.wordcount.driver;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.ksc.wordcount.conf.AppConfig;
import com.ksc.wordcount.conf.FuncCollection;
import com.ksc.wordcount.datasourceapi.split.FileFormat;
import com.ksc.wordcount.datasourceapi.split.PartionFile;
import com.ksc.wordcount.datasourceapi.SplitFileFormat;
import com.ksc.wordcount.datasourceapi.writer.AvroPartionWriter;
import com.ksc.wordcount.rpc.Driver.DriverActor;
import com.ksc.wordcount.rpc.Driver.DriverSystem;
import com.ksc.wordcount.shuffle.KryoShuffleWriter;
import com.ksc.wordcount.shuffle.ShuffleBlockId;
import com.ksc.wordcount.task.compare.CompareTaskContext;
import com.ksc.wordcount.task.map.MapTaskContext;
import com.ksc.wordcount.task.reduce.ReduceTaskContext;
import com.ksc.wordcount.thrift.server.ThriftServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 共分为三个阶段：
 * 1. map 阶段，传入 url 源文件，解析为键值对，保存 kryo 文件；
 * 2. reduce1 阶段，汇总键值对，并保存 kryo 文件；
 * 3. reduce2 阶段，选择 topN 键值对，并保存 avro 文件；
 */
public class WordCountDriver {
    private final static Logger logger = LoggerFactory.getLogger(WordCountDriver.class);

    /**
     * 注意三个阶段序号需唯一且按顺序递增，不可更改
     */
    public final static int mapStageId     = 1;
    public final static int reduceStageId  = 2;
    public final static int compareStageId = 3;


    public static void main(String[] args) {
        String masterConfigPath = AppConfig.MASTER_PATH;
        String urlTopNConfigPath = AppConfig.URL_TOP_N_PATH;

        if (args.length == 2) {
            logger.info("启动参数: {} {}", args[0], args[1]);
            masterConfigPath = args[0];
            urlTopNConfigPath = args[1];
        }

        AppConfig.MasterConfig masterConfig = null;
        AppConfig.URLTopNConfig urlTopNConfig = null;

        // 加载配置文件
        try {
            masterConfig = AppConfig.MasterConfig.loadFromPath(masterConfigPath);
            urlTopNConfig = AppConfig.URLTopNConfig.loadFromPath(urlTopNConfigPath);
        } catch (IOException e) {
            logger.error("配置文件加载失败: {}", e.getMessage(), e);
            System.exit(0);
        }

        // 初始化部分全局属性
        DriverEnv.host = masterConfig.ip;       // 本机 ip
        DriverEnv.port = masterConfig.akkaPort; // 与 akka 通信端口
        String appId   = urlTopNConfig.appId;

        startAkkaServerActor(); // 启动 akka 服务

        mapStep(appId, urlTopNConfig.inputDir, urlTopNConfig.partSize, urlTopNConfig.reduceTaskNum);
        reduceStep1(appId, urlTopNConfig.reduceTaskNum);
        reduceStep2(appId, urlTopNConfig.outputDir, urlTopNConfig.topN);
        logger.info("JOB FINISHED!");
        System.out.println("\n\n\n\n");

        ThriftServer.startServer(masterConfig.thriftPort);// 启动 thrift 服务
    }

    private static void startAkkaServerActor() {
        ActorSystem actorSystem = DriverSystem.getExecutorSystem();
        ActorRef driverActorRef = actorSystem.actorOf(Props.create(DriverActor.class), "driverActor");
        logger.info("ServerActor start url = {}", driverActorRef.path().toString());
    }

    public static void mapStep(String appId, String inputDir, int partSize, int reduceTaskNum) {
        logger.info("Map 阶段开始");
        TaskManager taskManager = DriverEnv.getTaskManager();
        taskManager.registerBlockingQueue(mapStageId, new LinkedBlockingQueue<>());  //添加stageId和任务的映射

        // 文件切片
        FileFormat fileFormat = new SplitFileFormat();
        PartionFile[]  partionFiles = fileFormat.getSplits(inputDir, partSize);
        for (PartionFile partionFile : partionFiles) {
            // 每个文件启动一个 MapTask, 这些 MapTask 共享同一 StageId
            MapTaskContext mapTaskContext = new MapTaskContext.Builder()
                    .taskContext(appId, "stage_" + mapStageId, taskManager.generateTaskId(), partionFile.getPartionId())
                    .partionFile(partionFile)
                    .reader(fileFormat.createReader())  // 从文件系统中读取数据
                    .writer(new KryoShuffleWriter(appId, mapStageId, reduceTaskNum))
                    .reduceTaskNum(reduceTaskNum)
                    .mapFunc(FuncCollection.urlMapFunc).build();
            taskManager.addTaskContext(mapStageId, mapTaskContext);
        }
        //提交 map 阶段的所有任务
        TaskScheduler scheduler = DriverEnv.getTaskScheduler();
        scheduler.submitTask(mapStageId);
        scheduler.waitStageFinish(mapStageId);

        logger.info("Map 阶段结束");
        scheduler.logInfo();
    }

    public static void reduceStep1(String appId, int reduceTaskNum) {
        logger.info("Reduce1 阶段开始");
        TaskManager taskManager = DriverEnv.getTaskManager();
        TaskScheduler scheduler = DriverEnv.getTaskScheduler();

        taskManager.registerBlockingQueue(reduceStageId, new LinkedBlockingQueue<>());
        // 该过程创建 reduceTaskNum 个最终文件
        for(int i = 0; i < reduceTaskNum; i++){
            // 加入文件分成 n 片，则在 map 阶段共生成 n * reduceTaskNum 个 shuffle 文件
            // 在 reduce 阶段，每 n 个 shuffle 文件经过 reduce 后写入 一个 shuffle 中，共需要执行 reduceTaskNum 个 reduce 过程
            ShuffleBlockId[] stageShuffleIds = taskManager.getShuffleIdByMapId(mapStageId, i);
            ReduceTaskContext reduceTaskContext = new ReduceTaskContext.Builder()
                    .taskContext(appId, "stage_" + reduceStageId, taskManager.generateTaskId(), i)
                    .shuffleBlockIds(stageShuffleIds) // 将文件路径交由 ReduceTask 中 netty 处理
                    .reduceFunc(FuncCollection.reduceFunction)
                    .writer(new KryoShuffleWriter(appId, reduceStageId, 1))
                    .build();
            taskManager.addTaskContext(reduceStageId, reduceTaskContext);
        }
        scheduler.submitTask(reduceStageId);
        scheduler.waitStageFinish(reduceStageId);
        logger.info("Reduce1 阶段结束");
        scheduler.logInfo();
    }

    public static void reduceStep2(String appId, String outputDir, int topN) {
        logger.info("Reduce2 阶段开始");
        TaskManager taskManager = DriverEnv.getTaskManager();
        TaskScheduler scheduler = DriverEnv.getTaskScheduler();

        taskManager.registerBlockingQueue(compareStageId, new LinkedBlockingQueue<>());
        ShuffleBlockId[] stageShuffleIds = taskManager.getShuffleIdsByReduceId(reduceStageId);
        CompareTaskContext compTaskContext = new CompareTaskContext.Builder()
                .taskContext(appId, "stage_" + compareStageId, taskManager.generateTaskId(), 0)
                .shuffleBlockIds(stageShuffleIds)
                .comReduceFunc(FuncCollection.compReduceFunc)
                .writer(new AvroPartionWriter(outputDir, 0)) // 最终结果 writer
                .topN(topN).build();
        taskManager.addTaskContext(compareStageId, compTaskContext);

        scheduler.submitTask(compareStageId);
        scheduler.waitStageFinish(compareStageId);
        logger.info("Reduce2 阶段结束");
        scheduler.logInfo();
    }

}
