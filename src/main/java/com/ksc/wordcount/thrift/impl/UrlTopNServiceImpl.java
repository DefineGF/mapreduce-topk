package com.ksc.wordcount.thrift.impl;

import com.ksc.wordcount.datasourceapi.TempResult;
import com.ksc.wordcount.datasourceapi.writer.AvroPartionWriter;
import com.ksc.wordcount.driver.*;
import com.ksc.wordcount.task.KeyValue;
import com.ksc.wordcount.thrift.UrlTopNAppRequest;
import com.ksc.wordcount.thrift.UrlTopNAppResponse;
import com.ksc.wordcount.thrift.UrlTopNResult;
import com.ksc.wordcount.thrift.UrlTopNService;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class UrlTopNServiceImpl implements UrlTopNService.Iface {
    private final static Logger logger = LoggerFactory.getLogger(UrlTopNServiceImpl.class);
    private final TaskManager taskManager;

    private final Map<String, UrlTopNAppRequest> requestMap;      // 记录提交的任务
    private final Map<String, List<UrlTopNResult>> resultMap;     // 保存结果



    public UrlTopNServiceImpl() {
        logger.info("创建 UrlTopNServiceImpl 实例!");
        taskManager = DriverEnv.getTaskManager();
        TaskScheduler scheduler = DriverEnv.getTaskScheduler();

        taskManager.clear();
        scheduler.clear();

        requestMap = Collections.synchronizedMap(new HashMap<>());
        resultMap = Collections.synchronizedMap(new HashMap<>());
    }


    /*

struct UrlTopNAppRequest {
    1: string applicationId;
    2: string inputPath;
    3: string ouputPath;
    4: i32 topN;
    5: i32 numReduceTasks;
    6: i32 splitSize;
}
     */
    @Override
    public UrlTopNAppResponse submitApp(UrlTopNAppRequest urlTopNAppRequest) throws TException {
        logger.info("接收 thrift 请求: {} {} {} {}", urlTopNAppRequest.getApplicationId(),
                urlTopNAppRequest.getInputPath(), urlTopNAppRequest.getOuputPath(), urlTopNAppRequest.getTopN() );

        String appId = urlTopNAppRequest.getApplicationId();
        if (requestMap.get(appId) != null) {
            // 任务已提交，调用接口获取状态
            try {
                return getAppStatus(urlTopNAppRequest.applicationId);
            } catch (TException e) {
                logger.error("getAppStatus error：{}", e.getMessage(), e);
            }
        }
        // 任务未提交
        String inputDir = urlTopNAppRequest.getInputPath();
        String outputDir = urlTopNAppRequest.getOuputPath();
        int topN = urlTopNAppRequest.getTopN();
        int numReduceTasks = urlTopNAppRequest.getNumReduceTasks();
        int splitSize = urlTopNAppRequest.getSplitSize();

        // 调用核心方法处理， 异步提交任务
        new Thread(() -> {
            WordCountDriver.mapStep(appId, inputDir, splitSize, numReduceTasks);
            WordCountDriver.reduceStep1( appId, numReduceTasks);
            WordCountDriver.reduceStep2(appId, outputDir, topN);
        }).start();
        requestMap.put(appId, urlTopNAppRequest);
        return new UrlTopNAppResponse(appId, 0);
    }

    /**
     * struct UrlTopNAppResponse {
     *     1: string applicationId;
     *     2: i32 appStatus; // 0: accepted, 1: running, 2: finished, 3: failed
     * }
     */
    @Override
    public UrlTopNAppResponse getAppStatus(String applicationId) throws TException {
        logger.info("返回 {} 状态", applicationId);
        if (requestMap.get(applicationId) == null) {
            logger.info("{} 任务未提交", applicationId);
            return null;
        }
        StageStatusEnum stageTaskStatus = taskManager.getStageTaskStatus(WordCountDriver.compareStageId);
        if (stageTaskStatus == StageStatusEnum.RUNNING) {
            return new UrlTopNAppResponse(applicationId, 1);
        } else if (stageTaskStatus == StageStatusEnum.FINISHED) {
            return new UrlTopNAppResponse(applicationId, 2);
        } else if (stageTaskStatus == StageStatusEnum.FAILED) {
            return new UrlTopNAppResponse(applicationId, 3);
        } else {
            return new UrlTopNAppResponse(applicationId, 0);
        }
    }

    @Override
    public List<UrlTopNResult> getTopNAppResult(String applicationId, int topN) throws TException {
        List<UrlTopNResult> result = resultMap.get(applicationId);
        if (result != null) {
            return result;
        }
        result = new LinkedList<>();
        String resultFilePath = taskManager.getFinalFilePath();
        logger.info("获取最终文件路径: {}", resultFilePath);
        if (resultFilePath != null) {
            if (resultFilePath.endsWith(".avro")) {
                DatumReader<GenericRecord> datumReader
                        = new GenericDatumReader<>(AvroPartionWriter.getKeyValueSchema());
                DataFileReader<GenericRecord> dataFileReader = null;
                try {
                    dataFileReader = new DataFileReader<>(new File(resultFilePath), datumReader);
                    try {
                        for (GenericRecord record : dataFileReader) {
                            String key = record.get("key").toString();
                            int value = (int) (record.get("value"));
                            result.add(new UrlTopNResult(key, value));
                        }
                    } finally {
                        try {
                            dataFileReader.close();
                        } catch (IOException e) {
                            logger.error("avro 文件关闭失败: {} ", e.getMessage(), e);
                        }
                    }
                } catch (IOException e) {
                    logger.error("avro 文件读取失败: {}", e.getMessage(), e);
                }
            }
        }
        return result;
    }
}
