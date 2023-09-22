package com.ksc.wordcount.driver;

import com.ksc.wordcount.rpc.ExecutorRegister;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * 用于管理 akka 中 Executor
 */
public class ExecutorManager {

    /**
     * ExecutorUrl和ExecutorRegister的映射
     */
    private final Map<String, ExecutorRegister> execURLToRegisterMap = Collections.synchronizedMap(new HashMap<>());

    /**
     * ExecutorUrl和 Core数的映射
     */
    private final Map<String, Integer> executorAvailableCoresMap = Collections.synchronizedMap(new HashMap<>());

    public void updateExecutorRegister(ExecutorRegister executorRegister) {
        execURLToRegisterMap.put(executorRegister.getExecutorUrl(), executorRegister);
        //建立ExecutorUrl和Core数的映射
        executorAvailableCoresMap.put(executorRegister.getExecutorUrl(), executorRegister.getCores());
    }

    public void logInfo() {
        for (Map.Entry<String, Integer> entry : executorAvailableCoresMap.entrySet()) {
            System.out.println("url: " + entry.getKey() + " : " + entry.getValue());
        }
    }

    public Map<String, Integer> getExecutorAvailableCoresMap() {
        return executorAvailableCoresMap;
    }

    public Map<String, ExecutorRegister> getExecURLToRegisterMap() {
        return execURLToRegisterMap;
    }

    public int getExecutorMaxCore(String executorUrl) {
        return execURLToRegisterMap.get(executorUrl).getCores();
    }

    public int getExecutorAvalibeCore(String executorUrl) {
        return executorAvailableCoresMap.get(executorUrl);
    }

    public synchronized void updateExecutorCores(String executorUrl, int addCores) {
        int oldCore = executorAvailableCoresMap.get(executorUrl) == null ? 0 : executorAvailableCoresMap.get(executorUrl);
        executorAvailableCoresMap.put(executorUrl, oldCore + addCores);
    }
}
