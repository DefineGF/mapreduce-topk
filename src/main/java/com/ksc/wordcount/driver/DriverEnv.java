package com.ksc.wordcount.driver;

public class DriverEnv {
    public static String host;

    public static int port;
    
    private static TaskManager taskManager;
    private static ExecutorManager executorManager;
    private static TaskScheduler taskScheduler;

    public static TaskManager getTaskManager() {
        if (taskManager == null) {
            synchronized (DriverEnv.class) {
                if (taskManager == null) {
                    taskManager = new TaskManager();
                }
            }
        }
        return taskManager;
    }


    public static ExecutorManager getExecutorManager() {
        if (executorManager == null) {
            synchronized (DriverEnv.class) {
                if (executorManager == null) {
                    executorManager = new ExecutorManager();
                }
            }
        }
        return executorManager;
    }

    public static TaskScheduler getTaskScheduler() {
        if (taskScheduler == null) {
            synchronized (DriverEnv.class) {
                if (taskScheduler == null) {
                    taskScheduler = new TaskScheduler(getTaskManager(), getExecutorManager());
                }
            }
        }
        return taskScheduler;
    }
}
