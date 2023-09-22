package com.ksc.wordcount.conf;

import com.ksc.wordcount.util.StringUtil;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AppConfig {

    public final static String MASTER_PATH = "D:\\homework\\last\\wordcountdemo2\\bin\\master.conf";
    public final static String URL_TOP_N_PATH   = "D:\\homework\\last\\wordcountdemo2\\bin\\urltopn.conf";

    public final static String shuffleTempDir = System.getProperty("java.io.tmpdir") + "/shuffle"; // /tmp/shuffle


    public static class MasterConfig  {
        public String ip;
        public int akkaPort;
        public int thriftPort;
        public long memory;

        public static MasterConfig loadFromPath(String path) throws IOException {
            MasterConfig config = null;
            Stream<String> lines = Files.lines(Paths.get(path));
            List<String> lineList = lines.map(String::trim)
                    .filter(line -> !line.isEmpty() && !line.startsWith("#"))
                    .collect(Collectors.toList());
            if (lineList.size() > 0) {
                String[] tokens = lineList.get(0).split("\\s+");
                if (tokens.length >= 4) {
                    config = new MasterConfig();
                    config.ip = tokens[0];
                    config.akkaPort = Integer.parseInt(tokens[1]);
                    config.thriftPort = Integer.parseInt(tokens[2]);
                    config.memory = StringUtil.getMemory(tokens[3]);
                }
            }
            return config;
        }

        @Override
        public String toString() {
            return "MasterConfig{" +
                    "ip='" + ip + '\'' +
                    ", akkaPort=" + akkaPort +
                    ", thriftPort=" + thriftPort +
                    ", memory=" + memory +
                    '}';
        }
    }

    public static class SlaveConfig {
        public String ip;
        public int akkaPort;
        public int rpcPort;
        public long memory;
        public int coreNum;

        public static List<SlaveConfig> loadFromPath(String path) throws IOException {
            List<SlaveConfig> slaveConfigList = new LinkedList<>();
            Stream<String> lines = Files.lines(Paths.get(path));
            List<String> lineList = lines.map(String::trim)
                    .filter(line -> !line.isEmpty() && !line.startsWith("#"))
                    .collect(Collectors.toList());

            for (String data : lineList) {
                String[] tokens = data.split("\\s+");
                if (tokens.length >= 5) {
                    SlaveConfig config = new SlaveConfig();
                    config.ip = tokens[0];
                    config.akkaPort = Integer.parseInt(tokens[1]);
                    config.rpcPort = Integer.parseInt(tokens[2]);
                    config.memory  = StringUtil.getMemory(tokens[3]);
                    config.coreNum = Integer.parseInt(tokens[4]);
                    slaveConfigList.add(config);
                }
            }
            return slaveConfigList;
        }

        @Override
        public String toString() {
            return "SlaveConfig{" +
                    "ip='" + ip + '\'' +
                    ", akkaPort=" + akkaPort +
                    ", rpcPort=" + rpcPort +
                    ", memory=" + memory +
                    ", coreNum=" + coreNum +
                    '}';
        }
    }

    public static class URLTopNConfig {
        public String appId;
        public String inputDir;
        public String outputDir;
        public int topN;
        public int reduceTaskNum;
        public int partSize;

        public static URLTopNConfig loadFromPath(String path) throws IOException {
            URLTopNConfig config = null;
            Stream<String> lines = Files.lines(Paths.get(path));
            List<String> lineList = lines.map(String::trim)
                    .filter(line -> !line.isEmpty() && !line.startsWith("#"))
                    .collect(Collectors.toList());
            if (lineList.size() > 0) {
                String[] tokens = lineList.get(0).split("\\s+");
                if (tokens.length >= 6) {
                    config = new URLTopNConfig();
                    config.appId = tokens[0];
                    config.inputDir = tokens[1];
                    config.outputDir = tokens[2];
                    config.topN = Integer.parseInt(tokens[3]);
                    config.reduceTaskNum = Integer.parseInt(tokens[4]);
                    config.partSize = Integer.parseInt(tokens[5]);
                }
            }
            return config;
        }

        @Override
        public String toString() {
            return "URLTopNConfig{" +
                    "appId='" + appId + '\'' +
                    ", inputDir='" + inputDir + '\'' +
                    ", outputDir='" + outputDir + '\'' +
                    ", topN=" + topN +
                    ", reduceTaskNum=" + reduceTaskNum +
                    ", partSize=" + partSize +
                    '}';
        }
    }
}
