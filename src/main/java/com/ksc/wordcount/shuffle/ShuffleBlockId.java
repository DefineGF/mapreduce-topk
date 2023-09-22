package com.ksc.wordcount.shuffle;

import java.io.Serializable;

/**
 * ShuffleBlockId 生成文件路径：
 *      shuffleBaseDir/applicationId/shuffle_shuffleId_mapId_reduceId.data
 */
public class ShuffleBlockId implements Serializable {
    String host;
    int port;
    String shuffleBaseDir;
    String shuffleId;
    String applicationId;
    int stageId;
    int reduceId;

    public ShuffleBlockId(String shuffleBaseDir, String applicationId, String shuffleId, int stageId, int reduceId) {
        this.shuffleBaseDir = shuffleBaseDir;
        this.applicationId = applicationId;
        this.shuffleId = shuffleId;
        this.stageId = stageId;
        this.reduceId = reduceId;
    }

    public void setHostAndPort(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getReduceId() {
        return reduceId;
    }

    public String name() {
        return "shuffle_" + shuffleId + "_" + stageId + "_" + reduceId;
    }

    public String getShufflePath(String suffix) {
        if (suffix == null || "".equals(suffix)) {
            suffix = ".data";
        }
        return getShuffleParentPath() + "/" + name() + suffix;
    }

    public String getShuffleParentPath() {
        return shuffleBaseDir + "/" + applicationId;
    }
}
