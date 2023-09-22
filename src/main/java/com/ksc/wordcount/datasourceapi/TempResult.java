package com.ksc.wordcount.datasourceapi;

import java.io.Serializable;

public class TempResult implements Serializable {
    private String url;
    private int count;

    public TempResult() {}

    public TempResult(String url, int count) {
        this.url = url;
        this.count = count;
    }

    public String getKey() {
        return url;
    }

    public void setKey(String url) {
        this.url = url;
    }

    public int getValue() {
        return count;
    }

    public void setValue(int count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "TempResult{" +
                "url='" + url + '\'' +
                ", count=" + count +
                '}';
    }
}
