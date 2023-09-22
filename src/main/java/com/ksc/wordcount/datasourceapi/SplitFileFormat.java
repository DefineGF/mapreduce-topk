package com.ksc.wordcount.datasourceapi;

import com.ksc.wordcount.datasourceapi.reader.PartionReader;
import com.ksc.wordcount.datasourceapi.reader.TextPartionReader;
import com.ksc.wordcount.datasourceapi.split.FileFormat;
import com.ksc.wordcount.datasourceapi.split.FileSplit;
import com.ksc.wordcount.datasourceapi.split.PartionFile;
import com.ksc.wordcount.datasourceapi.writer.PartionWriter;
import com.ksc.wordcount.datasourceapi.writer.TextPartionWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

public class SplitFileFormat implements FileFormat {
    private final static Logger log = LoggerFactory.getLogger(SplitFileFormat.class);

    private int partId = 0;


    @Override
    public boolean isSplitable(String filePath) {
        return true;
    }

    @Override
    public PartionFile[] getSplits(String filePath, long size) {
        File target = new File(filePath);
        if (target.isFile()) {
            return new PartionFile[] {splitSingleFile(filePath, size, partId)};
        }
        File[] files = target.listFiles();
        if (files == null || files.length == 0) {
            // 目录下没有文件
            return null;
        }
        List<PartionFile> result = new ArrayList<>();
        for (File file : files) {
            PartionFile temp = splitSingleFile(file.getAbsolutePath(), size, partId++);
            result.add(temp);
        }
        return result.toArray(new PartionFile[0]);
    }

    private PartionFile splitSingleFile(String filePath, long size, int id) {
        File file = new File(filePath);
        if (!file.exists() || file.length() == 0) return null;

        if (file.length() <= size) {
            // 直接切分
            FileSplit[] temp = {new FileSplit(filePath, 0, file.length())};
            return new PartionFile(id, temp);
        }
        // 按行切分
        List<FileSplit> result = new LinkedList<>();
        try {
            // 当前 line的字节长度 + linux换行符(utf-8下为1字节)
            Stream<Integer> lens = Files.lines(Paths.get(filePath))
                    .map(line -> line.getBytes(StandardCharsets.UTF_8).length + 1);

            long start = 0;
            long curSize = 0;
            Iterator<Integer> iterator = lens.iterator();
            while (iterator.hasNext()) {
                Integer next = iterator.next();
                curSize += next;
                if (curSize >= size || !iterator.hasNext()) {
                    // 当加上当前行满足长度 或者 已经是最后一个元素时
                    FileSplit split = new FileSplit(file.getAbsolutePath(), start, curSize);
                    start += curSize;
                    curSize = 0;
                    result.add(split);
                }
            }
            FileSplit[] temp = result.toArray(new FileSplit[0]);
            return new PartionFile(id, temp);
        } catch (IOException e) {
            log.error("切片出错: {}", e.getMessage(), e);
        }
        return null;
    }

    @Override
    public PartionReader createReader() {
        return new TextPartionReader();
    }

    @Override
    public PartionWriter createWriter(String destPath, int partionId) {
        return new TextPartionWriter(destPath, partionId);
    }
}
