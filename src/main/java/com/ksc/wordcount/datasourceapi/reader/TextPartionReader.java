package com.ksc.wordcount.datasourceapi.reader;

import com.ksc.wordcount.datasourceapi.split.FileSplit;
import com.ksc.wordcount.datasourceapi.split.PartionFile;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

/**
 * 用于以普通文件流读取行的方式获取 文本行的Stream
 */
public class TextPartionReader implements PartionReader<String>, Serializable {

    @Override
    public Stream<String> toStream(PartionFile partionFile) throws IOException {
        Stream<String> allStream = Stream.empty();
        for (FileSplit fileSplit : partionFile.getFileSplits()) {
            Stream<String> lineStream = Files.lines(Paths.get(fileSplit.getFileName()));
            allStream = Stream.concat(allStream, lineStream);
        }
        return allStream;
    }
}
