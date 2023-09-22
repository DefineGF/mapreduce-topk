package com.ksc.wordcount.datasourceapi.reader;

import com.ksc.wordcount.datasourceapi.split.PartionFile;

import java.io.IOException;
import java.util.stream.Stream;

public interface PartionReader<T>  {

    Stream<T> toStream(PartionFile partionFile) throws IOException;
}
