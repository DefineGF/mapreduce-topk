package com.ksc.wordcount.datasourceapi.writer;

import java.io.IOException;
import java.util.stream.Stream;

public interface PartionWriter<T>   {
    void write(Stream<T> stream) throws IOException;
}
