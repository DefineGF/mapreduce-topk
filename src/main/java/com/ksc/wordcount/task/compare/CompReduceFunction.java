package com.ksc.wordcount.task.compare;

import com.ksc.wordcount.task.KeyValue;

import java.util.stream.Stream;

@FunctionalInterface
public interface CompReduceFunction<K, V> extends java.io.Serializable {
    Stream<KeyValue<K, V>> reduce(Stream<KeyValue<K, V>> stream, int capacity);
}
