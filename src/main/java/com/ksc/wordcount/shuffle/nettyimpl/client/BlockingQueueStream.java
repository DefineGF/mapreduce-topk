package com.ksc.wordcount.shuffle.nettyimpl.client;


import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.stream.*;

class BlockingQueueStream<T> {

    private final BlockingQueue<T> queue;
    private boolean done = false;

    public BlockingQueueStream(int capacity) {
        queue = new LinkedBlockingQueue<>(capacity);
    }


    public Stream<T> stream() {
        Spliterator<T> spliterator = new Spliterators.AbstractSpliterator<T>(Long.MAX_VALUE, Spliterator.ORDERED) {

            public boolean tryAdvance(Consumer<? super T> action) {
                while (true) {
                    if (done && queue.isEmpty()) {
                        return false;
                    }
                    T t = queue.poll();
                    if (t != null) {
                        action.accept(t);
                        return true;
                    }
                }
            }
        };
        return StreamSupport.stream(spliterator, false);
    }

    public void add(T t) {
        try {
            queue.put(t);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void done() {
        done = true;
    }
}
