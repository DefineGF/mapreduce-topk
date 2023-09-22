package com.ksc.wordcount.conf;

import com.ksc.wordcount.task.KeyValue;
import com.ksc.wordcount.task.compare.CompReduceFunction;
import com.ksc.wordcount.task.map.MapFunction;
import com.ksc.wordcount.task.reduce.ReduceFunction;
import com.ksc.wordcount.util.StringUtil;

import java.util.*;
import java.util.stream.Stream;

public class FuncCollection {


    // map: 生成url 键值对
    public final static MapFunction<String, KeyValue<String, Integer>> urlMapFunc =
            stream -> stream.flatMap(line -> Stream.of(StringUtil.getUrlFromLine(line))).map(word -> new KeyValue<>(word, 1));

    // reduce: 处理 键值对
    public final static ReduceFunction<String, Integer, String, Integer> reduceFunction = stream -> {
        HashMap<String, Integer> map = new HashMap<>();
        stream.forEach(e ->{
            String key = e.getKey();
            Integer value = e.getValue();
            Integer newValue = map.getOrDefault(key, 0) + value;
            map.put(key, newValue);
        });
        return map.entrySet().stream().map(e -> new KeyValue<>(e.getKey(), e.getValue()));
    };

    // reduce: 对键值对进行
    public final static CompReduceFunction<String,Integer> compReduceFunc = (stream, capacity) -> {
        // 创建小根堆: 保存内容为 个数与该个数相关的 url 的映射
        PriorityQueue<KeyValue<Integer, List<String>>> result =
                new PriorityQueue<>(Comparator.comparing(KeyValue::getKey));
        stream.forEach(e -> {
            boolean isAdded = false;
            if (result.size() > 0) {
                Iterator<KeyValue<Integer, List<String>>> iterator = result.stream().iterator();
                while (iterator.hasNext()) {
                    KeyValue<Integer, List<String>> temp = iterator.next();
                    if (temp.getKey().equals(e.getValue())) {
                        temp.getValue().add(e.getKey());
                        isAdded = true;
                        break;
                    }
                }
                if (!isAdded && result.size() == capacity) {
                    result.poll();// 还没添加进去，同时达到容量上限
                }
            }
            if (!isAdded) {
                List<String> temp = new ArrayList<>();
                temp.add(e.getKey());
                result.add(new KeyValue<>(e.getValue(), temp));
            }
        });
        List<KeyValue<String, Integer>> ls = new LinkedList<>();
        KeyValue<Integer, List<String>> temp = result.poll();
        while (temp != null) {
            List<String> urls = temp.getValue();
            for (String url : urls) {
                ls.add(0, new KeyValue<>(url, temp.getKey()));
            }
            temp = result.poll();
        }
        return ls.stream();
    };
}
