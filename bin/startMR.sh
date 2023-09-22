#!/bin/bash
# 读取master 配置

master_conf="./master.conf"

line=$(grep -v '^#' "$master_conf")  # 过滤注释行
args=($line)
master_ip="${args[0]}" 
master_akka_port="${args[1]}"

#启动master
url_conf="./urltopn.conf"
java -cp mymr-1.0.jar com.ksc.wordcount.driver.WordCountDriver "$master_conf" "$url_conf" &

slave_conf="./slave.conf"
while IFS= read -r line || [[ -n "$line" ]]; do
  # 忽略以#开头的注释行
  if [[ $line == \#* ]]; then
    continue
  fi

  # 解析行中的参数
  args=($line)
  ip="${args[0]}"
  akka_port="${args[1]}"
  rpc_port="${args[2]}"
  memo="${args[3]}"
  cpu="${args[4]}"

  # 启动Java程序并传递参数
  java -cp mymr-1.0.jar com.ksc.wordcount.worker.Executor "$ip" "$master_ip" "$akka_port" "$rpc_port" "$master_akka_port" "$cpu" &
done < "$slave_conf"