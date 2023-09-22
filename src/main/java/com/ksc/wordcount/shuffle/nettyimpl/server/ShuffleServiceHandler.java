package com.ksc.wordcount.shuffle.nettyimpl.server;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.ksc.wordcount.shuffle.ShuffleBlockId;
import com.ksc.wordcount.shuffle.nettyimpl.FileComplate;
import com.ksc.wordcount.task.KeyValue;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class ShuffleServiceHandler extends ChannelInboundHandlerAdapter {
    private final static Logger log = LoggerFactory.getLogger(ShuffleServiceHandler.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ShuffleBlockId) {
            ShuffleBlockId shuffleBlockId = (ShuffleBlockId) msg;
            log.info("ShuffleServiceHandler received: {}", ((ShuffleBlockId) msg).name());

            File file = new File(shuffleBlockId.getShufflePath(".kryo"));
            if (file.exists()) {
//                ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(file));
//                Object obj = null;
//                //(obj=objectInputStream.readObject())!=null
//                do {
//                    try {
//                        obj = objectInputStream.readObject();
//                    } catch (EOFException e) {
//                        break;
//                    }
//                    ctx.writeAndFlush(obj);
//                } while (obj != null);

                // 通过 kryo 解析文件流
                Kryo kryo = new Kryo();
                kryo.register(KeyValue.class);
                Input input;
                try {
                    input = new Input(new FileInputStream(file));
                    KeyValue object2;
                    while (input.available() > 0) {
                        object2 = kryo.readObject(input, KeyValue.class);
                        // System.out.println(object2);
                        ctx.writeAndFlush(object2);
                    }
                    input.close();
                } catch (IOException e) {
                    log.error("kryo 流解析失败：{}", e.getMessage(), e);
                }
                ctx.writeAndFlush(new FileComplate());
            } else {
                ctx.writeAndFlush("shuffle File not found: " + file.getAbsolutePath());
            }
        } else {
            super.channelRead(ctx, msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }
}