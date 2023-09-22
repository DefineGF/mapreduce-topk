package com.ksc.wordcount.shuffle.nettyimpl.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;

import java.util.HashMap;
import java.util.Map;

public class ShuffleService {
    private final static Logger log = LoggerFactory.getLogger(ShuffleService.class);
    int serverPort;

    public ShuffleService(int serverPort){
        this.serverPort = serverPort;
    }


    /**
     * 使用Netty构建了一个TCP服务器，通过配置事件循环组、通道类型、通道选项和处理器等，实现了接收客户端连接请求、处理客户端的I/O操作
     */
    public void start() throws InterruptedException {
        // 创建了两个事件循环组，bossGroup 用于接收客户端连接请求，workerGroup 用于处理客户端连接的I/O操作。
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        // 创建了一个ServerBootstrap实例
        ServerBootstrap b = new ServerBootstrap().group(bossGroup, workerGroup);
        try {
                    b.channel(NioServerSocketChannel.class) // 指定了服务器使用NIO进行通信，并且使用NioServerSocketChannel作为服务器的通道类型。
                    .childOption(ChannelOption.SO_KEEPALIVE, true) // 其中SO_KEEPALIVE选项用于启用TCP keep-alive机制，以保持连接的活跃状态。
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        // 设置了服务器的子通道处理程序，用于配置新建立的客户端连接的处理流水线（pipeline）
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            // 定义了处理客户端连接的处理器（handler）的添加顺序。
                            // 这里使用了ObjectEncoder和ObjectDecoder进行对象的序列化与反序列化，
                            // 并添加了自定义的ShuffleServiceHandler处理器
                            ch.pipeline().addLast(new ObjectEncoder(),
                                    new ObjectDecoder(ClassResolvers.cacheDisabled(null)),
                                    new ShuffleServiceHandler());
                        }
                    });
            // 绑定服务器的端口并启动服务器，返回一个ChannelFuture对象，用于异步操作的结果处理
            ChannelFuture f = b.bind(serverPort).sync();
            log.info("netty server stated at port: {}", serverPort);
            f.channel().closeFuture().sync(); // 等待服务器的Channel关闭，即阻塞当前线程直到服务器关闭。
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }
}
