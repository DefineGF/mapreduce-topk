package com.ksc.wordcount.thrift.server;

import com.ksc.wordcount.thrift.UrlTopNService;
import com.ksc.wordcount.thrift.impl.UrlTopNServiceImpl;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftServer {
    private final static Logger logger = LoggerFactory.getLogger(ThriftServer.class);
    public static void startServer(int port) {
        logger.info("ThriftServer start ... ...");
        TProcessor tprocessor = new UrlTopNService.Processor<>(new UrlTopNServiceImpl());
        TServerSocket serverTransport;
        try {
            serverTransport = new TServerSocket(port);
            TThreadPoolServer.Args args = new TThreadPoolServer.Args(serverTransport)
                    .processor(tprocessor).protocolFactory(new TBinaryProtocol.Factory());
            TServer server = new TThreadPoolServer(args);
            server.serve();
        } catch (TTransportException e) {
            e.printStackTrace();
        }
    }
}
