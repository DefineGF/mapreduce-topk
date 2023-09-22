package com.ksc.wordcount.thrift.client;

import com.ksc.wordcount.thrift.UrlTopNAppRequest;
import com.ksc.wordcount.thrift.UrlTopNAppResponse;
import com.ksc.wordcount.thrift.UrlTopNResult;
import com.ksc.wordcount.thrift.UrlTopNService;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ThriftClient {
    private final static Logger logger = LoggerFactory.getLogger(ThriftClient.class);
    private final String ip;
    private final int port;

    public ThriftClient(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    public UrlTopNAppResponse submitApp(UrlTopNAppRequest urlTopNAppRequest) {
        try (TTransport transport = new TSocket(new TConfiguration(), ip, port, 3000)) {
            transport.open();

            TProtocol protocol = new TBinaryProtocol(transport); // 协议要和服务端一致
            UrlTopNService.Client client = new UrlTopNService.Client(protocol);
            UrlTopNAppResponse response = client.submitApp(urlTopNAppRequest);
            return response.deepCopy();
        } catch (TException e) {
            logger.error("submitApp error: {}", e.getMessage(), e);
        }
        return null;
    }

    /**
     *  0: accepted, 1: running, 2: finished, 3: failed
     */
    public UrlTopNAppResponse getAppStatus(String applicationId) {
        try (TTransport transport = new TSocket(new TConfiguration(), ip, port, 3000)) {
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport); // 协议要和服务端一致
            UrlTopNService.Client client = new UrlTopNService.Client(protocol);
            UrlTopNAppResponse response = client.getAppStatus(applicationId);
            return response.deepCopy();
        } catch (TException e) {
            logger.error("getAppStatus error: {}", e.getMessage(), e);
        }
        return null;
    }

    public List<UrlTopNResult> getTopNAppResult(String appId, int topN) {
        try (TTransport transport = new TSocket(new TConfiguration(), ip, port, 3000)) {
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport); // 协议要和服务端一致
            UrlTopNService.Client client = new UrlTopNService.Client(protocol);
            return client.getTopNAppResult(appId, topN);
        } catch (TException e) {
            logger.error("getTopNAppResult error: {}", e.getMessage(), e);
        }
        return null;
    }
}
