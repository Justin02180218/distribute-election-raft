package com.justin.distribute.election.raft.client;

import com.justin.distribute.election.raft.common.PropertiesUtil;
import com.justin.net.remoting.RemotingClient;
import com.justin.net.remoting.netty.NettyRemotingClient;
import com.justin.net.remoting.netty.conf.NettyClientConfig;
import com.justin.net.remoting.protocol.RemotingMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public class Client {
    private static final Logger logger = LogManager.getLogger(Client.class.getSimpleName());

    private final String[] raftNodes;
    private final String transType;
    private final RemotingClient client;

    private Client() {
        raftNodes = PropertiesUtil.getRaftNodesAddress().split(",", -1);
        transType = PropertiesUtil.getClientTransType().toUpperCase();

        client = new NettyRemotingClient(new NettyClientConfig());
        client.start();
    }

    private static class ClientSingle {
        private static final Client INSTANCE = new Client();
    }

    public static Client getInstance() {
        return ClientSingle.INSTANCE;
    }

    public void shutdown() {
        client.shutdown();
    }

    public String get(String key) {
        KVMessage kvMessage = KVMessage.getInstance();
        kvMessage.setKey(key);
        kvMessage.setKvType(KVMessage.KVType.GET);

        try {
            RemotingMessage response = client.invokeSync(getRandomNodeAddr(), kvMessage.request(), 3000);
            KVMessage res = KVMessage.getInstance().parseMessage(response);
            return res.getValue();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public boolean put(String key, String value) {
        KVMessage kvMessage = KVMessage.getInstance();
        kvMessage.setKey(key);
        kvMessage.setValue(value);
        kvMessage.setKvType(KVMessage.KVType.PUT);
        kvMessage.setTransType(KVMessage.TransType.valueOf(transType));

        try {
            RemotingMessage response = client.invokeSync(getRandomNodeAddr(), kvMessage.request(), 3000);
            KVMessage res = KVMessage.getInstance().parseMessage(response);
            return res.getSuccess();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    private String getRandomNodeAddr() {
        Random random = new Random();
        int i = random.nextInt(raftNodes.length);
        return raftNodes[i];
    }
}
