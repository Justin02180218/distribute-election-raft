package com.justin.distribute.election.raft.membership;

import com.justin.distribute.election.raft.common.PropertiesUtil;
import com.justin.net.remoting.RemotingClient;
import com.justin.net.remoting.netty.NettyRemotingClient;
import com.justin.net.remoting.netty.conf.NettyClientConfig;
import com.justin.net.remoting.protocol.RemotingMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Random;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public class MembershipAdmin {
    private static final Logger logger = LogManager.getLogger(MembershipAdmin.class.getSimpleName());

    private final String[] raftNodes;
    private final RemotingClient client;
    private long leaderId;

    private MembershipAdmin() {
        raftNodes = PropertiesUtil.getRaftNodesAddress().split(",", -1);

        client = new NettyRemotingClient(new NettyClientConfig());
        client.start();
    }

    private static class MembershipAdminSingle {
        private static final MembershipAdmin INSTANCE = new MembershipAdmin();
    }

    public static MembershipAdmin getInstance() {
        return MembershipAdminSingle.INSTANCE;
    }

    public void shutdown() {
        client.shutdown();
    }

    public Map<Integer, String> getConfiguration() {
        try {
            RemotingMessage response = client.invokeSync(getRandomNodeAddr(), ConfigurationMessage.getInstance().request(), 3000);
            ConfigurationMessage res = ConfigurationMessage.getInstance().parseMessage(response);
            leaderId = res.getLeaderId();
            return res.getMembers();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public boolean addPeer(final int peerId, final String peerAddr) {
        AddPeerMessage addPeerMsg = AddPeerMessage.getInstance();
        addPeerMsg.setPeerId(peerId);
        addPeerMsg.setAddress(peerAddr);

        try {
            RemotingMessage response = client.invokeSync(getRandomNodeAddr(), addPeerMsg.request(), 3000);
            AddPeerMessage res = AddPeerMessage.getInstance().parseMessage(response);
            return res.getSuccess();
        } catch(Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public boolean removePeer(final int peerId) {
        RemovePeerMessage removePeerMsg = RemovePeerMessage.getInstance();
        removePeerMsg.setPeerId(peerId);

        try {
            RemotingMessage response = client.invokeSync(getRandomNodeAddr(), removePeerMsg.request(), 3000);
            RemovePeerMessage res = RemovePeerMessage.getInstance().parseMessage(response);
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

    public long getLeaderId() {
        return leaderId;
    }
}
