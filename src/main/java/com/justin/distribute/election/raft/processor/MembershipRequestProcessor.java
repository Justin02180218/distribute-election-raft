package com.justin.distribute.election.raft.processor;

import com.justin.distribute.election.raft.Node;
import com.justin.distribute.election.raft.NodeMetadata;
import com.justin.distribute.election.raft.NodeStatus;
import com.justin.distribute.election.raft.log.Log;
import com.justin.distribute.election.raft.log.LogEntry;
import com.justin.distribute.election.raft.membership.*;
import com.justin.net.remoting.netty.NettyRequestProcessor;
import com.justin.net.remoting.protocol.RemotingMessage;
import io.netty.channel.ChannelHandlerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public class MembershipRequestProcessor implements NettyRequestProcessor {
    private static final Logger logger = LogManager.getLogger(MembershipRequestProcessor.class.getSimpleName());

    private final Node node;

    public MembershipRequestProcessor(final Node node) {
        this.node = node;
    }

    @Override
    public RemotingMessage processRequest(ChannelHandlerContext ctx, RemotingMessage request) throws Exception {
        try {
            if (node.getMembershipLock().tryLock(3000, TimeUnit.MILLISECONDS)) {
                String membershipType = request.getMessageHeader().getExtFields().get(MembershipMessage.MEMBERSHIP_MESSAGE_TYPE);
                if (membershipType.equals(MembershipMessageType.CONFIGURATION.name())) {
                    ConfigurationMessage confMsg = ConfigurationMessage.getInstance().parseMessage(request);
                    Map<Integer, String> conf = new HashMap<>();
                    for (Map.Entry<Integer, NodeMetadata> entry : node.getCluster().entrySet()) {
                        Integer key = entry.getKey();
                        NodeMetadata peer = entry.getValue();
                        conf.put(key, peer.getNodeAddress());
                    }
                    confMsg.setMembers(conf);
                    confMsg.setLeaderId(node.getLeaderId());

                    return confMsg.response(request);
                }

                if (node.getStatus() != NodeStatus.LEADER) {
                    logger.info("Redirect to leader: " + node.getLeaderId());
                    return node.redirect(request);
                }

                if (membershipType.equals(MembershipMessageType.ADD.name())) {
                    AddPeerMessage addPeerMsg = AddPeerMessage.getInstance().parseMessage(request);
                    NodeMetadata peer = new NodeMetadata(addPeerMsg.getPeerId(), addPeerMsg.getAddress(), false);
                    node.getCluster().put(addPeerMsg.getPeerId(), peer);

                    addPeerMsg.setSuccess(true);
                    return addPeerMsg.response(request);
                }

                if (membershipType.equals(MembershipMessageType.REMOVE.name())) {
                    RemovePeerMessage removePeerMsg = RemovePeerMessage.getInstance().parseMessage(request);
                    node.getCluster().remove(removePeerMsg.getPeerId());

                    removePeerMsg.setSuccess(true);
                    return removePeerMsg.response(request);
                }
            }
            return null;
        }finally {
            node.getMembershipLock().unlock();
        }
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}
