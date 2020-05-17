package com.justin.distribute.election.raft.processor;

import com.justin.distribute.election.raft.Node;
import com.justin.distribute.election.raft.NodeMetadata;
import com.justin.distribute.election.raft.message.MembersMessage;
import com.justin.net.remoting.netty.NettyRequestProcessor;
import com.justin.net.remoting.protocol.RemotingMessage;
import io.netty.channel.ChannelHandlerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public class MembersRequestProcessor implements NettyRequestProcessor {
    private static final Logger logger = LogManager.getLogger(MembersRequestProcessor.class.getSimpleName());

    private final Node node;

    public MembersRequestProcessor(final Node node) {
        this.node = node;
    }

    @Override
    public RemotingMessage processRequest(ChannelHandlerContext ctx, RemotingMessage request) throws Exception {
        MembersMessage resMembersMsg = MembersMessage.getInstance().parseMessage(request);
        List<NodeMetadata> peers = resMembersMsg.getPeers();
        for (NodeMetadata peer : peers) {
            NodeMetadata metadata = node.getCluster().get(peer.getNodeId());
            if (metadata == null) {
                logger.info("Add node: " + peer);
                node.getCluster().put(peer.getNodeId(), peer);
            }
        }
        for (Map.Entry<Integer, NodeMetadata> entry : node.getCluster().entrySet()) {
            boolean flag = false;
            for (NodeMetadata peer : peers) {
                if (peer.getNodeId() == entry.getKey()) {
                    flag = true;
                    break;
                }
            }
            if (!flag) {
                logger.info("Remove node: " + entry.getValue());
                node.getCluster().remove(entry.getKey());
            }
        }

        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}
