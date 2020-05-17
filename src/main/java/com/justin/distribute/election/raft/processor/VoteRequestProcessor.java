package com.justin.distribute.election.raft.processor;

import com.justin.distribute.election.raft.Node;
import com.justin.distribute.election.raft.NodeMetadata;
import com.justin.distribute.election.raft.NodeStatus;
import com.justin.distribute.election.raft.message.VoteMessage;
import com.justin.net.remoting.netty.NettyRequestProcessor;
import com.justin.net.remoting.protocol.RemotingMessage;
import io.netty.channel.ChannelHandlerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public class VoteRequestProcessor implements NettyRequestProcessor {
    private static final Logger logger = LogManager.getLogger(VoteRequestProcessor.class.getSimpleName());

    private final Node node;

    public VoteRequestProcessor(final Node node) {
        this.node = node;
    }

    @Override
    public RemotingMessage processRequest(ChannelHandlerContext ctx, RemotingMessage request) throws Exception {
        try {
            if (node.getVoteLock().tryLock()) {
                VoteMessage result = VoteMessage.getInstance().parseMessage(request);
                logger.info("Receive vote request from " + result);

                node.getCluster().get(result.getNodeId()).getCurrentTerm().set(result.getTerm());

                long currentTerm = node.getMetadata().getCurrentTerm().get();
                if (result.getTerm() < currentTerm) {
                    result.setTerm(currentTerm);
                    result.setVoteGranted(false);
                    return result.response(request);
                }

                if (result.getTerm() > currentTerm) {
                    node.comedown(result.getTerm());
                }

                if (node.getVoteFor() == -1 || node.getVoteFor() == result.getCandidateId()) {
                    node.setStatus(NodeStatus.FOLLOWER);
                    node.getMetadata().getCurrentTerm().compareAndSet(currentTerm, result.getTerm());
                    node.getMetadata().setVoteGrant(true);
                    node.setVoteFor(result.getNodeId());

                    result.setTerm(currentTerm);
                    result.setNodeId(node.getNodeId());
                    result.setVoteGranted(true);

                    logger.info("Send vote response: " + result);
                    return result.response(request);
                }
            }

            VoteMessage result = VoteMessage.getInstance();
            result.setTerm(node.getMetadata().getCurrentTerm().get());
            result.setVoteGranted(false);
            return result.response(request);
        }finally {
            node.getVoteLock().unlock();
        }
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}
