package com.justin.distribute.election.raft.callback;

import com.justin.distribute.election.raft.Node;
import com.justin.distribute.election.raft.NodeMetadata;
import com.justin.distribute.election.raft.NodeStatus;
import com.justin.distribute.election.raft.message.VoteMessage;
import com.justin.net.remoting.InvokeCallback;
import com.justin.net.remoting.ResponseProcessor;
import com.justin.net.remoting.protocol.RemotingMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public class VoteRequestCallback implements InvokeCallback {
    private static final Logger logger = LogManager.getLogger(VoteRequestCallback.class.getSimpleName());

    private final Node node;

    public VoteRequestCallback(final Node node) {
        this.node = node;
    }

    @Override
    public void operationComplete(ResponseProcessor responseProcessor) {
        try {
            if (node.getVoteLock().tryLock()) {
                RemotingMessage response = responseProcessor.getResponseMessage();
                VoteMessage resVoteMsg = VoteMessage.getInstance().parseMessage(response);
                logger.info("Receive vote message response from " + resVoteMsg);

                NodeMetadata peer = node.getCluster().get(resVoteMsg.getNodeId());
                peer.setVoteGrant(resVoteMsg.getVoteGranted());
                peer.getCurrentTerm().set(resVoteMsg.getTerm());

                long currentTerm = node.getMetadata().getCurrentTerm().get();
                if (node.getStatus() != NodeStatus.CANDIDATE) {
                    return;
                }

                if (resVoteMsg.getTerm() > currentTerm) {
                    node.comedown(resVoteMsg.getTerm());
                }else {
                    if (resVoteMsg.getVoteGranted()) {
                        int voteNums = 1;
                        for (Map.Entry<Integer, NodeMetadata> entry : node.getCluster().entrySet()) {
                            if (entry.getKey() == node.getNodeId()) {
                                continue;
                            }

                            if (entry.getValue().getVoteGrant()) {
                                voteNums += 1;
                            }
                        }

                        if (voteNums > node.getCluster().size() / 2) {
                            logger.info("Vote, leaderId={}, become leader ...", node.getNodeId());
                            node.becomeLeader();
                        }
                    }else {
                        logger.info("Vote peer:{} term:{}, local term:{}", resVoteMsg.getNodeId(), peer.getCurrentTerm(), currentTerm);
                    }
                }
            }
        }finally {
            node.getVoteLock().unlock();
        }
    }
}
