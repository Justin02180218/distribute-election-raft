package com.justin.distribute.election.raft.callback;

import com.justin.distribute.election.raft.Node;
import com.justin.distribute.election.raft.NodeMetadata;
import com.justin.distribute.election.raft.NodeStatus;
import com.justin.distribute.election.raft.log.LogEntry;
import com.justin.distribute.election.raft.message.EntryMessage;
import com.justin.net.remoting.InvokeCallback;
import com.justin.net.remoting.ResponseProcessor;
import com.justin.net.remoting.protocol.RemotingMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public class EntryRequestCallback implements InvokeCallback {
    private static final Logger logger = LogManager.getLogger(EntryRequestCallback.class.getSimpleName());

    private final Node node;

    public EntryRequestCallback(final Node node) {
        this.node = node;
    }

    @Override
    public void operationComplete(ResponseProcessor responseProcessor) {
        try {
            if (node.getEntryLock().tryLock(3000, TimeUnit.MILLISECONDS)) {
                RemotingMessage response = responseProcessor.getResponseMessage();
                EntryMessage resEntryMsg = EntryMessage.getInstance().parseMessage(response);
                logger.info("Receive entry message response from " + resEntryMsg);

                NodeMetadata peer = node.getCluster().get(resEntryMsg.getNodeId());

                if (resEntryMsg.getSuccess()) {
                    logger.info("Follower log entry success , follower=[{}], entry=[{}]", resEntryMsg.getNodeId(), resEntryMsg.getEntries());
                    peer.setCommitIndex(resEntryMsg.getCommitIndex());
                }else {
                    if (resEntryMsg.getTerm() > node.getMetadata().getCurrentTerm().get()) {
                        node.getMetadata().getCurrentTerm().set(resEntryMsg.getTerm());
                        node.setStatus(NodeStatus.FOLLOWER);
                    }
                }
            }
        } catch (InterruptedException e) {
            logger.error(e);
        } finally {
            node.getEntryLock().unlock();
        }
    }
}
