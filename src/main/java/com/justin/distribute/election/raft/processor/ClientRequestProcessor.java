package com.justin.distribute.election.raft.processor;

import com.justin.distribute.election.raft.Node;
import com.justin.distribute.election.raft.NodeStatus;
import com.justin.distribute.election.raft.client.KVMessage;
import com.justin.distribute.election.raft.common.PropertiesUtil;
import com.justin.distribute.election.raft.log.LogEntry;
import com.justin.net.remoting.common.Pair;
import com.justin.net.remoting.netty.NettyRequestProcessor;
import com.justin.net.remoting.protocol.RemotingMessage;
import io.netty.channel.ChannelHandlerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public class ClientRequestProcessor implements NettyRequestProcessor {
    private static final Logger logger = LogManager.getLogger(ClientRequestProcessor.class.getSimpleName());

    private final Node node;

    public ClientRequestProcessor(final Node node) {
        this.node = node;
    }

    @Override
    public RemotingMessage processRequest(ChannelHandlerContext ctx, RemotingMessage request) throws Exception {
        try {
            if (node.getClientLock().tryLock(3000, TimeUnit.MILLISECONDS)) {
                KVMessage kvMsg = KVMessage.getInstance().parseMessage(request);
                logger.info("Receive kv message from " + kvMsg);

                long currentTerm = node.getMetadata().getCurrentTerm().get();

                if (kvMsg.getKvType() == KVMessage.KVType.GET) {
                    LogEntry logEntry = node.getSnapshot().get(kvMsg.getKey());
                    if (logEntry != null) {
                        kvMsg.setValue(logEntry.getKv().getObject2());
                    } else {
                        kvMsg.setValue("");
                    }

                    logger.info("Client Get Response: " + kvMsg);
                    return kvMsg.response(request);
                }

                if (node.getStatus() != NodeStatus.LEADER) {
                    logger.info("Redirect to leader: " + node.getLeaderId());
                    return node.redirect(request);
                }

                if (kvMsg.getKvType() == KVMessage.KVType.PUT) {
                    long lastIndex = node.getLog().getLastIndex();
                    LogEntry logEntry = new LogEntry(lastIndex + 1, currentTerm, new Pair(kvMsg.getKey(), kvMsg.getValue()));
                    boolean flag = node.getSnapshot().apply(logEntry);
                    node.getSnapshotMap().put(node.getNodeId(), flag);

                    if (kvMsg.getTransType() == null) {
                        kvMsg.setTransType(KVMessage.TransType.valueOf(PropertiesUtil.getDataTransType().toUpperCase()));
                    }

                    if (kvMsg.getTransType() == KVMessage.TransType.ASYNC) {
                        logger.info("Leader Async write log!");
                        node.commitEntries(logEntry);

                        kvMsg.setSuccess(true);
                        return kvMsg.response(request);
                    }

                    if (kvMsg.getTransType() == KVMessage.TransType.SYNC) {
                        node.snapshotEntries(logEntry, true);
                        if (node.getSyncLatch().await(6000, TimeUnit.MILLISECONDS)) {
                            logger.info("Leader Sync write log!");
                            node.commitEntries(logEntry);

                            kvMsg.setSuccess(true);
                            return kvMsg.response(request);
                        }
                    }

                    if (kvMsg.getTransType() == KVMessage.TransType.HALF_SYNC) {
                        node.snapshotEntries(logEntry, false);
                        if (node.getHsyncLatch().await(6000, TimeUnit.MILLISECONDS)) {
                            logger.info("Leader Half-Sync write log!");
                            node.commitEntries(logEntry);

                            kvMsg.setSuccess(true);
                            return kvMsg.response(request);
                        }
                    }
                }
            }
            return null;
        }finally {
            node.getClientLock().unlock();
        }
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}
