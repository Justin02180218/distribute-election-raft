package com.justin.distribute.election.raft.processor;

import com.justin.distribute.election.raft.Node;
import com.justin.distribute.election.raft.NodeStatus;
import com.justin.distribute.election.raft.log.LogEntry;
import com.justin.distribute.election.raft.message.EntryMessage;
import com.justin.net.remoting.netty.NettyRequestProcessor;
import com.justin.net.remoting.protocol.RemotingMessage;
import io.netty.channel.ChannelHandlerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public class EntryRequestProcessor implements NettyRequestProcessor {
    private static final Logger logger = LogManager.getLogger(EntryRequestProcessor.class.getSimpleName());

    private final Node node;

    public EntryRequestProcessor(final Node node) {
        this.node = node;
    }

    @Override
    public RemotingMessage processRequest(ChannelHandlerContext ctx, RemotingMessage request) throws Exception {
        try {
            if (node.getEntryLock().tryLock(3000, TimeUnit.MILLISECONDS)) {
                EntryMessage result = EntryMessage.getInstance().parseMessage(request);

                long currentTerm = node.getMetadata().getCurrentTerm().get();
                result.setNodeId(node.getNodeId());
                result.setTerm(currentTerm);

                if (result.getTerm() < currentTerm) {
                    result.setSuccess(false);
                    return result.response(request);
                }

                node.getNodeConfig().setPreElectionTime(System.currentTimeMillis());
                node.getNodeConfig().setPreHeartbeatTime(System.currentTimeMillis());
                node.setLeaderId(result.getLeaderId());
                node.setStatus(NodeStatus.FOLLOWER);
                node.getMetadata().getCurrentTerm().set(result.getTerm());

                // heartbeat
                if (result.getType() == EntryMessage.Type.HEARTBEAT) {
                    logger.info("Receive heartbeat request from " + result);
                    if (result.getLeaderCommitIndex() > node.getMetadata().getCommitIndex()) {
                        for (LogEntry log : result.getEntries()) {
                            node.getLog().write(log);
                            node.getSnapshot().apply(log);
                        }
                    }else if (result.getLeaderCommitIndex() < node.getMetadata().getCommitIndex()) {
                        node.getLog().removeFromIndex(result.getLeaderCommitIndex()+1);
                    }

                    long commitIndex = node.getLog().getLastIndex();
                    node.getMetadata().setCommitIndex(commitIndex);

                    result.setCommitIndex(commitIndex);
                    result.setSuccess(true);
                    logger.info("Send heartbeat response: " + result);
                    return result.response(request);
                }

                if (result.getType() == EntryMessage.Type.COMMIT) {
                    if (node.getLog().getLastIndex() != 0 && result.getPreLogIndex() != 0) {
                        LogEntry logEntry = node.getLog().read(result.getPreLogIndex());
                        if (logEntry == null || logEntry.getTerm() != result.getPreLogTerm()) {
                            result.setSuccess(false);
                            return result.response(request);
                        }
                    }

                    LogEntry logEntry = node.getLog().read(result.getPreLogIndex() + 1);
                    if (logEntry != null) {
                        if (logEntry.getTerm() != result.getEntries().getFirst().getTerm()) {
                            node.getLog().removeFromIndex(result.getPreLogIndex() + 1);
                        } else {
                            result.setSuccess(true);
                            return result.response(request);
                        }
                    }

                    try {
                        for (LogEntry log : result.getEntries()) {
                            node.getLog().write(log);
                            LogEntry snapshot = node.getSnapshot().get(log.getKv().getObject1());
                            if (snapshot != null) {
                                continue;
                            }
                            node.getSnapshot().apply(log);
                        }

                        if (result.getLeaderCommitIndex() > node.getMetadata().getCommitIndex()) {
                            long commitIndex = Math.min(result.getLeaderCommitIndex(), node.getLog().getLastIndex());
                            node.getMetadata().setCommitIndex(commitIndex);
                            result.setCommitIndex(commitIndex);
                        }

                        result.setSuccess(true);
                        return result.response(request);
                    } catch (Exception e) {
                        logger.error(e);
                    }

                    result.setSuccess(false);
                    return result.response(request);
                }

                if (result.getType() == EntryMessage.Type.SNAPSHOT) {
                    LogEntry logEntry = result.getEntries().getFirst();
                    if (logEntry != null) {
                        boolean flag = node.getSnapshot().apply(logEntry);
                        node.getSnapshotMap().put(node.getNodeId(), true);
                        result.setSnapshotSuccess(flag);

                        result.setSuccess(true);
                        return result.response(request);
                    }
                }
            }

            EntryMessage result = EntryMessage.getInstance();
            result.setSuccess(false);
            return result.response(request);
        }finally {
            node.getEntryLock().unlock();
        }
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}
