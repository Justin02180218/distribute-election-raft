package com.justin.distribute.election.raft;

import com.justin.distribute.election.raft.callback.EntryRequestCallback;
import com.justin.distribute.election.raft.callback.VoteRequestCallback;
import com.justin.distribute.election.raft.common.PropertiesUtil;
import com.justin.distribute.election.raft.log.Log;
import com.justin.distribute.election.raft.log.LogEntry;
import com.justin.distribute.election.raft.log.Snapshot;
import com.justin.distribute.election.raft.message.*;
import com.justin.distribute.election.raft.processor.*;
import com.justin.net.remoting.RemotingClient;
import com.justin.net.remoting.RemotingServer;
import com.justin.net.remoting.netty.NettyRemotingClient;
import com.justin.net.remoting.netty.NettyRemotingServer;
import com.justin.net.remoting.netty.conf.NettyClientConfig;
import com.justin.net.remoting.netty.conf.NettyServerConfig;
import com.justin.net.remoting.protocol.RemotingMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public class Node {
    private static final Logger logger = LogManager.getLogger(Node.class.getSimpleName());

    private final ConcurrentMap<Integer, NodeMetadata> cluster = new ConcurrentHashMap<Integer, NodeMetadata>();
    private final ConcurrentMap<Integer, Boolean> snapshotMap = new ConcurrentHashMap<>();
    private final AtomicReference<NodeStatus> status = new AtomicReference<NodeStatus>(NodeStatus.FOLLOWER);
    private final Lock voteLock = new ReentrantLock();
    private final Lock entryLock = new ReentrantLock();
    private final Lock clientLock = new ReentrantLock();
    private final Lock membershipLock = new ReentrantLock();
    private final ExecutorService executorService;
    private final ScheduledExecutorService scheduledExecutorService;
    private final NodeConfig nodeConfig;
    private final NodeMetadata metadata;
    private final String localAddr;
    private final Snapshot snapshot;
    private final Log log;

    private final CountDownLatch syncLatch = new CountDownLatch(1);
    private final CountDownLatch hsyncLatch = new CountDownLatch(1);

    private volatile boolean running = false;

    private RemotingServer server;
    private RemotingClient client;

    private volatile int nodeId;
    private volatile int voteFor = -1;
    private volatile int leaderId = -1;

    public Node(final NodeConfig nodeConfig) {
        this.nodeConfig = nodeConfig;
        this.localAddr = nodeConfig.getLocalAddr();
        for (Map.Entry<Integer, String> entry : PropertiesUtil.getNodesAddress().entrySet()) {
            cluster.put(entry.getKey(), new NodeMetadata(entry.getKey(), entry.getValue(), false));
            if (nodeConfig.getLocalAddr().equals(entry.getValue())) {
                this.nodeId = entry.getKey();
            }
        }
        if (PropertiesUtil.getLocalNodeId() != null && !PropertiesUtil.getNodesAddress().containsKey(PropertiesUtil.getLocalNodeId())) {
            this.nodeId = PropertiesUtil.getLocalNodeId();
            cluster.put(nodeId, new NodeMetadata(nodeId, PropertiesUtil.getLocalAddress(), false));
        }
        this.metadata = cluster.get(nodeId);
        this.executorService = new ThreadPoolExecutor(nodeConfig.getCup(),
                nodeConfig.getMaxPoolSize(), nodeConfig.getKeepTime(), TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(nodeConfig.getQueueSize()));
        this.scheduledExecutorService = Executors.newScheduledThreadPool(2);
        this.snapshot = Snapshot.getInstance();
        this.log = Log.getInstance();
    }

    public void start() {
        if (running) {
            return;
        }

        synchronized (this) {
            if (running) {
                return;
            }

            server = new NettyRemotingServer(new NettyServerConfig(this.nodeConfig.getHost(), this.nodeConfig.getPort()));
            server.registerProcessor(MessageType.VOTE, new VoteRequestProcessor(this), executorService);
            server.registerProcessor(MessageType.ENTRIES, new EntryRequestProcessor(this), executorService);
            server.registerProcessor(MessageType.MEMBERS, new MembersRequestProcessor(this), executorService);
            server.registerProcessor(MessageType.MEMBERSHIP, new MembershipRequestProcessor(this), executorService);
            server.registerProcessor(MessageType.CLIENT, new ClientRequestProcessor(this), executorService);
            server.start();

            client = new NettyRemotingClient(new NettyClientConfig());
            client.start();

            LogEntry logEntry = log.read(log.getLastIndex());
            if (logEntry != null) {
                metadata.getCurrentTerm().set(logEntry.getTerm());
                metadata.setCommitIndex(logEntry.getIndex());
            }

            running = true;

            scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    heartbeat();
                }
            }, 0, nodeConfig.getHeartbeatTimeout(), TimeUnit.MILLISECONDS);

            scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    election();
                }
            }, 6000, 500, TimeUnit.MILLISECONDS);

            scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    members();
                }
            }, 3000, nodeConfig.getMembersTimeout(), TimeUnit.MILLISECONDS);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                shutdown();
            }
        });
    }

    private void shutdown() {
        synchronized (this) {
            if (server != null) {
                server.shutdown();
            }
            if (client != null) {
                client.shutdown();
            }
            if (executorService != null) {
                executorService.shutdown();
            }
            if (scheduledExecutorService != null) {
                scheduledExecutorService.shutdown();
            }
            if (snapshot != null) {
                snapshot.close();
            }
            if (log != null) {
                log.close();
            }
            running = false;
        }
    }

    private void heartbeat() {
        if (status.get() != NodeStatus.LEADER) {
            return;
        }

        if (!nodeConfig.resetHeartbeatTick()) {
            return;
        }

        long currentTerm = metadata.getCurrentTerm().get();

        for (Map.Entry<Integer, NodeMetadata> entry : cluster.entrySet()) {
            if (nodeId == entry.getKey()) {
                continue;
            }

            NodeMetadata peer = entry.getValue();
            LogEntry logEntry = null;
            if (metadata.getCommitIndex() > peer.getCommitIndex()) {
                logEntry = log.getLastLogEntry();
            }
            EntryMessage heartbeat = committedEntryMessage(EntryMessage.Type.HEARTBEAT, logEntry, peer);

            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        logger.info("Heartbeat request:{} to {}", heartbeat, peer.getNodeId());
                        RemotingMessage response = client.invokeSync(peer.getNodeAddress(), heartbeat.request(), 3 * 1000);
                        EntryMessage resEntryMsg = EntryMessage.getInstance().parseMessage(response);
                        logger.info("Heartbeat response:{} from {}", resEntryMsg, resEntryMsg.getNodeId());
                        peer.setCommitIndex(resEntryMsg.getCommitIndex());

                        if (resEntryMsg.getTerm() > currentTerm) {
                            comedown(resEntryMsg.getTerm());
                        }
                    } catch (Exception e) {
                        logger.error(e);
                    }
                }
            });
        }
    }

    public void election() {
        if (status.get() == NodeStatus.LEADER) {
            return;
        }

        if (!nodeConfig.resetElectionTick()) {
            return;
        }

        setStatus(NodeStatus.CANDIDATE);
        logger.info("Node {} {} become CANDIDATE, current term {}", nodeId, localAddr, metadata.getCurrentTerm());

        metadata.getCurrentTerm().incrementAndGet();
        voteFor = nodeId;

        VoteMessage voteMsg = VoteMessage.getInstance();
        voteMsg.setNodeId(nodeId);
        voteMsg.setTerm(metadata.getCurrentTerm().get());
        voteMsg.setCandidateId(nodeId);

        for (Map.Entry<Integer, NodeMetadata> entry : cluster.entrySet()) {
            if (nodeId == entry.getKey()) {
                continue;
            }
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        logger.info("Vote request {} to {}", voteMsg, entry.getValue().getNodeAddress());
                        client.invokeAsync(entry.getValue().getNodeAddress(), voteMsg.request(), 3*1000,
                                new VoteRequestCallback(Node.this));
                    } catch (Exception e) {
                        logger.error(e);
                    }
                }
            });
        }
    }

    private void members() {
        if (status.get() != NodeStatus.LEADER) {
            return;
        }

        if (!nodeConfig.resetMembersTick()) {
            return;
        }

        List<NodeMetadata> peers = new ArrayList<>();
        MembersMessage membersMsg = MembersMessage.getInstance();
        for (NodeMetadata peer : cluster.values()) {
            peers.add(peer);
        }
        membersMsg.setPeers(peers);

        for (Map.Entry<Integer, NodeMetadata> entry : cluster.entrySet()) {
            if (nodeId == entry.getKey()) {
                continue;
            }

            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        logger.info("Members request {} to {}", membersMsg, entry.getValue().getNodeAddress());
                        client.invokeOneway(entry.getValue().getNodeAddress(), membersMsg.request(), 3000);
                    } catch (Exception e) {
                        logger.error(e);
                    }
                }
            });
        }
    }

    public void becomeLeader() {
        status.set(NodeStatus.LEADER);
        leaderId = getNodeId();
        voteFor = -1;

        System.out.println("------=======---------========>" + metadata);
    }

    public void comedown(long term) {
        long currentTerm = metadata.getCurrentTerm().get();
        if (currentTerm > term) {
            return;
        }

        if (currentTerm < term) {
            metadata.getCurrentTerm().set(term);
            leaderId = -1;
            voteFor = -1;
        }
        setStatus(NodeStatus.FOLLOWER);
    }

    public RemotingMessage redirect(RemotingMessage request) {
        RemotingMessage response = null;
        try {
            response = client.invokeSync(cluster.get(leaderId).getNodeAddress(), request, 3*1000);
        } catch (Exception e) {
            logger.error(e);
        }
        return response;
    }

    public void commitEntries(final LogEntry logEntry) {
        log.write(logEntry);
        metadata.setCommitIndex(logEntry.getIndex());

        for (Map.Entry<Integer, NodeMetadata> entry : cluster.entrySet()) {
            Integer key = entry.getKey();
            NodeMetadata peer = entry.getValue();

            if (key == nodeId) {
                continue;
            }

            EntryMessage entryMessage = committedEntryMessage(EntryMessage.Type.COMMIT, logEntry, peer);

            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        client.invokeAsync(peer.getNodeAddress(), entryMessage.request(), 3000,
                                new EntryRequestCallback(Node.this));
                    } catch (Exception e) {
                        logger.error(e);
                    }
                }
            });
        }
    }

    public void snapshotEntries(final LogEntry logEntry, final boolean isSync) {
        for (Map.Entry<Integer, NodeMetadata> entry : cluster.entrySet()) {
            NodeMetadata peer = entry.getValue();

            if (entry.getKey() == nodeId) {
                continue;
            }

            LinkedList<LogEntry> entries = new LinkedList<>();
            entries.add(logEntry);

            EntryMessage entryMessage = EntryMessage.getInstance();
            entryMessage.setType(EntryMessage.Type.SNAPSHOT);
            entryMessage.setNodeId(nodeId);
            entryMessage.setTerm(metadata.getCurrentTerm().get());
            entryMessage.setLeaderId(nodeId);
            entryMessage.setEntries(entries);

            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        RemotingMessage response = client.invokeSync(peer.getNodeAddress(), entryMessage.request(), 3000);
                        EntryMessage resEntryMsg = EntryMessage.getInstance().parseMessage(response);
                        snapshotMap.put(resEntryMsg.getNodeId(), resEntryMsg.getSnapshotSuccess());

                        int counter = 0;
                        for (Boolean flag : snapshotMap.values()) {
                            if (flag) {
                                counter += 1;
                            }
                        }
                        if (!isSync && counter > cluster.size()/2) {
                            hsyncLatch.countDown();
                        }
                        if (isSync && counter == cluster.size()) {
                            syncLatch.countDown();
                        }
                    } catch (Exception e) {
                        logger.error(e);
                    }
                }
            });
        }
    }

    private EntryMessage committedEntryMessage(final EntryMessage.Type type, final LogEntry logEntry, final NodeMetadata peer) {
        EntryMessage entryMessage = EntryMessage.getInstance();
        entryMessage.setNodeId(nodeId);
        entryMessage.setTerm(metadata.getCurrentTerm().get());
        entryMessage.setLeaderId(nodeId);
        entryMessage.setLeaderCommitIndex(metadata.getCommitIndex());
        entryMessage.setType(type);

        if (logEntry != null) {
            long nextIndex = peer.getCommitIndex() + 1;
            LinkedList<LogEntry> logEntryList = new LinkedList<>();
            for (long i = nextIndex; i <= logEntry.getIndex(); i++) {
                LogEntry l = log.read(i);
                if (l != null) {
                    logEntryList.add(l);
                }
            }

            entryMessage.setPreLogTerm(logEntryList.getFirst().getTerm());
            entryMessage.setPreLogIndex(logEntryList.getFirst().getIndex());
            entryMessage.setEntries(logEntryList);
        }

        return entryMessage;
    }

    public Lock getVoteLock() {
        return voteLock;
    }

    public Lock getEntryLock() {
        return entryLock;
    }

    public Lock getClientLock() {
        return clientLock;
    }

    public Lock getMembershipLock() {
        return membershipLock;
    }

    public NodeMetadata getMetadata() {
        return metadata;
    }

    public int getNodeId() {
        return nodeId;
    }

    public String getLocalAddr() {
        return localAddr;
    }

    public ConcurrentMap<Integer, NodeMetadata> getCluster() {
        return cluster;
    }

    public NodeStatus getStatus() {
        return status.get();
    }

    public void setStatus(final NodeStatus nodeStatus) {
        status.compareAndSet(getStatus(), nodeStatus);
    }

    public int getVoteFor() {
        return voteFor;
    }

    public void setVoteFor(int voteFor) {
        this.voteFor = voteFor;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(int leaderId) {
        this.leaderId = leaderId;
    }

    public NodeConfig getNodeConfig() {
        return nodeConfig;
    }

    public Snapshot getSnapshot() {
        return snapshot;
    }

    public Log getLog() {
        return log;
    }

    public ConcurrentMap<Integer, Boolean> getSnapshotMap() {
        return snapshotMap;
    }

    public CountDownLatch getSyncLatch() {
        return syncLatch;
    }

    public CountDownLatch getHsyncLatch() {
        return hsyncLatch;
    }
}
