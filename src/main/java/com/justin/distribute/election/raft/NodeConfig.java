package com.justin.distribute.election.raft;

import com.justin.distribute.election.raft.common.PropertiesUtil;

import java.util.concurrent.ThreadLocalRandom;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public class NodeConfig {
    private int cup = Runtime.getRuntime().availableProcessors();
    private int maxPoolSize = cup * 2;
    private int queueSize = 1024;
    private long keepTime = 60 * 1000;

    private int electionTimeout = 15 * 1000;
    private int heartbeatTimeout = 5 * 1000;
    private int membersTimeout = 30 * 1000;

    private volatile long preHeartbeatTime = 0;
    private volatile long preElectionTime = 0;
    private volatile long preMembersTime = 0;

    private final String host;
    private final int port;

    private String transType = PropertiesUtil.getDataTransType();

    public NodeConfig(final String host, final int port) {
        this.host = host;
        this.port = port;
    }

    public boolean resetElectionTick() {
        long current = System.currentTimeMillis();
        electionTimeout = electionTimeout + ThreadLocalRandom.current().nextInt(50);
        if (current - preElectionTime < electionTimeout) {
            return false;
        }
        preElectionTime = System.currentTimeMillis() + ThreadLocalRandom.current().nextInt(200) + 150;
        return true;
    }

    public boolean resetHeartbeatTick() {
        long current = System.currentTimeMillis();
        if (current - preHeartbeatTime < heartbeatTimeout) {
            return false;
        }
        preHeartbeatTime = System.currentTimeMillis();
        return true;
    }

    public boolean resetMembersTick() {
        long current = System.currentTimeMillis();
        if (current - preMembersTime < membersTimeout) {
            return false;
        }else {
            preMembersTime = System.currentTimeMillis();
            return true;
        }
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getLocalAddr() {
        return host + ":" + port;
    }

    public int getHeartbeatTimeout() {
        return heartbeatTimeout;
    }

    public void setHeartbeatTimeout(int heartbeatTimeout) {
        this.heartbeatTimeout = heartbeatTimeout;
    }

    public int getCup() {
        return cup;
    }

    public void setCup(int cup) {
        this.cup = cup;
    }

    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    public void setMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }

    public int getQueueSize() {
        return queueSize;
    }

    public void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
    }

    public long getKeepTime() {
        return keepTime;
    }

    public void setKeepTime(long keepTime) {
        this.keepTime = keepTime;
    }

    public long getPreHeartbeatTime() {
        return preHeartbeatTime;
    }

    public void setPreHeartbeatTime(long preHeartbeatTime) {
        this.preHeartbeatTime = preHeartbeatTime;
    }

    public long getPreElectionTime() {
        return preElectionTime;
    }

    public void setPreElectionTime(long preElectionTime) {
        this.preElectionTime = preElectionTime;
    }

    public int getMembersTimeout() {
        return membersTimeout;
    }

    public void setMembersTimeout(int membersTimeout) {
        this.membersTimeout = membersTimeout;
    }

    public String getTransType() {
        return transType;
    }

    public void setTransType(String transType) {
        this.transType = transType;
    }
}
