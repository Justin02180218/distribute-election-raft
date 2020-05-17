package com.justin.distribute.election.raft;

import java.util.concurrent.atomic.AtomicLong;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public class NodeMetadata {
    private final int nodeId;
    private final String nodeAddress;
    private Boolean voteGrant;
    private final AtomicLong currentTerm = new AtomicLong(0);
    private volatile long commitIndex;
    private String snapshotKey;

    public NodeMetadata(final int nodeId, final String nodeAddress, final Boolean voteGrant) {
        this.nodeId = nodeId;
        this.nodeAddress = nodeAddress;
        this.voteGrant = voteGrant;
        this.commitIndex = -1;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[Node Metadata: nodeId=" + nodeId);
        sb.append(", nodeAddr=" + nodeAddress);
        sb.append(", voteGrant=" + voteGrant);
        sb.append(", term=" + currentTerm);
        sb.append(", commitIndex=" + commitIndex);
        sb.append(", snapshotKey=" + snapshotKey);
        sb.append("]");
        return sb.toString();
    }

    public int getNodeId() {
        return nodeId;
    }

    public String getNodeAddress() {
        return nodeAddress;
    }

    public Boolean getVoteGrant() {
        return voteGrant;
    }

    public void setVoteGrant(Boolean voteGrant) {
        this.voteGrant = voteGrant;
    }

    public AtomicLong getCurrentTerm() {
        return currentTerm;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
    }

    public String getSnapshotKey() {
        return snapshotKey;
    }

    public void setSnapshotKey(String snapshotKey) {
        this.snapshotKey = snapshotKey;
    }
}
