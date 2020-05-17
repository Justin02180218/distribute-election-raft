package com.justin.distribute.election.raft.message;

import com.justin.distribute.election.raft.log.LogEntry;

import java.util.LinkedList;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public class EntryMessage extends AbstractMessage<EntryMessage> {
    public enum Type {
        HEARTBEAT, SNAPSHOT, COMMIT
    }

    private int leaderId;
    private long preLogIndex;
    private long preLogTerm;
    private LinkedList<LogEntry> entries = new LinkedList<>();
    private long leaderCommitIndex;
    private long commitIndex;
    private Type type;
    private Boolean snapshotSuccess;

    private Boolean success;

    private EntryMessage() {}

    public static EntryMessage getInstance() {
        return new EntryMessage();
    }

    @Override
    public int getMessageType() {
        return MessageType.ENTRIES;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("EntryMessage [");
        sb.append(super.toString());
        sb.append(" leaderId: " + leaderId);
        sb.append(" preLogIndex: " + preLogIndex);
        sb.append(" preLogTerm: " + preLogTerm);
        sb.append(" leaderCommitIndex: " + leaderCommitIndex);
        sb.append(" type: " + type);
        for (LogEntry logEntry : entries) {
            sb.append(" " + logEntry.toString());
        }
        sb.append(" success: " + success);
        sb.append("]");
        return sb.toString();
    }

    public int getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(int leaderId) {
        this.leaderId = leaderId;
    }

    public Boolean getSuccess() {
        return success;
    }

    public void setSuccess(Boolean success) {
        this.success = success;
    }

    public long getPreLogIndex() {
        return preLogIndex;
    }

    public void setPreLogIndex(long preLogIndex) {
        this.preLogIndex = preLogIndex;
    }

    public long getPreLogTerm() {
        return preLogTerm;
    }

    public void setPreLogTerm(long preLogTerm) {
        this.preLogTerm = preLogTerm;
    }

    public LinkedList<LogEntry> getEntries() {
        return entries;
    }

    public void setEntries(LinkedList<LogEntry> entries) {
        this.entries = entries;
    }

    public long getLeaderCommitIndex() {
        return leaderCommitIndex;
    }

    public void setLeaderCommitIndex(long leaderCommitIndex) {
        this.leaderCommitIndex = leaderCommitIndex;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public Boolean getSnapshotSuccess() {
        return snapshotSuccess;
    }

    public void setSnapshotSuccess(Boolean snapshotSuccess) {
        this.snapshotSuccess = snapshotSuccess;
    }
}
