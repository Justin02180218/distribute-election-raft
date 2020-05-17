package com.justin.distribute.election.raft.message;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public class VoteMessage extends AbstractMessage<VoteMessage> {
    private int candidateId;
    private Boolean voteGranted;

    private VoteMessage(){}

    public static VoteMessage getInstance() {
        return new VoteMessage();
    }

    @Override
    public int getMessageType() {
        return MessageType.VOTE;
    }

    public int getCandidateId() {
        return candidateId;
    }

    public void setCandidateId(int candidateId) {
        this.candidateId = candidateId;
    }

    public Boolean getVoteGranted() {
        return voteGranted;
    }

    public void setVoteGranted(Boolean voteGranted) {
        this.voteGranted = voteGranted;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("VoteMessage [");
        sb.append(super.toString());
        sb.append(" candidateId: " + candidateId);
        sb.append(" voteGranted: " + voteGranted);
        sb.append("]");
        return sb.toString();
    }
}
