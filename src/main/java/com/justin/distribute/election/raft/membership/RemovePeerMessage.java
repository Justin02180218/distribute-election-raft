package com.justin.distribute.election.raft.membership;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public class RemovePeerMessage extends MembershipMessage<RemovePeerMessage> {
    private Integer peerId;

    private Boolean success;

    private RemovePeerMessage() {}

    public static RemovePeerMessage getInstance() {
        return new RemovePeerMessage();
    }

    @Override
    protected MembershipMessageType getMembershipMessageType() {
        return MembershipMessageType.REMOVE;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("RemovePeerMessage: [");
        sb.append(" peerId=" + peerId);
        sb.append(" success=" + success);
        sb.append("]");
        return sb.toString();
    }

    public Integer getPeerId() {
        return peerId;
    }

    public void setPeerId(Integer peerId) {
        this.peerId = peerId;
    }

    public Boolean getSuccess() {
        return success;
    }

    public void setSuccess(Boolean success) {
        this.success = success;
    }
}
