package com.justin.distribute.election.raft.membership;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public class AddPeerMessage extends MembershipMessage<AddPeerMessage> {
    private Integer peerId;
    private String address;

    private Boolean success;

    private AddPeerMessage() {}

    private static class AddPeerMessageSingle {
        private static final AddPeerMessage INSTANCE = new AddPeerMessage();
    }

    public static AddPeerMessage getInstance() {
        return AddPeerMessageSingle.INSTANCE;
    }

    @Override
    protected MembershipMessageType getMembershipMessageType() {
        return MembershipMessageType.ADD;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("AddPeerMessage: [");
        sb.append(" peerId=" + peerId);
        sb.append(" address=" + address);
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

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public Boolean getSuccess() {
        return success;
    }

    public void setSuccess(Boolean success) {
        this.success = success;
    }
}
