package com.justin.distribute.election.raft.message;

import com.justin.distribute.election.raft.NodeMetadata;

import java.util.List;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public class MembersMessage extends AbstractMessage<MembersMessage> {
    private List<NodeMetadata> peers;

    private MembersMessage() {}

    public static MembersMessage getInstance() {
        return new MembersMessage();
    }

    @Override
    public int getMessageType() {
        return MessageType.MEMBERS;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Members: [");
        sb.append(" peers: " + peers);
        sb.append("]");
        return sb.toString();
    }

    public List<NodeMetadata> getPeers() {
        return peers;
    }

    public void setPeers(List<NodeMetadata> peers) {
        this.peers = peers;
    }
}
