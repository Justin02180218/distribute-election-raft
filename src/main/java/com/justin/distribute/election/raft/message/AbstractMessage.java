package com.justin.distribute.election.raft.message;

import com.justin.net.remoting.protocol.JSONSerializable;
import com.justin.net.remoting.protocol.RemotingMessage;
import com.justin.net.remoting.protocol.RemotingMessageHeader;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public abstract class AbstractMessage<T> {
    protected long term;
    protected int nodeId;

    public RemotingMessage request() {
        RemotingMessageHeader header = new RemotingMessageHeader();
        header.setCode(getMessageType());

        byte[] body = JSONSerializable.encode(this);
        RemotingMessage remotingMessage = new RemotingMessage(header, body);
        return remotingMessage;
    }

    public RemotingMessage response(final RemotingMessage request) {
        byte[] body = JSONSerializable.encode(this);

        RemotingMessage remotingMessage = new RemotingMessage(request.getMessageHeader(), body);
        return remotingMessage;
    }

    public T parseMessage(final RemotingMessage remotingMessage) {
        return (T) JSONSerializable.decode(remotingMessage.getMessageBody(), this.getClass());
    }

    public abstract int getMessageType();

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public String toString() {
        return " nodeId: " + nodeId + ", term: " + term;
    }
}
