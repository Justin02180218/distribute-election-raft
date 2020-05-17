package com.justin.distribute.election.raft.membership;

import com.justin.distribute.election.raft.message.MessageType;
import com.justin.net.remoting.protocol.JSONSerializable;
import com.justin.net.remoting.protocol.RemotingMessage;
import com.justin.net.remoting.protocol.RemotingMessageHeader;

import java.util.HashMap;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public abstract class MembershipMessage<T> {
    public static final String MEMBERSHIP_MESSAGE_TYPE = "membership_message_type";

    public RemotingMessage request() {
        byte[] body = JSONSerializable.encode(this);

        RemotingMessageHeader header = new RemotingMessageHeader();
        header.setCode(MessageType.MEMBERSHIP);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put(MEMBERSHIP_MESSAGE_TYPE, getMembershipMessageType().toString());
        header.setExtFields(extFields);

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

    protected abstract MembershipMessageType getMembershipMessageType();
}
