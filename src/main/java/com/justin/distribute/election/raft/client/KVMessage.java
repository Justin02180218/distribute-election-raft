package com.justin.distribute.election.raft.client;

import com.justin.distribute.election.raft.message.AbstractMessage;
import com.justin.distribute.election.raft.message.MessageType;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public class KVMessage extends AbstractMessage<KVMessage> {
    public enum TransType {
        SYNC,
        ASYNC,
        HALF_SYNC,
    }

    public enum KVType {
        PUT,
        GET,
    }

    private String key;
    private String value;
    private TransType transType;
    private KVType kvType;

    private Boolean success;

    private KVMessage() {}

    public static KVMessage getInstance() {
        return new KVMessage();
    }

    @Override
    public int getMessageType() {
        return MessageType.CLIENT;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("KVMessage [");
        sb.append(" key: " + key);
        sb.append(" value: " + value);
        sb.append(" transType: " + transType);
        sb.append(" kvType: " + kvType);
        sb.append(" success: " + success);
        sb.append(" ]");
        return sb.toString();
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public TransType getTransType() {
        return transType;
    }

    public void setTransType(TransType transType) {
        this.transType = transType;
    }

    public KVType getKvType() {
        return kvType;
    }

    public void setKvType(KVType kvType) {
        this.kvType = kvType;
    }

    public Boolean getSuccess() {
        return success;
    }

    public void setSuccess(Boolean success) {
        this.success = success;
    }
}
