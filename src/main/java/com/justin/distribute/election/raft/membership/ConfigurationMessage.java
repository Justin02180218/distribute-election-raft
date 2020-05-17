package com.justin.distribute.election.raft.membership;

import java.util.Map;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public class ConfigurationMessage extends MembershipMessage<ConfigurationMessage> {
    private Integer leaderId;
    private Map<Integer, String> members;

    private ConfigurationMessage() {}

    public static ConfigurationMessage getInstance() {
        return new ConfigurationMessage();
    }

    @Override
    protected MembershipMessageType getMembershipMessageType() {
        return MembershipMessageType.CONFIGURATION;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ConfigurationMessage: [");
        sb.append(" leaderId=" + leaderId);
        sb.append(" members=" + members);
        sb.append("]");
        return sb.toString();
    }

    public Integer getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(Integer leaderId) {
        this.leaderId = leaderId;
    }

    public Map<Integer, String> getMembers() {
        return members;
    }

    public void setMembers(Map<Integer, String> members) {
        this.members = members;
    }
}
