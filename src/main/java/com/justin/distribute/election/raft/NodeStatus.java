package com.justin.distribute.election.raft;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public enum NodeStatus {
    FOLLOWER,
    PRE_CANDIDATE,
    CANDIDATE,
    LEADER
}
