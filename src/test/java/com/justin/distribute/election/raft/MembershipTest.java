package com.justin.distribute.election.raft;

import com.justin.distribute.election.raft.membership.MembershipAdmin;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public class MembershipTest {
    private MembershipAdmin membershipAdmin;
    int peerId = 5;
    String peerAddr = "127.0.0.1:4005";

    @Before
    public void setUp() {
        membershipAdmin = MembershipAdmin.getInstance();
    }

    @After
    public void tearDown() {
//        membershipAdmin.shutdown();
    }

    @Test
    public void testGetConfiguration() {
        Map<Integer, String> conf = membershipAdmin.getConfiguration();
        System.out.println(conf);
    }


    @Test
    public void testAddPeer() {
        boolean success = membershipAdmin.addPeer(peerId, peerAddr);
        System.out.println("Add peer: " + success);
    }

    @Test
    public void testRemovePeer() {
        boolean success = membershipAdmin.removePeer(peerId);
        System.out.println("Remove peer: " + success);
    }
}
