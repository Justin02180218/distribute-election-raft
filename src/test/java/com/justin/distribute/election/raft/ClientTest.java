package com.justin.distribute.election.raft;

import com.justin.distribute.election.raft.client.Client;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public class ClientTest {
    private Client client;

    @Before
    public void setUp() {
        client = Client.getInstance();
    }

    @After
    public void tearDown() {
//        Client.shutdown();
    }

    @Test
    public void testKVPut() {
        String key = "hello_half_sync";
        String value = "world_half_sync";
        boolean flag = client.put(key, value);
        System.out.println("Put-------====>flag: " + flag);
    }

    @Test
    public void testKVGet() {
        String key = "hello_half_sync";
        String value = client.get(key);
        System.out.println("Get-------====>value: " + value);
    }
}
