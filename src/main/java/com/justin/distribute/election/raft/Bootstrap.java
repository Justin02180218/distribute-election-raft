package com.justin.distribute.election.raft;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public class Bootstrap {
    public static void main(String[] args) {
        String localAddr = System.getProperty("address");
        if (localAddr == null || localAddr.equals("")) {
            System.exit(-1);
        }

        String[] addr = localAddr.split(":", -1);
        String host = addr[0];
        int port = Integer.parseInt(addr[1]);

        Node node = new Node(new NodeConfig(host, port));
        node.start();
    }
}
