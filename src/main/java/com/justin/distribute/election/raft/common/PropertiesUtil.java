package com.justin.distribute.election.raft.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public class PropertiesUtil {

    public static final PropertiesUtil getInstance() {
        return new PropertiesUtil();
    }

    public static Properties getProParms() {
        return PropertiesUtil.getInstance().getProParms("/raft/node.properties");
    }

    public static Map<Integer, String> getNodesAddress() {
        Map<Integer, String> map = new HashMap<Integer, String>();
        String[] nodesAddr =  getClusterNodesAddress().split(",");
        for (String nodeAddr : nodesAddr) {
            int i = nodeAddr.lastIndexOf(":");
            String addr = nodeAddr.substring(0, i);
            int id = Integer.parseInt(nodeAddr.substring(i+1));
            map.put(id, addr);
        }
        return map;
    }

    public static String getClusterNodesAddress() {
        return getProParms().getProperty("cluster.nodes.address");
    }

    public static String getRaftNodesAddress() {
        return PropertiesUtil.getInstance().getProParms("/raft/client.properties").getProperty("raft.nodes.address");
    }

    public static String getLocalAddress() { return getProParms().getProperty("node.local.address"); }

    public static Integer getLocalNodeId() {
        return Integer.parseInt(getProParms().getProperty("node.local.id"));
    }

    public static String getDBDir() {
        String[] addr = System.getProperty("address").split(":", -1);
        return getProParms().getProperty("node.db.dir") + addr[1];
    }

    public static String getSnapshoteDir() {
        return getDBDir() + "/snapshot";
    }

    public static String getLogDir() {
        return getDBDir() + "/log";
    }

    public static String getDataTransType() {
        return getProParms().getProperty("node.data.trans");
    }

    public static String getClientTransType() {
        return PropertiesUtil.getInstance().getProParms("/raft/client.properties").getProperty("node.data.trans");
    }

    private Properties getProParms(String propertiesName) {
        InputStream is = getClass().getResourceAsStream(propertiesName);
        Properties prop = new Properties();
        try {
            prop.load(is);
        } catch (IOException e1) {
            e1.printStackTrace();
        }finally {
            if(is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return prop;
    }
}
