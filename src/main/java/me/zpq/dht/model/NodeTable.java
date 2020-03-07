package me.zpq.dht.model;

import java.util.Objects;

/**
 * @author zpq
 * @date 2019-08-26
 */
public class NodeTable {

    private String nid;

    private String ip;

    private int port;

    private long time;

    public NodeTable() {
    }

    public NodeTable(String nid, String ip, int port, long time) {
        this.nid = nid;
        this.ip = ip;
        this.port = port;
        this.time = time;
    }

    public String getNid() {
        return nid;
    }

    public void setNid(String nid) {
        this.nid = nid;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NodeTable nodeTable = (NodeTable) o;
        return port == nodeTable.port &&
                time == nodeTable.time &&
                nid.equals(nodeTable.nid) &&
                ip.equals(nodeTable.ip);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nid, ip, port, time);
    }
}
