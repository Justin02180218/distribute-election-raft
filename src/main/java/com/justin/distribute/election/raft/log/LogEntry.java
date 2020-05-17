package com.justin.distribute.election.raft.log;


import com.justin.net.remoting.common.Pair;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public class LogEntry implements Comparable<LogEntry> {
    private Long index;
    private long term;
    private Pair<String, String> kv;

    public LogEntry() {}

    public LogEntry(long term, Pair<String, String> kv) {
        this.term = term;
        this.kv = kv;
    }

    public LogEntry(long index, long term, Pair<String, String> kv) {
        this.index = index;
        this.term = term;
        this.kv = kv;
    }

    @Override
    public int compareTo(LogEntry o) {
        if (null == o) {
            return -1;
        }
        if (this.getIndex() > o.getIndex()) {
            return 1;
        }
        return -1;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("LogEntry [");
        sb.append(" index: " + index);
        sb.append(" term: " + term);
        sb.append(" kv: " + kv.getObject1() + ":" + kv.getObject2());
        sb.append("] ");
        return sb.toString();
    }

    public Long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public Pair<String, String> getKv() {
        return kv;
    }

    public void setKv(Pair<String, String> kv) {
        this.kv = kv;
    }
}
