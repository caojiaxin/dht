package me.zpq.dht.scheduled;

import me.zpq.dht.model.NodeTable;

import java.util.Iterator;
import java.util.Map;

public class RemoveNode implements Runnable {

    private Map<String, NodeTable> table;

    private int minNodes;

    private long timeout;

    public RemoveNode(Map<String, NodeTable> table, int minNodes ,long timeout) {

        this.table = table;
        this.minNodes = minNodes;
        this.timeout = timeout;
    }

    @Override
    public void run() {

        if (this.table.size() < minNodes) {

            return;
        }
        Iterator<Map.Entry<String, NodeTable>> iterator = table.entrySet().iterator();
        while (iterator.hasNext()) {

            Map.Entry<String, NodeTable> next = iterator.next();
            long diff = System.currentTimeMillis() - next.getValue().getTime();
            if (diff > timeout) {

                iterator.remove();
            }
        }
    }
}
