package me.zpq.dht.scheduled;

import me.zpq.dht.model.NodeTable;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

public class RemoveNode implements Runnable {

    private Set<NodeTable> nodeTables;

    private long timeout;

    public RemoveNode(Set<NodeTable> nodeTables, long timeout) {

        this.nodeTables = nodeTables;
        this.timeout = timeout;
    }

    @Override
    public void run() {

        Set<NodeTable> nodeTableSet = Collections.synchronizedSet(nodeTables);

        Iterator<NodeTable> iterator = nodeTableSet.iterator();

        while (iterator.hasNext()) {

            NodeTable next = iterator.next();
            long diff = System.currentTimeMillis() - next.getTime();
            if (diff > timeout) {

                iterator.remove();
            }
        }
    }
}
