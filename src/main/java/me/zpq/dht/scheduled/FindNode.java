package me.zpq.dht.scheduled;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.socket.DatagramPacket;
import me.zpq.dht.model.BootstrapAddress;
import me.zpq.dht.protocol.DhtProtocol;
import me.zpq.dht.util.Utils;
import me.zpq.dht.model.NodeTable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;

public class FindNode implements Runnable {

    private Channel channel;

    private byte[] transactionId;

    private byte[] nodeId;

    private Set<NodeTable> nodeTables;

    private int minNodes;

    private List<BootstrapAddress> list = new ArrayList<>();

    public FindNode(Channel channel, byte[] transactionId, byte[] nodeId, Set<NodeTable> nodeTables, int minNodes) {
        this.channel = channel;
        this.transactionId = transactionId;
        this.nodeId = nodeId;
        this.nodeTables = nodeTables;
        this.minNodes = minNodes;
        list.add(new BootstrapAddress("router.bittorrent.com", 6881));
        list.add(new BootstrapAddress("router.utorrent.com", 6881));
        list.add(new BootstrapAddress("dht.transmissionbt.com", 6881));
    }

    @Override
    public void run() {

        Set<NodeTable> nodeTableSet = Collections.synchronizedSet(nodeTables);
        if (nodeTableSet.size() >= minNodes) {

            return;
        }

        try {

            final byte[] findNodeQuery = DhtProtocol.findNodeQuery(transactionId, nodeId, Utils.nodeId());
            list.forEach(bootstrapAddress -> channel.writeAndFlush(new DatagramPacket(Unpooled.copiedBuffer(findNodeQuery),
                    new InetSocketAddress(bootstrapAddress.getHost(), bootstrapAddress.getPort()))));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
