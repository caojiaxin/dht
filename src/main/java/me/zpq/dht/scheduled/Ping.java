package me.zpq.dht.scheduled;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.socket.DatagramPacket;
import me.zpq.dht.protocol.DhtProtocol;
import me.zpq.dht.model.NodeTable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Set;

/**
 * @author zpq
 * @date 2019-08-29
 */
public class Ping implements Runnable {

    private Channel channel;

    private Set<NodeTable> nodeTables;

    private byte[] transactionId;

    private byte[] nodeId;

    public Ping(Channel channel, byte[] transactionId, byte[] nodeId, Set<NodeTable> nodeTables) {

        this.channel = channel;
        this.nodeTables = nodeTables;
        this.transactionId = transactionId;
        this.nodeId = nodeId;
    }

    @Override
    public void run() {

        Set<NodeTable> nodeTableSet = Collections.synchronizedSet(nodeTables);
        nodeTableSet.forEach(nodeTable -> {

            try {
                channel.writeAndFlush(new DatagramPacket(Unpooled.copiedBuffer(DhtProtocol.pingQuery(transactionId, nodeId)),
                        new InetSocketAddress(nodeTable.getIp(), nodeTable.getPort())));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }
}
