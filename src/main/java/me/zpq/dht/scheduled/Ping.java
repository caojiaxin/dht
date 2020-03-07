package me.zpq.dht.scheduled;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.socket.DatagramPacket;
import me.zpq.dht.protocol.DhtProtocol;
import me.zpq.dht.model.NodeTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author zpq
 * @date 2019-08-29
 */
public class Ping implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(Ping.class);

    private Channel channel;

    private Map<String, NodeTable> table;

    private byte[] transactionId;

    private byte[] nodeId;

    public Ping(Channel channel, byte[] transactionId, byte[] nodeId, Map<String, NodeTable> table) {

        this.channel = channel;
        this.table = table;
        this.transactionId = transactionId;
        this.nodeId = nodeId;
    }

    @Override
    public void run() {

        List<NodeTable> nodeTables = new ArrayList<>(table.values());

        try {

            byte[] pingQuery = DhtProtocol.pingQuery(transactionId, nodeId);

            nodeTables.forEach(nodeTable -> channel.writeAndFlush(
                    new DatagramPacket(Unpooled.copiedBuffer(pingQuery),
                            new InetSocketAddress(nodeTable.getIp(), nodeTable.getPort())
                    )));

        } catch (IOException e) {

            log.error("ping", e);
        }
    }
}
