package me.zpq.dht.scheduled;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.socket.DatagramPacket;
import me.zpq.dht.model.NodeTable;
import me.zpq.dht.protocol.DhtProtocol;
import me.zpq.dht.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GetPeers implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(GetPeers.class);

    private Channel channel;

    private byte[] transactionId;

    private byte[] nodeId;

    private Map<String, NodeTable> tableMap;

    // need test
    public GetPeers(Channel channel, byte[] transactionId, byte[] nodeId, Map<String, NodeTable> tableMap) {

        this.channel = channel;
        this.transactionId = transactionId;
        this.nodeId = nodeId;
        this.tableMap = tableMap;
    }

    @Override
    public void run() {

        List<NodeTable> nodeTables = new ArrayList<>(tableMap.values());
        try {

            byte[] getPeersQuery = DhtProtocol.getPeersQuery(transactionId, nodeId, Utils.nodeId());

            nodeTables.forEach(nodeTable -> channel.writeAndFlush(
                    new DatagramPacket(Unpooled.copiedBuffer(getPeersQuery),
                            new InetSocketAddress(nodeTable.getIp(), nodeTable.getPort())
                    )));
        } catch (IOException e) {

            log.error(e.getMessage(), e);
        }


    }
}
