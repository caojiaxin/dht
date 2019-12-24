package me.zpq.dht.server;

import be.adaxisoft.bencode.BDecoder;
import be.adaxisoft.bencode.BEncodedValue;
import be.adaxisoft.bencode.InvalidBEncodingException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import me.zpq.dht.client.PeerClient;
import me.zpq.dht.JsonMetaInfo;
import me.zpq.dht.protocol.DhtProtocol;
import me.zpq.dht.model.NodeTable;
import me.zpq.dht.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author zpq
 * @date 2019-08-21
 */
public class DiscardServerHandler extends SimpleChannelInboundHandler<DatagramPacket> {

    private DhtProtocol dhtProtocol = new DhtProtocol();

    private static final Logger LOGGER = LoggerFactory.getLogger(DiscardServerHandler.class);

    private byte[] nodeId;

    private Map<String, NodeTable> nodeTable;

    private Integer maxNodes;

    private ThreadPoolExecutor threadPoolExecutor;

    public DiscardServerHandler(Map<String, NodeTable> nodeTable, byte[] nodeId, Integer maxNodes, ThreadPoolExecutor threadPoolExecutor) {

        this.nodeId = nodeId;
        this.nodeTable = nodeTable;
        this.maxNodes = maxNodes;
        this.threadPoolExecutor = threadPoolExecutor;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, DatagramPacket datagramPacket) {

        try {

            ByteBuf content = datagramPacket.copy().content();
            byte[] req = new byte[content.readableBytes()];
            content.readBytes(req);
            content.release();

            BEncodedValue data = BDecoder.decode(new ByteArrayInputStream(req));
            byte[] transactionId = data.getMap().get("t").getBytes();

            switch (data.getMap().get("y").getString()) {

                case "q":

                    Map<String, BEncodedValue> a = data.getMap().get("a").getMap();
                    switch (data.getMap().get("q").getString()) {

                        case "ping":
                            this.queryPing(channelHandlerContext, datagramPacket, transactionId, a);
                            break;
                        case "find_node":
                            this.queryFindNode(channelHandlerContext, datagramPacket, transactionId);
                            break;
                        case "get_peers":
                            this.queryGetPeers(channelHandlerContext, datagramPacket, transactionId, a);
                            break;
                        case "announce_peer":
                            this.queryAnnouncePeer(channelHandlerContext, datagramPacket, transactionId, a);
                            break;
                        default:
//                            this.queryMethodUnknown(channelHandlerContext, datagramPacket, transactionId);
                            break;
                    }

                    break;
                case "r":

                    Map<String, BEncodedValue> r = data.getMap().get("r").getMap();
                    if (r.get("a") != null) {

                        this.responseHasId(r, datagramPacket);
                    }

                    if (r.get("nodes") != null) {

                        this.responseHasNodes(r);
                    }

                    break;

                case "e":

                    this.responseError(data);

                    break;
                default:
                    break;
            }


        } catch (Exception e) {

            // nothing to do
        }

    }

    private void queryPing(ChannelHandlerContext channelHandlerContext, DatagramPacket datagramPacket, byte[] transactionId, Map<String, BEncodedValue> a) throws IOException {

        channelHandlerContext.writeAndFlush(new DatagramPacket(
                Unpooled.copiedBuffer(dhtProtocol.pingResponse(transactionId, nodeId)),
                datagramPacket.sender()));
        String id = a.get("id").getString();
        if (nodeTable.containsKey(id)) {

            NodeTable nodeTable = this.nodeTable.get(id);
            nodeTable.setTime(System.currentTimeMillis());
            this.nodeTable.put(id, nodeTable);
        }
    }

    private void queryFindNode(ChannelHandlerContext channelHandlerContext, DatagramPacket datagramPacket, byte[] transactionId) throws IOException {

        List<NodeTable> table = new ArrayList<>(nodeTable.values());
        channelHandlerContext.writeAndFlush(new DatagramPacket(
                Unpooled.copiedBuffer(
                        dhtProtocol.findNodeResponse(transactionId, nodeId, Utils.nodesEncode(table))),
                datagramPacket.sender()));
    }

    private void queryGetPeers(ChannelHandlerContext channelHandlerContext, DatagramPacket datagramPacket, byte[] transactionId, Map<String, BEncodedValue> a) throws IOException {

        byte[] token = this.getToken(a.get("info_hash").getBytes());
//        List<NodeTable> nodes = new ArrayList<>(nodeTable.values());
        channelHandlerContext.writeAndFlush(new DatagramPacket(
                Unpooled.copiedBuffer(
                        dhtProtocol.getPeersResponseNodes(transactionId, nodeId, token, new byte[0])),
                datagramPacket.sender()));

    }

    private void queryAnnouncePeer(ChannelHandlerContext channelHandlerContext, DatagramPacket datagramPacket, byte[] transactionId, Map<String, BEncodedValue> a) throws IOException {

        // sha1
        byte[] infoHash = a.get("info_hash").getBytes();

        // token
        byte[] needValidatorToken = a.get("token").getBytes();

        if (!this.validatorToken(infoHash, needValidatorToken)) {

            return;
        }
        // ip
        String address = datagramPacket.sender().getAddress().getHostAddress();

        // port
        int port;

        if (a.get("implied_port") != null && a.get("implied_port").getInt() != 0) {

            port = datagramPacket.sender().getPort();

        } else {

            port = a.get("port").getInt();
        }
        channelHandlerContext.writeAndFlush(new DatagramPacket(
                Unpooled.copiedBuffer(
                        dhtProtocol.announcePeerResponse(transactionId, nodeId)),
                datagramPacket.sender()));
        if (threadPoolExecutor.getQueue().remainingCapacity() <= 0) {

            return;
        }
        LOGGER.info("ip {} port {} infoHash {}", address, port, Utils.bytesToHex(infoHash));
        threadPoolExecutor.execute(() -> {

            PeerClient peerClient = new PeerClient(address, port, infoHash);
            LOGGER.info("todo request peerClient ......");
            byte[] info = peerClient.request();
            if (info != null) {

                JsonMetaInfo jsonMetaInfo = new JsonMetaInfo();
                try {
                    jsonMetaInfo.show(info);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private void queryMethodUnknown(ChannelHandlerContext channelHandlerContext, DatagramPacket datagramPacket, byte[] transactionId) throws IOException {

        channelHandlerContext.writeAndFlush(new DatagramPacket(
                Unpooled.copiedBuffer(
                        dhtProtocol.error(transactionId, 204, "Method Unknown")),
                datagramPacket.sender()));
    }

    private void responseHasId(Map<String, BEncodedValue> r, DatagramPacket datagramPacket) throws InvalidBEncodingException {

        String id = r.get("id").getString();

        if (this.nodeTable.containsKey(id)) {

            String address = datagramPacket.sender().getAddress().getHostAddress();

            int port = datagramPacket.sender().getPort();

            this.nodeTable.put(id, new NodeTable(Utils.bytesToHex(r.get("id").getBytes()), address, port, System.currentTimeMillis()));
        }
    }

    private void responseHasNodes(Map<String, BEncodedValue> r) throws InvalidBEncodingException {

        byte[] nodes = r.get("nodes").getBytes();

        List<NodeTable> nodeTableList = Utils.nodesDecode(nodes);

        if (nodeTable.size() >= maxNodes) {

            return;
        }
        nodeTableList.forEach(nodeTable ->
                this.nodeTable.put(nodeTable.getNid(), new NodeTable(nodeTable.getNid(), nodeTable.getIp(),
                nodeTable.getPort(), System.currentTimeMillis())));
    }

    private void responseError(BEncodedValue data) throws InvalidBEncodingException {

        List<BEncodedValue> e = data.getMap().get("e").getList();

        LOGGER.error(" r : error Code: {} , Description: {}", e.get(0).getInt(), e.get(1).getString());

    }

    private byte[] getToken(byte[] infoHash) {

        return Arrays.copyOfRange(infoHash, 0, 5);
    }

    private boolean validatorToken(byte[] infoHash, byte[] needValidatorToken) {

        byte[] token = this.getToken(infoHash);

        if (needValidatorToken.length != token.length) {

            LOGGER.error("announcePeer validator false length not eq length: {} ", needValidatorToken.length);
            return false;
        }
        for (int i = 0; i < token.length; i++) {

            if (token[i] != needValidatorToken[i]) {

                LOGGER.error("announcePeer validator false token not eq ");
                return false;
            }
        }
        return true;
    }
}
