package me.zpq.dht.server;

import be.adaxisoft.bencode.BDecoder;
import be.adaxisoft.bencode.BEncodedValue;
import be.adaxisoft.bencode.InvalidBEncodingException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
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
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author zpq
 * @date 2019-08-21
 */
public class DiscardServerHandler extends SimpleChannelInboundHandler<DatagramPacket> {

    private DhtProtocol dhtProtocol = new DhtProtocol();

    private static final Logger LOGGER = LoggerFactory.getLogger(DiscardServerHandler.class);

    private static final String ID = "id";

    private static final String NODES = "nodes";

    private static final String IMPLIED_PORT = "implied_port";

    private static final String INFO_HASH = "info_hash";

    private static final String PORT = "port";

    private static final String TOKEN = "token";

    private static final String A = "a";

    private static final String Y = "y";

    private static final String Q = "q";

    private static final String T = "t";

    private static final String R = "r";

    private static final String E = "e";

    private static final String PING = "ping";

    private static final String FIND_NODE = "find_node";

    private static final String GET_PEERS = "get_peers";

    private static final String ANNOUNCE_PEER = "announce_peer";

    private byte[] nodeId;

    private Map<String, NodeTable> nodeTable;

    private int maxNodes;

    private LinkedBlockingQueue<String> metadata;

    public DiscardServerHandler(Map<String, NodeTable> nodeTable, byte[] nodeId, int maxNodes, LinkedBlockingQueue<String> metadata) {

        this.nodeId = nodeId;
        this.nodeTable = nodeTable;
        this.maxNodes = maxNodes;
        this.metadata = metadata;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, DatagramPacket datagramPacket) {

        try {

            ByteBuf content = datagramPacket.copy().content();
            byte[] req = new byte[content.readableBytes()];
            content.readBytes(req);
            content.release();

            BEncodedValue data = BDecoder.decode(new ByteArrayInputStream(req));
            byte[] transactionId = data.getMap().get(T).getBytes();

            switch (data.getMap().get(Y).getString()) {

                case Q:

                    Map<String, BEncodedValue> a = data.getMap().get(A).getMap();
                    switch (data.getMap().get(Q).getString()) {

                        case PING:
                            this.queryPing(channelHandlerContext, datagramPacket, transactionId, a);
                            break;
                        case FIND_NODE:
                            this.queryFindNode(channelHandlerContext, datagramPacket, transactionId);
                            break;
                        case GET_PEERS:
                            this.queryGetPeers(channelHandlerContext, datagramPacket, transactionId, a);
                            break;
                        case ANNOUNCE_PEER:
                            this.queryAnnouncePeer(channelHandlerContext, datagramPacket, transactionId, a);
                            break;
                        default:
//                            this.queryMethodUnknown(channelHandlerContext, datagramPacket, transactionId);
                            break;
                    }

                    break;
                case R:

                    Map<String, BEncodedValue> r = data.getMap().get(R).getMap();
                    if (r.get(A) != null) {

                        this.responseHasId(r, datagramPacket);
                    }

                    if (r.get(NODES) != null) {

                        this.responseHasNodes(r);
                    }

                    break;

                case E:

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
        String id = a.get(ID).getString();
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

        byte[] token = this.getToken(a.get(INFO_HASH).getBytes());
//        List<NodeTable> nodes = new ArrayList<>(nodeTable.values());
        channelHandlerContext.writeAndFlush(new DatagramPacket(
                Unpooled.copiedBuffer(
                        dhtProtocol.getPeersResponseNodes(transactionId, nodeId, token, new byte[0])),
                datagramPacket.sender()));

    }

    private void queryAnnouncePeer(ChannelHandlerContext channelHandlerContext, DatagramPacket datagramPacket, byte[] transactionId, Map<String, BEncodedValue> a) throws IOException {

        // sha1
        byte[] infoHash = a.get(INFO_HASH).getBytes();

        // token
        byte[] needValidatorToken = a.get(TOKEN).getBytes();

        if (!this.validatorToken(infoHash, needValidatorToken)) {

            return;
        }
        // ip
        String address = datagramPacket.sender().getAddress().getHostAddress();

        // port
        int port;

        if (a.get(IMPLIED_PORT) != null && a.get(IMPLIED_PORT).getInt() != 0) {

            port = datagramPacket.sender().getPort();

        } else {

            port = a.get(PORT).getInt();
        }

        String meta = Utils.packMeta(address, port, infoHash);

        if (!metadata.contains(meta)) {

            metadata.add(meta);
        }

        channelHandlerContext.writeAndFlush(new DatagramPacket(
                Unpooled.copiedBuffer(
                        dhtProtocol.announcePeerResponse(transactionId, nodeId)),
                datagramPacket.sender()));
    }

    private void queryMethodUnknown(ChannelHandlerContext channelHandlerContext, DatagramPacket datagramPacket, byte[] transactionId) throws IOException {

        channelHandlerContext.writeAndFlush(new DatagramPacket(
                Unpooled.copiedBuffer(
                        dhtProtocol.error(transactionId, 204, "Method Unknown")),
                datagramPacket.sender()));
    }

    private void responseHasId(Map<String, BEncodedValue> r, DatagramPacket datagramPacket) throws InvalidBEncodingException {

        String id = r.get(ID).getString();

        if (this.nodeTable.containsKey(id)) {

            String address = datagramPacket.sender().getAddress().getHostAddress();

            int port = datagramPacket.sender().getPort();

            this.nodeTable.put(id, new NodeTable(Utils.bytesToHex(r.get("id").getBytes()), address, port, System.currentTimeMillis()));
        }
    }

    private void responseHasNodes(Map<String, BEncodedValue> r) throws InvalidBEncodingException {

        byte[] nodes = r.get(NODES).getBytes();

        List<NodeTable> nodeTableList = Utils.nodesDecode(nodes);

        if (nodeTable.size() >= maxNodes) {

            return;
        }
        nodeTableList.forEach(nodeTable ->
                this.nodeTable.put(nodeTable.getNid(), new NodeTable(nodeTable.getNid(), nodeTable.getIp(),
                        nodeTable.getPort(), System.currentTimeMillis()))
        );

    }

    private void responseError(BEncodedValue data) throws InvalidBEncodingException {

        List<BEncodedValue> e = data.getMap().get(E).getList();

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
