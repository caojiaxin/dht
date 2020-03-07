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

    private static final Logger log = LoggerFactory.getLogger(DiscardServerHandler.class);

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
    protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket datagramPacket) {

        try {

            ByteBuf content = datagramPacket.content();
            byte[] req = new byte[content.readableBytes()];
            content.readBytes(req);

            BEncodedValue data = BDecoder.decode(new ByteArrayInputStream(req));
            byte[] transactionId = data.getMap().get(DhtProtocol.T).getBytes();

            switch (data.getMap().get(DhtProtocol.Y).getString()) {

                case DhtProtocol.Q:

                    Map<String, BEncodedValue> a = data.getMap().get(DhtProtocol.A).getMap();
                    switch (data.getMap().get(DhtProtocol.Q).getString()) {

                        case DhtProtocol.PING:
                            this.queryPing(ctx, datagramPacket, transactionId, a);
                            break;
                        case DhtProtocol.FIND_NODE:
                            this.queryFindNode(ctx, datagramPacket, transactionId);
                            break;
                        case DhtProtocol.GET_PEERS:
                            this.queryGetPeers(ctx, datagramPacket, transactionId, a);
                            break;
                        case DhtProtocol.ANNOUNCE_PEER:
                            this.queryAnnouncePeer(ctx, datagramPacket, transactionId, a);
                            break;
                        default:
                            this.queryMethodUnknown(ctx, datagramPacket, transactionId);
                            break;
                    }

                    break;
                case DhtProtocol.R:

                    Map<String, BEncodedValue> r = data.getMap().get(DhtProtocol.R).getMap();
                    if (r.get(DhtProtocol.A) != null) {

                        this.responseHasId(r, datagramPacket);
                    }

                    if (r.get(DhtProtocol.NODES) != null) {

                        this.responseHasNodes(r);
                    }

                    break;

                case DhtProtocol.E:

                    this.responseError(data);

                    break;
                default:
                    break;
            }

        } catch (Exception e) {

            log.error(e.getMessage(), e);
        }

    }

    private void queryPing(ChannelHandlerContext ctx, DatagramPacket datagramPacket, byte[] transactionId, Map<String, BEncodedValue> a) throws IOException {

        ctx.writeAndFlush(new DatagramPacket(
                Unpooled.copiedBuffer(DhtProtocol.pingResponse(transactionId, nodeId)),
                datagramPacket.sender()));

        String id = Utils.bytesToHex(a.get(DhtProtocol.ID).getBytes());
        String ip = datagramPacket.sender().getAddress().getHostAddress();
        int port = datagramPacket.sender().getPort();
        this.updateNodeTable(id, ip, port);
    }

    private void queryFindNode(ChannelHandlerContext ctx, DatagramPacket datagramPacket, byte[] transactionId) throws IOException {

        List<NodeTable> table = new ArrayList<>(nodeTable.values());
        ctx.writeAndFlush(new DatagramPacket(
                Unpooled.copiedBuffer(
                        DhtProtocol.findNodeResponse(transactionId, nodeId, Utils.nodesEncode(table))),
                datagramPacket.sender()));
    }

    private void queryGetPeers(ChannelHandlerContext ctx, DatagramPacket datagramPacket, byte[] transactionId, Map<String, BEncodedValue> a) throws IOException {

        byte[] token = this.getToken(a.get(DhtProtocol.INFO_HASH).getBytes());
//        List<NodeTable> nodes = new ArrayList<>(nodeTable.values());
        ctx.writeAndFlush(new DatagramPacket(
                Unpooled.copiedBuffer(
                        DhtProtocol.getPeersResponseNodes(transactionId, nodeId, token, new byte[0])),
                datagramPacket.sender()));

    }

    private void queryAnnouncePeer(ChannelHandlerContext ctx, DatagramPacket datagramPacket, byte[] transactionId, Map<String, BEncodedValue> a) throws IOException {

        String id = Utils.bytesToHex(a.get(DhtProtocol.ID).getBytes());

        // sha1
        byte[] infoHash = a.get(DhtProtocol.INFO_HASH).getBytes();

        // token
        byte[] needValidatorToken = a.get(DhtProtocol.TOKEN).getBytes();

        if (!this.validatorToken(infoHash, needValidatorToken)) {

            return;
        }
        // ip
        String address = datagramPacket.sender().getAddress().getHostAddress();

        // port
        int port;

        if (a.get(DhtProtocol.IMPLIED_PORT) != null && a.get(DhtProtocol.IMPLIED_PORT).getInt() != 0) {

            port = datagramPacket.sender().getPort();

        } else {

            port = a.get(DhtProtocol.PORT).getInt();
        }

        String meta = Utils.packMeta(address, port, infoHash);

        if (!metadata.contains(meta)) {

            metadata.add(meta);
        }

        ctx.writeAndFlush(new DatagramPacket(
                Unpooled.copiedBuffer(
                        DhtProtocol.announcePeerResponse(transactionId, nodeId)),
                datagramPacket.sender()));
        this.updateNodeTable(id, address, datagramPacket.sender().getPort());
    }

    private void queryMethodUnknown(ChannelHandlerContext ctx, DatagramPacket datagramPacket, byte[] transactionId) throws IOException {

        ctx.writeAndFlush(new DatagramPacket(
                Unpooled.copiedBuffer(
                        DhtProtocol.error(transactionId, 204, "Method Unknown")),
                datagramPacket.sender()));
    }

    private void responseHasId(Map<String, BEncodedValue> r, DatagramPacket datagramPacket) throws InvalidBEncodingException {

        String id = Utils.bytesToHex(r.get(DhtProtocol.ID).getBytes());

        if (this.nodeTable.containsKey(id)) {

            String address = datagramPacket.sender().getAddress().getHostAddress();

            int port = datagramPacket.sender().getPort();

            this.nodeTable.put(id, new NodeTable(id, address, port, System.currentTimeMillis()));
        }
    }

    private void responseHasNodes(Map<String, BEncodedValue> r) throws InvalidBEncodingException {

        byte[] nodes = r.get(DhtProtocol.NODES).getBytes();

        List<NodeTable> nodeTableList = Utils.nodesDecode(nodes);

        if (nodeTable.size() >= maxNodes) {

            return;
        }
        nodeTableList.forEach(table ->
                nodeTable.put(table.getNid(), new NodeTable(table.getNid(), table.getIp(),
                        table.getPort(), System.currentTimeMillis()))
        );

    }

    private void responseError(BEncodedValue data) throws InvalidBEncodingException {

        List<BEncodedValue> e = data.getMap().get(DhtProtocol.E).getList();

        log.error(" r : error Code: {} , Description: {}", e.get(0).getInt(), e.get(1).getString());

    }

    private byte[] getToken(byte[] infoHash) {

        return Arrays.copyOfRange(infoHash, 0, 5);
    }

    private boolean validatorToken(byte[] infoHash, byte[] needValidatorToken) {

        byte[] token = this.getToken(infoHash);

        if (needValidatorToken.length != token.length) {

            log.error("announcePeer validator false length not eq length: {} ", needValidatorToken.length);
            return false;
        }
        for (int i = 0; i < token.length; i++) {

            if (token[i] != needValidatorToken[i]) {

                log.error("announcePeer validator false token not eq ");
                return false;
            }
        }
        return true;
    }

    private void updateNodeTable(String id, String ip, int port) {

        if (nodeTable.containsKey(id)) {

            NodeTable nodeTable = this.nodeTable.get(id);
            nodeTable.setTime(System.currentTimeMillis());
            this.nodeTable.put(id, nodeTable);
        } else {

            this.nodeTable.put(id, new NodeTable(id, ip, port, System.currentTimeMillis()));
        }
    }
}
