package me.zpq.dht.client;

import be.adaxisoft.bencode.BDecoder;
import be.adaxisoft.bencode.BEncodedValue;
import be.adaxisoft.bencode.BEncoder;
import me.zpq.dht.JsonMetaInfo;
import me.zpq.dht.util.Utils;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class PeerClient implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(PeerClient.class);

    private static final String PEER_ID = "-WW0001-123456789012";

    private static final String PROTOCOL = "BitTorrent protocol";

    private static final String M = "m";

    private static final String UT_METADATA = "ut_metadata";

    private static final String METADATA_SIZE = "metadata_size";

    private static final String MSG_TYPE = "msg_type";

    private static final String PIECE = "piece";

    private static final String TOTAL_SIZE = "total_size";

    private static final int CONNECT_TIMEOUT = 30 * 1000;

    private static final int READ_TIMEOUT = 60 * 1000;

    private static final int BLOCK_SIZE = 16384;

    private String host;

    private int port;

    private byte[] infoHash;

    public PeerClient(String host, int port, byte[] infoHash) {
        this.host = host;
        this.port = port;
        this.infoHash = infoHash;
    }

    @Override
    public void run() {

        try (Socket socket = new Socket()) {
            socket.setSoTimeout(READ_TIMEOUT);
            socket.setTcpNoDelay(true);
            socket.setKeepAlive(true);
            log.info("connect server host: {} port: {} hash: {}", host, port, Utils.bytesToHex(this.infoHash));
            socket.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT);
            OutputStream outputStream = socket.getOutputStream();
            InputStream inputStream = socket.getInputStream();
            log.info("try to handshake");
            this.handshake(outputStream);
            if (!this.validatorHandshake(inputStream)) {

                return;
            }
            log.info("try to extHandShake");
            this.extHandShake(outputStream);
            BEncodedValue bEncodedValue = this.validatorExtHandShake(inputStream);
            if (bEncodedValue == null) {

                return;
            }
            int utMetadata = bEncodedValue.getMap().get(M).getMap().get(UT_METADATA).getInt();
            int metaDataSize = bEncodedValue.getMap().get(METADATA_SIZE).getInt();
            int block = metaDataSize % BLOCK_SIZE > 0 ? metaDataSize / BLOCK_SIZE + 1 : metaDataSize / BLOCK_SIZE;
            log.info("metaDataSize: {} block: {}", metaDataSize, block);
            for (int i = 0; i < block; i++) {

                this.metadataRequest(outputStream, utMetadata, i);
                log.info("request block index: {} ok", i);
            }
            ByteBuffer metaInfo = ByteBuffer.allocate(metaDataSize);
            for (int i = 0; i < block; i++) {

                Map<String, BEncodedValue> m = new HashMap<>(6);
                m.put(MSG_TYPE, new BEncodedValue(1));
                m.put(PIECE, new BEncodedValue(i));
                m.put(TOTAL_SIZE, new BEncodedValue(metaDataSize));
                byte[] response = BEncoder.encode(m).array();
                byte[] length = this.resolveLengthMessage(inputStream, 4);
                byte[] result = this.resolveLengthMessage(inputStream, byte2int(length));
                metaInfo.put(Arrays.copyOfRange(result, response.length + 2, result.length));
                log.info("resolve block index: {} ok", i);
            }
            log.info("validator sha1");
            byte[] info = metaInfo.array();
            byte[] sha1 = DigestUtils.sha1(info);
            if (sha1.length != infoHash.length) {

                log.error("length fail");
                return;
            }
            for (int i = 0; i < infoHash.length; i++) {

                if (infoHash[i] != sha1[i]) {

                    log.error("info hash not eq");
                    return;
                }
            }
            log.info("success");
            String json = JsonMetaInfo.show(info);
            log.info(json);
        } catch (Exception e) {

            log.error("{} : {}", e.getClass().getName(), e.getMessage());
        }

    }

    private void handshake(OutputStream outputStream) throws IOException {

        byte[] extension = new byte[]{0, 0, 0, 0, 0, 16, 0, 0};
        ByteBuffer handshake = ByteBuffer.allocate(68);
        handshake.put((byte) PROTOCOL.length())
                .put(PROTOCOL.getBytes())
                .put(extension)
                .put(infoHash)
                .put(PEER_ID.getBytes());
        outputStream.write(handshake.array());
        outputStream.flush();
    }

    private boolean validatorHandshake(InputStream inputStream) throws IOException {

        byte[] bitTorrent = this.resolveMessage(inputStream);
        if (!PROTOCOL.equals(new String(bitTorrent))) {

            log.error("protocol != BitTorrent, protocol: {}", new String(bitTorrent));
            return false;
        }
        byte[] last = this.resolveLengthMessage(inputStream, 48);
        byte[] infoHash = Arrays.copyOfRange(last, 8, 28);
        if (infoHash.length != this.infoHash.length) {

            log.error("info hash length is diff");
            return false;
        }
        for (int i = 0; i < 20; i++) {

            if (infoHash[i] != this.infoHash[i]) {

                log.error("info hash byte is diff");
                return false;
            }
        }
        return true;
    }

    private void extHandShake(OutputStream outputStream) throws IOException {

        Map<String, BEncodedValue> m = new HashMap<>(6);
        Map<String, BEncodedValue> utMetadata = new HashMap<>(6);
        utMetadata.put(UT_METADATA, new BEncodedValue(1));
        m.put(M, new BEncodedValue(utMetadata));
        outputStream.write(this.packMessage(20, 0, BEncoder.encode(m).array()));
        outputStream.flush();
    }

    private BEncodedValue validatorExtHandShake(InputStream inputStream) throws IOException {

        byte[] prefix = this.resolveLengthMessage(inputStream, 4);
        int length = byte2int(prefix);
        byte[] data = this.resolveLengthMessage(inputStream, length);
        int messageId = data[0];
        int messageType = data[1];
        if (messageId != 20) {

            log.error("want to get messageId 20 but messageId: {}", messageId);
            return null;
        }
        if (messageType != 0) {

            log.error("want to get messageType 0 but messageType: {}", messageType);
            return null;
        }
        byte[] bDecode = Arrays.copyOfRange(data, 2, length);
        BEncodedValue decode = BDecoder.decode(new ByteArrayInputStream(bDecode));
        if (decode.getMap().get(METADATA_SIZE) == null) {

            log.error("metadata_size == null");
            return null;
        }
        if (decode.getMap().get(METADATA_SIZE).getInt() <= 0) {

            log.error("metadata_size <= 0");
            return null;
        }
        if (decode.getMap().get(M) == null) {

            log.error("m == null");
            return null;
        }
        if (decode.getMap().get(M).getMap().get(UT_METADATA) == null) {

            log.error("m.ut_metadata == null");
            return null;
        }
        while (inputStream.available() > 0) {

            int read = inputStream.read();
        }
        return decode;

    }

    private void metadataRequest(OutputStream outputStream, int utMetadata, int piece) throws IOException {

        Map<String, BEncodedValue> d = new HashMap<>(6);
        d.put(MSG_TYPE, new BEncodedValue(0));
        d.put(PIECE, new BEncodedValue(piece));
        outputStream.write(this.packMessage(20, utMetadata, BEncoder.encode(d).array()));
        outputStream.flush();
    }

    private int byte2int(byte[] bytes) {

        int value = 0;
        for (int i = 0; i < 4; i++) {
            int shift = (3 - i) * 8;
            value += (bytes[i] & 0xFF) << shift;
        }
        return value;
    }

    private byte[] resolveMessage(InputStream inputStream) throws IOException {

        int length = inputStream.read();
        if (length <= 0) {

            throw new IOException("end of the stream is reached");
        }
        return this.resolveLengthMessage(inputStream, length);
    }

    private byte[] resolveLengthMessage(InputStream inputStream, int length) throws IOException {

        byte[] result = new byte[length];
        for (int i = 0; i < length; i++) {

            int r = inputStream.read();
            if (r == -1) {

                throw new IOException("end of the stream is reached");
            }
            result[i] = (byte) r;
        }
        return result;
    }

    private byte[] packMessage(int messageId, int messageType, byte[] data) {

        ByteBuffer byteBuffer = ByteBuffer.allocate(data.length + 6);
        byteBuffer.putInt(data.length + 2)
                .put((byte) (messageId))
                .put((byte) (messageType))
                .put(data);
        return byteBuffer.array();
    }
}
