package me.zpq.dht.client;

import be.adaxisoft.bencode.BDecoder;
import be.adaxisoft.bencode.BEncodedValue;
import be.adaxisoft.bencode.BEncoder;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import me.zpq.dht.util.Utils;
import org.apache.commons.codec.digest.DigestUtils;
import org.bson.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PeerClient implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(PeerClient.class);

    /**
     * @see <a href="http://www.bittorrent.org/beps/bep_0020.html">Peer ID Conventions</>
     */
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

    private MongoCollection<Document> document;

    private static final String HASH = "hash";

    private static final String NAME = "name";

    private static final String NAME_UTF8 = "name.utf-8";

    private static final String PIECE_LENGTH = "piece length";

    private static final String CREATED_DATETIME = "created datetime";

    private static final String FILES = "files";

    private static final String LENGTH = "length";

    private static final String PATH = "path";

    private static final String PATH_UTF8 = "path.utf-8";

    private static final String DHT = "dht";

    private static final String METADATA = "metadata";

    private static final String SIZE = "size";

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy/MM/dd");

    private static final String FILE_EXT = ".info";

    private String host;

    private int port;

    private byte[] infoHash;

    public PeerClient(String host, int port, byte[] infoHash, MongoClient mongoClient) {
        this.host = host;
        this.port = port;
        this.infoHash = infoHash;
        this.document = mongoClient.getDatabase(DHT).getCollection(METADATA);
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

                log.info("length fail");
                return;
            }
            for (int i = 0; i < infoHash.length; i++) {

                if (infoHash[i] != sha1[i]) {

                    log.info("info hash not eq");
                    return;
                }
            }
            log.info("success");
            this.save(info);
        } catch (Exception e) {

            log.info("{} : {}", e.getClass().getName(), e.getMessage());
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

    private void save(byte[] info) throws IOException {
        byte[] sha1 = DigestUtils.sha1(info);
        String hex = Utils.bytesToHex(sha1);
        if (this.isExist(sha1)) {

            log.info("sha1: {} is exist", hex);
            return;
        }
        String date = LocalDate.now().format(FORMATTER);
        String dir = "/" + METADATA + "/" + date;
        if (!Files.exists(Paths.get(dir))) {

            Files.createDirectories(Paths.get(dir));
        }
        String fileName = hex + FILE_EXT;
        BEncodedValue decode = BDecoder.decode(new ByteArrayInputStream(info));
        Document metaInfo = new Document();
        long size = 0;
        metaInfo.put(HASH, new BsonBinary(sha1));
        String name = decode.getMap().get(NAME).getString();
        if (decode.getMap().get(NAME_UTF8) != null) {

            // 存在uft-8扩展
            name = decode.getMap().get(NAME_UTF8).getString();
        }
        metaInfo.put(NAME, name);
        metaInfo.put(PIECE_LENGTH, decode.getMap().get(PIECE_LENGTH).getInt());
        metaInfo.put(CREATED_DATETIME, new BsonDateTime(System.currentTimeMillis()));
        if (decode.getMap().get(LENGTH) != null) {

            // single-file mode
            size = decode.getMap().get(LENGTH).getLong();
            metaInfo.put(LENGTH, new BsonInt64(size));
        } else {

            // multi-file mode
            BsonArray bsonArray = new BsonArray();
            List<BEncodedValue> files = decode.getMap().get(FILES).getList();
            for (BEncodedValue file : files) {

                BsonDocument f = new BsonDocument();
                size += file.getMap().get(LENGTH).getLong();
                f.put(LENGTH, new BsonInt64(file.getMap().get(LENGTH).getLong()));
                BsonArray path = new BsonArray();
                List<BEncodedValue> paths = file.getMap().get(PATH).getList();
                if (file.getMap().get(PATH_UTF8) != null) {

                    // 存在uft-8扩展
                    paths = file.getMap().get(PATH_UTF8).getList();
                }
                for (BEncodedValue p : paths) {

                    path.add(new BsonString(p.getString()));
                }
                f.put(PATH, path);
                bsonArray.add(f);
            }
            metaInfo.put(FILES, bsonArray);
        }
        metaInfo.put(PATH, "/" + date + "/" + fileName);
        metaInfo.put(SIZE, new BsonInt64(size));
        OutputStream outputStream = Files.newOutputStream(Paths.get(dir + "/" + fileName));
        outputStream.write(info);
        outputStream.close();
        document.insertOne(metaInfo);
    }

    private Boolean isExist(byte[] sha1) {

        Document has = new Document();
        has.put(HASH, new BsonBinary(sha1));
        FindIterable<Document> documents = document.find(has);
        Document first = documents.first();
        return first != null;
    }
}
