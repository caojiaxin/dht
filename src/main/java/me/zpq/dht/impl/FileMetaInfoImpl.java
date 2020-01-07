package me.zpq.dht.impl;

import be.adaxisoft.bencode.BDecoder;
import be.adaxisoft.bencode.BEncodedValue;
import com.mongodb.client.*;
import me.zpq.dht.MetaInfo;
import me.zpq.dht.util.Utils;
import org.apache.commons.codec.digest.DigestUtils;
import org.bson.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class FileMetaInfoImpl implements MetaInfo {

    private JedisPool jedisPool;

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

    public FileMetaInfoImpl(JedisPool jedisPool, MongoClient mongoClient) {

        this.jedisPool = jedisPool;
        MongoDatabase database = mongoClient.getDatabase(DHT);
        document = database.getCollection(METADATA);
    }

    @Override
    public void todoSomething(byte[] info) throws IOException {

        byte[] sha1 = DigestUtils.sha1(info);
        if (this.isExist(sha1)) {

            return;
        }
        String date = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy/MM/dd"));
        String dir = "/" + METADATA + "/" + date;
        if (!Files.exists(Paths.get(dir))) {

            Files.createDirectories(Paths.get(dir));
        }
        String hex = Utils.bytesToHex(sha1);
        String fileName = hex + ".info";
        OutputStream outputStream = Files.newOutputStream(Paths.get(dir + "/" + fileName));
        outputStream.write(info);
        outputStream.close();
        BEncodedValue decode = BDecoder.decode(new ByteArrayInputStream(info));
        Document metaInfo = new Document();
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
            metaInfo.put(LENGTH, new BsonInt64(decode.getMap().get(LENGTH).getLong()));
        } else {

            // multi-file mode
            BsonArray bsonArray = new BsonArray();
            List<BEncodedValue> files = decode.getMap().get(FILES).getList();
            for (BEncodedValue file : files) {

                BsonDocument f = new BsonDocument();
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
        metaInfo.put(PATH, date + "/" + fileName);
        document.insertOne(metaInfo);
    }

    @Override
    public void onAnnouncePeer(String host, Integer port, byte[] hash) {

        if (this.isExist(hash)) {

            return;
        }
        try (Jedis jedis = jedisPool.getResource()) {

            String infoHash = Utils.bytesToHex(hash);

            jedis.sadd(METADATA, String.join(":", host, infoHash, String.valueOf(port)));
        }
    }

    @Override
    public String redisKey() {

        return METADATA;
    }

    private Boolean isExist(byte[] sha1) {

        Document has = new Document();
        has.put(HASH, new BsonBinary(sha1));
        FindIterable<Document> documents = document.find(has);
        Document first = documents.first();
        return first != null;
    }
}
