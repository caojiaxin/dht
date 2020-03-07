package me.zpq.dht;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import me.zpq.dht.impl.FileMetaInfoImpl;
import me.zpq.dht.model.NodeTable;
import me.zpq.dht.scheduled.FindNode;
import me.zpq.dht.scheduled.Peer;
import me.zpq.dht.scheduled.Ping;
import me.zpq.dht.scheduled.RemoveNode;
import me.zpq.dht.server.DiscardServerHandler;
import me.zpq.dht.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;

/**
 * @author zpq
 * @date 2019-08-28
 */
public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws InterruptedException, IOException {

        ((LoggerContext) LoggerFactory.getILoggerFactory()).getLogger("org.mongodb.driver").setLevel(Level.ERROR);
        String dir = System.getProperty("user.dir");
        Path configFile = Paths.get(dir + "/config.properties");
        if (!Files.exists(configFile)) {

            LOGGER.error("error config");
            return;
        }
        LinkedBlockingQueue<String> metadata = new LinkedBlockingQueue<>();
        byte[] transactionId = new byte[5];
        new Random().nextBytes(transactionId);

        InputStream inputStream = Files.newInputStream(configFile);
        Properties properties = new Properties();
        properties.load(inputStream);

        String host = properties.getProperty("server.ip");
        int port = Integer.parseInt(properties.getProperty("server.port"));
        int minNodes = Integer.parseInt(properties.getProperty("server.nodes.min"));
        int maxNodes = Integer.parseInt(properties.getProperty("server.nodes.max"));
        int timeout = Integer.parseInt(properties.getProperty("server.nodes.timeout"));
        int corePoolSize = Integer.parseInt(properties.getProperty("server.peers.core.pool.size"));
        int maximumPoolSize = Integer.parseInt(properties.getProperty("server.peers.maximum.pool.size"));
        int findNodeInterval = Integer.parseInt(properties.getProperty("server.findNode.interval"));
        int pingInterval = Integer.parseInt(properties.getProperty("server.ping.interval"));
        int removeNodeInterval = Integer.parseInt(properties.getProperty("server.removeNode.interval"));
        int peerRequestInterval = Integer.parseInt(properties.getProperty("server.peerRequest.interval"));
        String mongoUri = properties.getProperty("mongodb.uri");
        inputStream.close();

        MongoClient mongoClient = Main.mongo(mongoUri);

        Bootstrap bootstrap = new Bootstrap();
        byte[] nodeId = Utils.nodeId();
        Set<NodeTable> nodeTables = new HashSet<>(6);
        nodeTables.add(new NodeTable(Utils.bytesToHex(nodeId), host, port, System.currentTimeMillis()));
        MetaInfo metaInfo = new FileMetaInfoImpl(mongoClient);
        EventLoopGroup group = new NioEventLoopGroup();
        bootstrap.group(group)
                .channel(NioDatagramChannel.class)
                .option(ChannelOption.SO_BROADCAST, true)
                .handler(new DiscardServerHandler(nodeTables, nodeId, maxNodes, metadata));
        final Channel channel = bootstrap.bind(port).sync().channel();

        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(5);
        LOGGER.info("start autoFindNode");
        scheduledExecutorService.scheduleWithFixedDelay(new FindNode(channel, transactionId, nodeId, nodeTables, minNodes), findNodeInterval, findNodeInterval, TimeUnit.SECONDS);
        LOGGER.info("start ok autoFindNode");

        LOGGER.info("start Ping");
        scheduledExecutorService.scheduleWithFixedDelay(new Ping(channel, transactionId, nodeId, nodeTables), pingInterval, pingInterval, TimeUnit.SECONDS);
        LOGGER.info("start ok Ping");

        LOGGER.info("start RemoveNode");
        scheduledExecutorService.scheduleWithFixedDelay(new RemoveNode(nodeTables, timeout), removeNodeInterval, removeNodeInterval, TimeUnit.SECONDS);
        LOGGER.info("start ok RemoveNode");

        LOGGER.info("start peerRequestTask");
        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize,
                0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), threadFactory);
        scheduledExecutorService.scheduleAtFixedRate(new Peer(threadPoolExecutor, metaInfo, metadata), peerRequestInterval, peerRequestInterval, TimeUnit.SECONDS);
        LOGGER.info("start ok peerRequestTask");
        LOGGER.info("server ok");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {

            mongoClient.close();
            threadPoolExecutor.shutdown();
            scheduledExecutorService.shutdown();
            group.shutdownGracefully();
            LOGGER.info("server shutdown");
        }));
    }

    private static MongoClient mongo(String mongoUri) {

        MongoClientSettings.Builder mongoClientSettings = MongoClientSettings.builder();
        ConnectionString connectionString = new ConnectionString(mongoUri);
        mongoClientSettings.applyConnectionString(connectionString);
        return MongoClients.create(mongoClientSettings.build());
    }
}
