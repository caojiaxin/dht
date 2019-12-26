package me.zpq.dht;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
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
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
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
        ClassLoader classLoader = Main.class.getClassLoader();
        URL url = classLoader.getResource("config.properties");
        if (url == null) {

            LOGGER.error("error config");
            return;
        }

        byte[] transactionId = new byte[5];
        new Random().nextBytes(transactionId);

        InputStream inputStream = Files.newInputStream(Paths.get(url.getFile()));
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
        String redisHost = properties.getProperty("redis.host");
        int redisPort = Integer.parseInt(properties.getProperty("redis.port"));
        String redisPassword = properties.getProperty("redis.password");
        String mongoUri = properties.getProperty("mongodb.uri");
        inputStream.close();

        JedisPool jedisPool = Main.redisPool(corePoolSize, maximumPoolSize, redisHost, redisPort, redisPassword);
        Bootstrap bootstrap = new Bootstrap();
        byte[] nodeId = Utils.nodeId();
        Map<String, NodeTable> table = new Hashtable<>();
        table.put(new String(nodeId), new NodeTable(Utils.bytesToHex(nodeId), host, port, System.currentTimeMillis()));
        MetaInfo metaInfo = new FileMetaInfoImpl(jedisPool, mongoUri);
        bootstrap.group(new NioEventLoopGroup())
                .channel(NioDatagramChannel.class)
                .option(ChannelOption.SO_BROADCAST, true)
                .handler(new DiscardServerHandler(table, nodeId, maxNodes, metaInfo));
        final Channel channel = bootstrap.bind(port).sync().channel();

        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(5);
        LOGGER.info("start autoFindNode");
        scheduledExecutorService.scheduleWithFixedDelay(new FindNode(channel, transactionId, nodeId, table, minNodes), findNodeInterval, findNodeInterval, TimeUnit.SECONDS);
        LOGGER.info("start ok autoFindNode");

        LOGGER.info("start Ping");
        scheduledExecutorService.scheduleWithFixedDelay(new Ping(channel, transactionId, nodeId, table), pingInterval, pingInterval, TimeUnit.SECONDS);
        LOGGER.info("start ok Ping");

        LOGGER.info("start RemoveNode");
        scheduledExecutorService.scheduleWithFixedDelay(new RemoveNode(table, timeout), removeNodeInterval, removeNodeInterval, TimeUnit.SECONDS);
        LOGGER.info("start ok RemoveNode");

        LOGGER.info("start peerRequestTask");
        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize,
                0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), threadFactory);
        scheduledExecutorService.scheduleAtFixedRate(new Peer(threadPoolExecutor, metaInfo, jedisPool), peerRequestInterval, peerRequestInterval, TimeUnit.SECONDS);
        LOGGER.info("start ok peerRequestTask");
        LOGGER.info("server ok");
    }

    private static JedisPool redisPool(int corePoolSize, int maximumPoolSize, String redisHost, int redisPort, String redisPassword) {

        GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
        genericObjectPoolConfig.setMaxTotal(maximumPoolSize * 2);
        genericObjectPoolConfig.setMaxIdle(maximumPoolSize);
        genericObjectPoolConfig.setMinIdle(corePoolSize);
        JedisPool jedisPool;
        if (redisPassword == null || "".equals(redisPassword.trim())) {

            jedisPool = new JedisPool(genericObjectPoolConfig, redisHost, redisPort, 30000);

        } else {

            jedisPool = new JedisPool(genericObjectPoolConfig, redisHost, redisPort, 30000, redisPassword);

        }
        return jedisPool;
    }
}
