package me.zpq.dht.scheduled;

import me.zpq.dht.MetaInfo;
import me.zpq.dht.client.PeerClient;
import me.zpq.dht.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author zpq
 * @date 2019-09-19
 */
public class Peer implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Peer.class);

    private ThreadPoolExecutor threadPoolExecutor;

    private MetaInfo mongoMetaInfo;

    private LinkedBlockingQueue<String> metadata;

    public Peer(ThreadPoolExecutor threadPoolExecutor, MetaInfo metaInfo, LinkedBlockingQueue<String> metadata) {
        this.threadPoolExecutor = threadPoolExecutor;
        this.mongoMetaInfo = metaInfo;
        this.metadata = metadata;
    }

    @Override
    public void run() {

        if (threadPoolExecutor.getActiveCount() >= threadPoolExecutor.getMaximumPoolSize()) {

            return;
        }

        LOGGER.info("linkedBlockQueue len {}", metadata.size());

        String metaInfo = metadata.poll();
        if (metaInfo == null) {

            return;
        }

        String[] info = Utils.unpackMeta(metaInfo);
        String ip = info[0];
        byte[] infoHash = Utils.hexToByte(info[1]);
        int port = Integer.parseInt(info[2]);
        threadPoolExecutor.execute(() -> {

            PeerClient peerClient = new PeerClient(ip, port, infoHash);
            LOGGER.info("todo request peerClient ......");
            byte[] metadata = peerClient.request();
            if (metadata != null) {

                try {

                    mongoMetaInfo.todoSomething(metadata);

                } catch (Exception e) {

                    LOGGER.error("mongoMetaInfo error: {}", e.getMessage());

                }
            }
        });
    }
}
