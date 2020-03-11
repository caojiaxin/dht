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

    private static final Logger log = LoggerFactory.getLogger(Peer.class);

    private ThreadPoolExecutor threadPoolExecutor;

    public Peer(ThreadPoolExecutor threadPoolExecutor) {
        this.threadPoolExecutor = threadPoolExecutor;
    }

    @Override
    public void run() {

        int poolSize = threadPoolExecutor.getPoolSize();
        int corePoolSize = threadPoolExecutor.getCorePoolSize();
        int maximumPoolSize = threadPoolExecutor.getMaximumPoolSize();
        int largestPoolSize = threadPoolExecutor.getLargestPoolSize();
        int activeCount = threadPoolExecutor.getActiveCount();
        long taskCount = threadPoolExecutor.getTaskCount();
        int queueSize = threadPoolExecutor.getQueue().size();
        long completedTaskCount = threadPoolExecutor.getCompletedTaskCount();
        log.info("threadPoolExecutor poolSize: {} corePoolSize: {} maximumPoolSize: {} largestPoolSize: {} " +
                        "activeCount: {} taskCount: {} queueSize: {} completedTaskCount: {}", poolSize, corePoolSize,
                maximumPoolSize, largestPoolSize, activeCount, taskCount, queueSize, completedTaskCount);

    }
}
