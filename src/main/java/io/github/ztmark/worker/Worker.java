package io.github.ztmark.worker;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sun.xml.internal.ws.api.FeatureConstructor;

import io.github.ztmark.common.FetchJob;
import io.github.ztmark.common.HeartBeat;
import io.github.ztmark.MapReduce;
import io.github.ztmark.common.Command;
import io.github.ztmark.common.NamedThreadFactory;

/**
 * @Author: Mark
 * @Date : 2020/2/13
 */
public class Worker {

    private Logger logger = Logger.getLogger(Worker.class.getName());

    private String workerId;

    private MapReduce mapReduce;
    private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors(),
            new NamedThreadFactory("worker-scheduler"));
    private WorkerClient client;

    private volatile boolean shutdown = false;

    public Worker(MapReduce mapReduce) throws InterruptedException {
        workerId = generateId();
        this.mapReduce = mapReduce;
        this.client = new WorkerClient();
    }

    public void start() throws InterruptedException {

        if (client.register(workerId)) {
            scheduler.scheduleAtFixedRate(() -> {
                try {
                    client.sendHeartBeat(new HeartBeat(workerId));
                    logger.info("send heart beat");
                } catch (InterruptedException e) {
                    logger.log(Level.SEVERE, e.getMessage());
                }
            }, 10, 3000, TimeUnit.MILLISECONDS);


            while (!shutdown) {
                try {
                    final Command command = client.fetchJob(new FetchJob(workerId));
                    logger.info("get job " + command);
                    TimeUnit.SECONDS.sleep(3);
                } catch (Exception e) {
                    logger.severe(e.getMessage());
                }
            }


        }
    }


    private String generateId() {
        return UUID.randomUUID().toString().replaceAll("-", "");
    }

}
