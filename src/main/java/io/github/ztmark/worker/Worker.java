package io.github.ztmark.worker;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

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

    public Worker(MapReduce mapReduce) {
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
        }
    }


    private String generateId() {
        return UUID.randomUUID().toString().replaceAll("-", "");
    }

}
