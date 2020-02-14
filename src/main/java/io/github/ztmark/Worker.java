package io.github.ztmark;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @Author: Mark
 * @Date : 2020/2/13
 */
public class Worker {

    private MapReduce mapReduce;
    private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors(),
            new NamedThreadFactory("worker-scheduler"));
    private WorkerClient client;

    public Worker(MapReduce mapReduce) {
        this.mapReduce = mapReduce;
        this.client = new WorkerClient();
    }

    public void start() {
        scheduler.scheduleAtFixedRate(() -> {
            client.sendHeartBeat(new Command(0, new HeartBeat("ping")));
            System.out.println("send heart beat");
        }, 10, 3000, TimeUnit.MILLISECONDS);
    }



}
