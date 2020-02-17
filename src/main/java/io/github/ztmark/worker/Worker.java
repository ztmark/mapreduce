package io.github.ztmark.worker;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.github.ztmark.MapReduce;
import io.github.ztmark.common.Command;
import io.github.ztmark.common.CommandCode;
import io.github.ztmark.common.FetchJob;
import io.github.ztmark.common.FetchJobResp;
import io.github.ztmark.common.HeartBeat;
import io.github.ztmark.common.Job;
import io.github.ztmark.common.KeyValue;
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
            final ScheduledFuture<?> heartBeat = scheduler.scheduleAtFixedRate(() -> {
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
                    doTheJob(command);
                    // todo call master job done
                    TimeUnit.SECONDS.sleep(3);
                } catch (Exception e) {
                    logger.severe(e.getMessage());
                }
            }

            heartBeat.cancel(true);
            scheduler.shutdownNow();
            client.stop();
            logger.info(workerId + " shutdown");
        }
    }

    private void doTheJob(Command command) {
        if (command != null) {
            if (command.getCode() == CommandCode.FETCH_JOB_RESP) {
                FetchJobResp jobResp = (FetchJobResp) command.getBody();
                if (jobResp != null && jobResp.getJob() != null) {
                    final Job job = jobResp.getJob();
                    final int jobType = job.getJobType();
                    try {
                        if (jobType == Job.MAP_JOB) {
                            final String content = readFile(job.getArg());
                            final List<KeyValue> result = mapReduce.map(job.getArg(), content);
                            // hash key 将相同hash值的写到 med-workid-hash 文件中
                            writeInterMediateFile(result);
                        } else if (jobType == Job.REDUCE_JOB) {
                             // todo reduce
                            // 将文件写到 output-workid 文件中
                        } else {
                            shutdown = true;
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                        shutdown = true;
                    }
                }
            }
        }
    }

    private void writeInterMediateFile(List<KeyValue> result) {
        Map<Integer, List<KeyValue>> map = new HashMap<>();
        for (KeyValue keyValue : result) {
            final int hash = hashHash(keyValue.getKey().hashCode());
            List<KeyValue> list = map.computeIfAbsent(hash, k -> new ArrayList<>());
            list.add(keyValue);
        }

        for (Map.Entry<Integer, List<KeyValue>> entry : map.entrySet()) {
            String filename = "im-out-" + workerId + "-" + entry.getKey();
            final File file = new File(filename);
            try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true)))) {

                for (KeyValue keyValue : entry.getValue()) {
                    writer.append(String.format("%s %s", keyValue.getKey(), keyValue.getValue()));
                    writer.newLine();
                }
                writer.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private int hashHash(int hash) {
        return Math.abs(hash % 5);
    }

    private String readFile(String filename) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(filename)))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
            return sb.toString();
        }
    }


    private String generateId() {
        return UUID.randomUUID().toString().replaceAll("-", "");
    }

}
