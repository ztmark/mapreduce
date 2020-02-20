package io.github.ztmark.worker;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.github.ztmark.MapReduce;
import io.github.ztmark.common.Command;
import io.github.ztmark.common.CommandCode;
import io.github.ztmark.common.DoneJob;
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

    public Worker(MapReduce mapReduce) throws InterruptedException, UnknownHostException {
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
                    final DoneJob doneJob = doTheJob(command);
                    if (doneJob.getJobType() != Job.POISON) {
                        client.sendDoneJob(doneJob);
                    }
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

    private DoneJob doTheJob(Command command) {
        final DoneJob doneJob = new DoneJob();
        doneJob.setWorkerId(workerId);
        if (command != null) {
            if (command.getCode() == CommandCode.FETCH_JOB_RESP) {
                FetchJobResp jobResp = (FetchJobResp) command.getBody();
                if (jobResp != null && jobResp.getJob() != null) {
                    final Job job = jobResp.getJob();
                    final int jobType = job.getJobType();
                    doneJob.setJobType(jobType);
                    try {
                        Set<String> resultFile = new HashSet<>();
                        if (jobType == Job.MAP_JOB) {
                            doneJob.setArg(job.getArg());
                            final String content = String.join(System.lineSeparator(), readFile(job.getArg()));
                            final List<KeyValue> result = mapReduce.map(job.getArg(), content);
                            // hash key 将相同hash值的写到 med-workid-hash 文件中
                            resultFile.addAll(writeInterMediateFile(result));
                        } else if (jobType == Job.REDUCE_JOB) {
                            // 将文件写到 output-workid 文件中
                            final String[] args = job.getArg().split(":");
                            doneJob.setArg(args[0]);
                            resultFile.addAll(doReduce(args[1].split(";")));
                        } else {
                            shutdown = true;
                        }
                        doneJob.setResult(resultFile);
                    } catch (IOException e) {
                        e.printStackTrace();
                        shutdown = true;
                    }
                }
            }
        }
        return doneJob;
    }

    private List<String> doReduce(String[] filenames) throws IOException {
        List<String> result = new ArrayList<>();
        final List<String> lines = new ArrayList<>();
        for (String filename : filenames) {
            lines.addAll(readFile(filename));
        }
        Map<String, List<String>> map = new HashMap<>();
        for (String line : lines) {
            final String[] s = line.split(" ");
            if (s.length == 2) {
                final List<String> values = map.computeIfAbsent(s[0], v -> new ArrayList<>());
                values.add(s[1]);
            }
        }
        File file = new File("output-" + workerId);
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true)))) {
            for (Map.Entry<String, List<String>> entry : map.entrySet()) {
                final String reduce = mapReduce.reduce(entry.getKey(), entry.getValue());
                writer.append(String.format("%s %s", entry.getKey(), reduce));
                writer.newLine();
            }
        }
        return result;
    }

    private Set<String> writeInterMediateFile(List<KeyValue> result) {
        Map<Integer, List<KeyValue>> map = new HashMap<>();
        for (KeyValue keyValue : result) {
            final int hash = hashHash(keyValue.getKey().hashCode());
            List<KeyValue> list = map.computeIfAbsent(hash, k -> new ArrayList<>());
            list.add(keyValue);
        }

        Set<String> resultFile = new HashSet<>();
        for (Map.Entry<Integer, List<KeyValue>> entry : map.entrySet()) {
            String filename = "im-out-" + workerId + "-" + entry.getKey();
            resultFile.add(filename);
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
        return resultFile;
    }

    private int hashHash(int hash) {
        hash = Math.max(0, Math.abs(hash));
        return hash % 5;
    }

    private List<String> readFile(String filename) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(filename)))) {
            List<String> lines = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line.trim());
            }
            return lines;
        }
    }


    private String generateId() throws UnknownHostException {
        final int port = 8800;
        final String hostString = InetAddress.getLocalHost().getHostAddress();
        return hostString.replaceAll("\\.", "_") + "_" + port;
    }

}
