package io.github.ztmark.master;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

import io.github.ztmark.common.Job;
import io.netty.channel.Channel;

/**
 * @Author: Mark
 * @Date : 2020/2/13
 */
public class Master {

    private MasterServer server;
    private Set<String> toMapFile;
    private Set<String> mappingFile;
    private Set<String> toReduceFile;
    private Set<String> reducingFile;
    private Set<String> resultFile;
    private int reduceNum;
    private volatile boolean done = false;

    private Map<String, ChannelWrapper> workers;
    private Set<String> idleWorkers;
    private Set<String> workingWorkers;

    public Master(List<String> files, int reduceNum) throws InterruptedException {
        toMapFile = new HashSet<>(files);
        mappingFile = new HashSet<>();
        toReduceFile = new HashSet<>();
        reducingFile = new HashSet<>();
        resultFile = new HashSet<>();
        this.reduceNum = reduceNum;
        workers = new ConcurrentHashMap<>();
        idleWorkers = new ConcurrentSkipListSet<>();
        workingWorkers = new ConcurrentSkipListSet<>();
        server = new MasterServer(this, Collections.singletonList(new CommandHandler(this)));
        server.start();
    }

    public boolean isDone() {
        return done;
    }

    public void addWorker(String workerId, Channel channel) {
        final ChannelWrapper channelWrapper = new ChannelWrapper();
        channelWrapper.setChannel(channel);
        channelWrapper.setLastPingTime(System.currentTimeMillis());
        workers.put(workerId, channelWrapper);
        idleWorkers.add(workerId);
    }

    public void ping(String workerId) {
        final ChannelWrapper channelWrapper = workers.get(workerId);
        if (channelWrapper != null) {
            channelWrapper.setLastPingTime(System.currentTimeMillis());
        }
    }

    public Job fetchJob(String workId) {
        if (!toMapFile.isEmpty()) {
            String mapFile = null;
            final Iterator<String> iterator = toMapFile.iterator();
            if (iterator.hasNext()) {
                mapFile = iterator.next();
                iterator.remove();
            }
            if (mapFile != null) {
                final Job job = new Job();
                job.setArg(mapFile);
                job.setJobType(Job.MAP_JOB);
                return job;
            }
        }
        if (mappingFile.isEmpty() && !toReduceFile.isEmpty()) {
            String reduceFile = null;
            final Iterator<String> iterator = toReduceFile.iterator();
            if (iterator.hasNext()) {
                reduceFile = iterator.next();
                iterator.remove();
            }
            if (reduceFile != null) {
                final Job job = new Job();
                job.setArg(reduceFile);
                job.setJobType(Job.REDUCE_JOB);
                return job;
            }
        }
        return new Job();
    }

}
