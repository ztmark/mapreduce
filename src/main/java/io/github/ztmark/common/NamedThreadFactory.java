package io.github.ztmark.common;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * @Author: Mark
 * @Date : 2020/2/12
 */
public class NamedThreadFactory implements ThreadFactory {

    private String namePrefix;
    private AtomicInteger threadNum;
    private boolean daemon;

    public NamedThreadFactory(String namePrefix) {
        this(namePrefix, false);
    }

    public NamedThreadFactory(String namePrefix, boolean daemon) {
        this.namePrefix = namePrefix;
        this.daemon = daemon;
        threadNum = new AtomicInteger(1);
    }

    @Override
    public Thread newThread(Runnable r) {
        final Thread thread = new Thread(r, namePrefix + "-" + threadNum.getAndIncrement());
        thread.setDaemon(daemon);
        return thread;
    }
}
