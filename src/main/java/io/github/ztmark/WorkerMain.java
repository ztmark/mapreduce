package io.github.ztmark;

import java.util.ServiceLoader;

/**
 * @Author: Mark
 * @Date : 2020/2/13
 */
public class WorkerMain {

    public static void main(String[] args) {
        MapReduce mapReduce = null;
        final ServiceLoader<MapReduce> load = ServiceLoader.load(MapReduce.class, Thread.currentThread().getContextClassLoader());
        for (MapReduce reduce : load) {
            mapReduce = reduce;
            break;
        }
        final Worker worker = new Worker(mapReduce);
        worker.start();
    }
}
