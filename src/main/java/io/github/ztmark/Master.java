package io.github.ztmark;

/**
 * @Author: Mark
 * @Date : 2020/2/13
 */
public class Master {

    private MasterServer server;
    private String[] files;
    private int reduceNum;
    private volatile boolean done = false;

    public Master(String[] files, int reduceNum) throws InterruptedException {
        this.files = files;
        this.reduceNum = reduceNum;
        server = new MasterServer();
    }

    public boolean isDone() {
        return done;
    }


}
