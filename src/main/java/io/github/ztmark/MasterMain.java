package io.github.ztmark;

import java.util.concurrent.TimeUnit;

import io.github.ztmark.master.Master;

/**
 * @Author: Mark
 * @Date : 2020/2/13
 */
public class MasterMain {

    public static void main(String[] args) throws InterruptedException {
        final Master master = new Master(args, 10);
        while (!master.isDone()) {
            TimeUnit.SECONDS.sleep(1);
        }
    }

}
