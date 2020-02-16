package io.github.ztmark;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.github.ztmark.master.Master;

/**
 * @Author: Mark
 * @Date : 2020/2/13
 */
public class MasterMain {

    public static void main(String[] args) throws InterruptedException {


        List<String> files = Arrays.asList();

        final String path = MasterMain.class.getClassLoader().getResource("").getPath();
        files = files.stream().map(f -> path + File.separator + f).collect(Collectors.toList());

        final Master master = new Master(files, 10);
        while (!master.isDone()) {
            TimeUnit.SECONDS.sleep(1);
        }
    }

}
