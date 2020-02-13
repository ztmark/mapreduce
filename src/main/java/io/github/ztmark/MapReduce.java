package io.github.ztmark;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

/**
 * @Author: Mark
 * @Date : 2020/2/13
 */
public interface MapReduce {


    List<KeyValue> map(String filename, String content) throws IOException;

    String reduce(String key, List<String> values);

}
