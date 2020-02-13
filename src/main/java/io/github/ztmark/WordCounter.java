package io.github.ztmark;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Mark
 * @Date : 2020/2/13
 */
public class WordCounter implements MapReduce {
    @Override
    public List<KeyValue> map(String filename, String content) throws IOException {
        List<KeyValue> res = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(filename)))) {
            String line;
            while ((line = reader.readLine()) != null) {
                final KeyValue keyValue = new KeyValue(line.trim(), "1");
                res.add(keyValue);
            }
        }

        return res;
    }

    @Override
    public String reduce(String key, List<String> values) {
        return String.valueOf(values.size());
    }
}
