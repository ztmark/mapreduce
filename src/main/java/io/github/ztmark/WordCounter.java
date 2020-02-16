package io.github.ztmark;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import io.github.ztmark.common.KeyValue;

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

                final List<String> words = splitToWord(line);
                words.forEach(w -> {
                    final KeyValue keyValue = new KeyValue(w, "1");
                    res.add(keyValue);
                });
            }
        }

        return res;
    }

    private List<String> splitToWord(String line) {
        List<String> words = new ArrayList<>();
        if (line != null && !"".equals(line)) {
            final String[] split = line.split(" ");
            for (String s : split) {
                words.addAll(extractWord(s.trim()));
            }
        }
        return words;
    }

    private List<String> extractWord(String s) {
        List<String> res = new ArrayList<>();
        if (s != null && !"".equals(s)) {
            final int length = s.length();
            boolean meetValid = false;
            int startIndex = -1;
            for (int i = 0; i < length; i++) {
                if (Character.isLetter(s.charAt(i))) {
                    if (!meetValid) {
                        startIndex = i;
                        meetValid = true;
                    }
                } else {
                    if (meetValid) {
                        res.add(s.substring(startIndex, i));
                        meetValid = false;
                    }
                    startIndex = -1;
                }
            }
            if (meetValid) {
                res.add(s.substring(startIndex, length));
            }
        }
        return res;
    }

    @Override
    public String reduce(String key, List<String> values) {
        return String.valueOf(values.size());
    }
}
