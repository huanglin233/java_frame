package com.hl.bigdata.flink.stream.vo;

/**
 * @author huanglin
 * @date 2024/07/03 14:45
 */
public class WordWithCount {

    public String word;
    public long   count;

    public WordWithCount(){};
    public WordWithCount(String word, long count) {
        this.word  = word;
        this.count = count;
    }

    @Override
    public String toString() {
        return "WordWithCount{" + "word='" + this.word + '\'' + ", count=" + count + '}';
    }
}
