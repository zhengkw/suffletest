package com.zhengkw.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName:WordcountMapper
 * @author: zhengkw
 * @description: mapper
 * @date: 20/02/24上午 8:42
 * @version:1.0
 * @since: jdk 1.8
 */
public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text k = new Text();
    IntWritable v = new IntWritable(1);

    /**
     * @param key
     * @param value
     * @param context
     * @descrption:重写map 实现wordcount
     * @return: void
     * @date: 20/02/24 上午 8:47
     * @author: zhengkw
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 获取一行
        String line = value.toString().trim();
        // 2 切割
        String[] words = line.split(" ");

        // 3 输出
        for (String word : words
        ) {
            k.set(word);
           //期望输出的是<hadoop,1> --> <string,int>
            context.write(k, v);
        }

    }
}
