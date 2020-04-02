package com.zhengkw.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName:WordCountCombiner
 * @author: zhengkw
 * @description: 提前合并
 * @date: 20/02/27上午 11:41
 * @version:1.0
 * @since: jdk 1.8
 */
public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

    int sum;
    IntWritable v = new IntWritable();

    /**
     * @param key
     * @param values
     * @param context
     * @descrption:
     * @return: void
     * @date: 20/02/24 上午 8:51
     * @author: zhengkw
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // 1 累加求和
        for (IntWritable value : values
        ) {
            sum += value.get();
        }

        // 2 输出
        v.set(sum);
        context.write(key, v);

    }
}
