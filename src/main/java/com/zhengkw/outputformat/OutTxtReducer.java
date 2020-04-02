package com.zhengkw.outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName:OutTxtReducer
 * @author: zhengkw
 * @description:
 * @date: 20/02/27下午 4:05
 * @version:1.0
 * @since: jdk 1.8
 */
public class OutTxtReducer extends Reducer<NullWritable, Text, Text, NullWritable> {

    Text k = new Text();

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            k.set(value + "\r\n");
            context.write(k, NullWritable.get());
        }
    }


}
