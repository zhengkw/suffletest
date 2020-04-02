package com.zhengkw.outputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName:OutTxtMapper
 * @author: zhengkw
 * @description:
 * @date: 20/02/27下午 4:05
 * @version:1.0
 * @since: jdk 1.8
 */
public class OutTxtMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    // Text line = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


        context.write(NullWritable.get(), value);
    }
}
