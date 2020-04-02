package com.zhengkw.topn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName:TopNMapper
 * @author: zhengkw
 * @description: 按照手机号求和以后总流量倒序排序(mapper截取封装)
 * @date: 20/02/27上午 9:49
 * @version:1.0
 * @since: jdk 1.8
 */
public class TopNMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    FlowBean k = new FlowBean();
    //tel
    Text v = new Text();

    //13509468723	117684	110349	228033
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] val = line.split("\t");
        v.set(val[0]);
        k.setUpFlow(Long.parseLong(val[1]));
        k.setDownFlow(Long.parseLong(val[2]));
        k.setTotalFlow(Long.parseLong(val[3]));

        context.write(k, v);
    }
}
