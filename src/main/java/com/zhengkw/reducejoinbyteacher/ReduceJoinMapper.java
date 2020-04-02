package com.zhengkw.reducejoinbyteacher;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @ClassName:ReduceJoinMapper
 * @author: zhengkw
 * @description:
 * @date: 20/03/01下午 9:55
 * @version:1.0
 * @since: jdk 1.8
 */
public class ReduceJoinMapper extends Mapper<LongWritable, Text, NullWritable, OrderBean> {
    String fileName;
    OrderBean otb = new OrderBean();

    /**
     * @param context
     * @descrption: 通过FileSplit获取文件的路径
     * 方便map中区分内容来自哪个文件方便封装信息
     * @return: void
     * @date: 20/03/01 下午 10:06
     * @author: zhengkw
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        //获取文件名
        fileName = fileSplit.getPath().getName();

    }

    /**
     * @param key
     * @param value
     * @param context
     * @descrption: 通过setup阶段区分数据源后对数据封装，再通过分组，
     * 让相同pid的分到一个组进行join操作
     * @return: void
     * @date: 20/03/01 下午 10:06
     * @author: zhengkw
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.trim().split("\t");
        int length = words.length;
        //通过判断filename来判断读取到的内容来自哪里
        if (fileName.contains("order")) {
            // 1001	01	1
            otb.setOrderId(words[0]);
            otb.setpId(words[length - 2]);
            otb.setAmount(Integer.parseInt(words[length - 1]));
            otb.setSource(fileName);
            otb.setpName("nodata");

        } else {
            // 01	小米
            otb.setpId(words[0]);
            otb.setpName(words[length - 1]);
            otb.setOrderId("nodata");
            otb.setAmount(0);
            otb.setSource(fileName);
        }

        context.write(NullWritable.get(), otb);
    }

}
