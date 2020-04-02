package com.zhengkw.groupingcomparator;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

/**
 * @ClassName:OrderDriver
 * @author: zhengkw
 * @description:
 * @date: 20/02/27下午 3:10
 * @version:1.0
 * @since: jdk 1.8
 */
public class OrderDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        // 输入路径
        Path inputPath = new Path("F:\\mrinput\\groupcomparator");
        // 输出路径
        Path outputPath = new Path("f:/output4");

        Configuration conf = new Configuration();

        //判断输出路径是否已经存在 存在则删除
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        // 创建job
        Job job = Job.getInstance(conf);
        //设置3个类
        job.setJarByClass(OrderDriver.class);
        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);
        job.setGroupingComparatorClass(OrderGroupComparator.class);

        //设置2个阶段的K V类
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        //指定最终输出的 KV 类
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(OrderBean.class);


        //设置输入输出路径
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        //提交job true打印job信息  获取返回值退出程序
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);

    }
}
