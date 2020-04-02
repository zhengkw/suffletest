package com.zhengkw.reducejoinbyteacher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ClassName:OrderJionDriver
 * @author: zhengkw
 * @description:
 * @date: 20/02/27下午 11:33
 * @version:1.0
 * @since: jdk 1.8
 */
public class OrderJionDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 输入路径
        Path inputPath = new Path("F:/mrinput/reducejoin");
        // 输出路径
        Path outputPath = new Path("f:/output5");

        Configuration conf = new Configuration();

        //判断输出路径是否已经存在 存在则删除
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf);


        job.setMapperClass(ReduceJoinMapper.class);
        job.setReducerClass(JoinReducer.class);
        job.setPartitionerClass(JoinPartition.class);
        //设置reduceTask的个数
        job.setNumReduceTasks(1);


        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(OrderBean.class);
        job.setOutputValueClass(OrderBean.class);
        job.setOutputKeyClass(NullWritable.class);

        //设置文件的输入输出路径
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
