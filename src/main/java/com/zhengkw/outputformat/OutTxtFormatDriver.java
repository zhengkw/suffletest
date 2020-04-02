package com.zhengkw.outputformat;


import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ClassName:OutTxtFormat
 * @author: zhengkw
 * @description:
 * @date: 20/02/27下午 5:10
 * @version:1.0
 * @since: jdk 1.8
 */
public class OutTxtFormatDriver {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        // 输入路径
        Path inputPath = new Path("F:\\mrinput\\outputformat");
        // 输出路径
        Path outputPath = new Path("f:/atguigu");

        Configuration conf = new Configuration();

        //判断输出路径是否已经存在 存在则删除
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }


        //用配置文件反射实例化job对象
        Job job = Job.getInstance(conf);

        // 2 设置jar加载路径
        job.setJarByClass(OutTxtFormatDriver.class);

        // 3 设置map和reduce类
        job.setMapperClass(OutTxtMapper.class);
        job.setReducerClass(OutTxtReducer.class);

        // 4 设置map输出
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(NullWritable.class);

        // 5 设置最终输出kv类型
        //  job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(OutTxtOutputFormat.class);

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, inputPath);
        //必须设置 job提交时会检验文件夹是否存在，不存在则不会提交job
        FileOutputFormat.setOutputPath(job, outputPath);

        // 7 提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
