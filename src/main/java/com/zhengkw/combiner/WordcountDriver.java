package com.zhengkw.combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @descrption:
 * @return:
 * @date: 20/02/24 上午 8:53
 * @author: zhengkw
 */
public class WordcountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 输入路径
        Path inputPath = new Path("F:\\mrinput\\combine");
        // 输出路径
        Path outputPath = new Path("f:/output3");

        Configuration conf = new Configuration();

        //判断输出路径是否已经存在 存在则删除
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }


        //用配置文件反射实例化job对象
        Job job = Job.getInstance(conf);

        // 2 设置jar加载路径
        job.setJarByClass(WordcountDriver.class);

        // 3 设置map和reduce类
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordcountReduce.class);
        job.setCombinerClass(WordCountCombiner.class);
        // 4 设置map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // 7 提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);

    }
}
