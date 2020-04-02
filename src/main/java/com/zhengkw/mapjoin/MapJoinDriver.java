package com.zhengkw.mapjoin;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @ClassName:MapJoinDriver
 * @author: zhengkw
 * @description:
 * @date: 20/02/28下午 7:23
 * @version:1.0
 * @since: jdk 1.8
 */
public class MapJoinDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        Path input = new Path("F:/mrinput/reducejoin/order.txt");
        Path output = new Path("f:/output6");

        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(output)) {
            fileSystem.delete(output, true);
        }
        Job job = Job.getInstance(conf);

        //设置分布式缓存存放文件路径
        job.addCacheFile(new URI("file:///f:/mrinput/reducejoin/pd.txt"));
        //设置没有reduce阶段
        job.setNumReduceTasks(0);
///job.getCacheFiles()

        job.setMapperClass(MapJoinMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.waitForCompletion(true);


    }
}
