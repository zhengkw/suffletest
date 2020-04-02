package com.zhengkw.elt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * @ClassName:ETLDriver
 * @author: zhengkw
 * @description:
 * @date: 20/03/09下午 8:55
 * @version:1.0
 * @since: jdk 1.8
 */
public class ETLDriver {


    public static void main(String[] args) throws Exception {

        Path input = new Path("E:\\BaiduNetdiskDownload\\第三阶段\\Hive\\2.资料\\gulivideo\\video\\2008\\0222");
        Path output = new Path("e:/ETL");

            /*Path input=new Path("/mapjoin");
            Path output=new Path("/mroutput/mapjoin");*/

        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);
        

        if (fileSystem.exists(output)) {

            fileSystem.delete(output, true);

        }

        Job job = Job.getInstance(conf);

        //设置job
        job.setMapperClass(ETLMapper.class);

        //设置没有reduce阶段
        job.setNumReduceTasks(0);
        //设置输入和输出目录
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);
        job.waitForCompletion(true);
    }


}
