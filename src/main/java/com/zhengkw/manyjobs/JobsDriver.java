package com.zhengkw.manyjobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ClassName:JobsDriver
 * @author: zhengkw
 * @description:
 * @date: 20/02/29上午 12:48
 * @version:1.0
 * @since: jdk 1.8
 */
public class JobsDriver {
    public static void main(String[] args) throws Exception {

        //定义路径
        Path input = new Path("F:/mrinput/manyjobs");
        Path output = new Path("F:\\output8");
        Path finaloutput = new Path("F:\\outputfina");

        // 先创建两个Job，分配对Job进行配置
        // Job1
        Configuration conf1 = new Configuration();
        FileSystem fs = FileSystem.get(conf1);
        if (fs.exists(output)) {
            fs.delete(output, true);
        } else if (fs.exists(finaloutput)) {
            fs.delete(finaloutput, true);
        }

        Job job1 = Job.getInstance(conf1);

        // job1.setJobName("job1");

        job1.setMapperClass(FirstMapper.class);
        job1.setReducerClass(FirstReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job1, input);
        FileOutputFormat.setOutputPath(job1, output);

        //job2
        Configuration conf2 = new Configuration();
        //设置Keyvalueinputformat的分隔符
        // conf2.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");

        Job job2 = Job.getInstance(conf2);

        //  job2.setJobName("job2");

        job2.setMapperClass(SecondMapper.class);
        job2.setReducerClass(SecondReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setInputFormatClass(KeyValueTextInputFormat.class);

        FileInputFormat.setInputPaths(job2, output);
        FileOutputFormat.setOutputPath(job2, finaloutput);

        //构建ControledJob
        //不能将conf1直接传入
        ControlledJob controlledJob1 = new ControlledJob(job1.getConfiguration());
        ControlledJob controlledJob2 = new ControlledJob(job2.getConfiguration());

        //指定依赖关系
        controlledJob2.addDependingJob(controlledJob1);

        // 创建JobControl(是个线程)
        JobControl jobControl = new JobControl("JOb1");

        // 添加job到JobControl
        jobControl.addJob(controlledJob1);
        jobControl.addJob(controlledJob2);

        //运行JobControl
        Thread thread = new Thread(jobControl);

        //设置其为守护线程
        thread.setDaemon(true);

        //启动线程
        thread.start();

        //判断线程是否运行结束
        while (true) {

            if (jobControl.allFinished()) {

                //查看哪些任务成功
                System.out.println(jobControl.getSuccessfulJobList());

                break;

            }

            Thread.sleep(1500);
        }

        System.out.println("运行结束！");


    }


}
