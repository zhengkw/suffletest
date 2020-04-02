package com.zhengkw.manyjobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

import java.io.IOException;

/**
 * @ClassName:jobsDriver2
 * @author: zhengkw
 * @description:
 * @date: 20/03/01下午 1:22
 * @version:1.0
 * @since: jdk 1.8
 */
public class jobsDriver2 {
    public static void main(String[] args) throws IOException {

        //定义路径
        Path input = new Path("F:/mrinput/manyjobs");
        Path output = new Path("F:\\output8");
        Path finaloutput = new Path("F:\\outputfina");

        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1);
        //为job1加载各种类信息
        job1.setMapperClass(FirstMapper.class);
        job1.setReducerClass(FirstReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        //job2
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2);
        job2.setMapperClass(SecondMapper.class);
        job2.setReducerClass(SecondReducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        //
        JobControl jobsControl = new JobControl("JobsTest");


        //设置2个作业的依赖关系

        ControlledJob controlledJob1 = new ControlledJob(conf1);
        ControlledJob controlledJob2 = new ControlledJob(conf2);

        boolean flag = controlledJob2.addDependingJob(controlledJob1);
        System.out.println(flag ? "成功绑定依赖" : "绑定失败");

      //  jobsControl.addJob();


    }


}
