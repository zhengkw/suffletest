package com.zhengkw.manyjobs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @ClassName:FirstMapper
 * @author: zhengkw
 * @description:
 * @date: 20/02/28下午 11:31
 * @version:1.0
 * @since: jdk 1.8
 */
public class FirstMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Text keyout = new Text();
        Text valueout = new Text();

        //zhengkw zhengkw 空格分割
        String line = value.toString();
        String[] words = line.trim().split(" ");
        //期望拿到文件名  zhengkw  ，（filename  1）
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String filename = fileSplit.getPath().getName();
        //循环拼接
        for (String word : words) {
            keyout.set(word +"\t"+ filename);
            valueout.set("1");
            context.write(keyout, valueout);
        }

    }
}
