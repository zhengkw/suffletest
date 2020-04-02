package com.zhengkw.manyjobs;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName:SecondMapper
 * @author: zhengkw
 * @description:
 * @date: 20/02/28下午 11:34
 * @version:1.0
 * @since: jdk 1.8
 */
public class SecondMapper extends Mapper<Text, Text, Text, Text> {
    // Text keyout = new Text();
    Text valout = new Text();

    /**
     * @param key     zhengkw \t filename
     * @param value   N
     * @param context
     * @descrption: key zhengkw  val  filename N
     * @return: void
     * @date: 20/02/29 上午 12:20
     * @author: zhengkw
     */
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

     //   String[] keys = value.toString().trim().split("\t");
        // keyout.set(keys[0]);
        // valout.set(keys[0] + "\t" + );
        context.write(key, value);

    }
}
