package com.zhengkw.manyjobs;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName:SecondReducer
 * @author: zhengkw
 * @description: zhengkw   c.txt-->2 b.txt-->2 a.txt-->3
 * @date: 20/02/28下午 11:34
 * @version:1.0
 * @since: jdk 1.8
 */
public class SecondReducer extends Reducer<Text, Text, Text, Text> {
    //    Text keyout = new Text();
    Text valout = new Text();

    /**
     * @param key
     * @param values
     * @param context
     * @descrption:
     * @return: void
     * @date: 20/02/29 上午 12:37
     * @author: zhengkw
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuffer sb = new StringBuffer();
        for (Text value : values) {
            String words = value.toString();
            String[] word = words.split("\t");
            sb.append(word[0] + "-->" + word[1]);
            valout.set(String.valueOf(sb));

        }
        context.write(key, valout);


    }
}
