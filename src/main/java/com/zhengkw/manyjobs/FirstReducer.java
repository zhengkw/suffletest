package com.zhengkw.manyjobs;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName:FirstReducer
 * @author: zhengkw
 * @description: 期望zhengkw  ，（filename  N）
 * @date: 20/02/28下午 11:32
 * @version:1.0
 * @since: jdk 1.8
 */
public class FirstReducer extends Reducer<Text, Text, Text, Text> {
    //Text keyout = new Text();
    Text valueout = new Text();


    /**
     * @param key     zhengkw
     * @param values  filename 1
     * @param context
     * @descrption: 期望(zhengkw filename)  N
     * @return: void
     * @date: 20/02/29 上午 12:08
     * @author: zhengkw
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //整合求和
        int sum = 0;
        for (Text value : values) {
            sum += Integer.parseInt(value.toString());
        }
        //zhengkw \t filename \t N

        valueout.set(String.valueOf(sum));
        context.write(key, valueout);

    }
}
