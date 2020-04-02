package com.zhengkw.elt;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName:ETLMapper
 * @author: zhengkw
 * @description:
 * @date: 20/03/09下午 8:55
 * @version:1.0
 * @since: jdk 1.8
 */
public class ETLMapper extends Mapper<LongWritable, Text, String, NullWritable> {
    /**
     * @param key
     * @param value
     * @param context
     * @descrption: 通过工具类清洗数据再输出
     * @return: void
     * @date: 20/03/09 下午 9:14
     * @author: zhengkw
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //通过工具类清洗数据
        String result = ETLUtil.parse(value.toString());
      //如果是脏数据，则跳过
       if(result==null){
           return;
       }

        context.write(result, NullWritable.get());
    }


}
