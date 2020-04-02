package com.zhengkw.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static java.lang.System.out;

/**
 * @ClassName:MapJoinMapper
 * @author: zhengkw
 * @description:
 * @date: 20/02/28下午 7:14
 * @version:1.0
 * @since: jdk 1.8
 */
public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    //创建容器
    Map<String, String> pdMap = new HashMap();

    /**
     * @param context
     * @descrption: 此阶段在map执行之前，可以将小表利用分布式缓存
     * 进行存储 给每个maptask在运行之前能把小文件读取到
     * 解决一个job从hdfs上下载一次，减少并发时对hdfs读取数据线程占用
     * @return: void
     * @date: 20/02/28 下午 7:24
     * @author: zhengkw
     */
    @Override
    protected void setup(Context context) throws IOException {

        FileSystem fs = FileSystem.get(context.getConfiguration());


        //创建分布式缓存将文件上传 需要在main中声明文件uri
        URI[] cacheFiles = context.getCacheFiles();
        //按行读取数据  bufferreader  遍历缓存文件的所有uri
        for (URI file : cacheFiles) {
            FSDataInputStream is = fs.open(new Path(file.toString()));
            //读取文件最后判断是否读完
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            String line = "";
            //判断是否读到内容并且赋值
            while (!StringUtils.isBlank(line = reader.readLine())) {

                //分割该行  //pd.txt  pid 01字符串    pname 小米字符串
                String[] words = line.trim().split("\t");
                //将值封装成一个kv对 直接提供给map
                pdMap.put(words[0], words[1]);
            }
        }


    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        Text keyout = new Text();

        //传入来的order.txt  orderid pid amount
        String lineOrder = value.toString();
        String[] words = lineOrder.trim().split("\t");
        //orderid pid amount
        //通过pid获取pname
        String a = pdMap.get("01");
        //    out.println("----------======"+a);
        keyout.set(words[0] + "\t" + pdMap.get(words[1]) + "\t" + words[2]);
        context.write(keyout, NullWritable.get());


    }
}
