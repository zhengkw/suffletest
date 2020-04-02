package com.zhengkw.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

import static java.lang.System.out;


/**
 * @ClassName:OutTxtRecordWriter
 * @author: zhengkw
 * @description:
 * @date: 20/02/27下午 4:17
 * @version:1.0
 * @since: jdk 1.8
 */
public class OutTxtRecordWriter extends RecordWriter<Text, NullWritable> {
    FileSystem fs = null;
    Path atguigu = new Path("f:/outputFormat/atguigu.log");
    Path other = new Path("f:/outputFormat/other.log");
    FSDataOutputStream ofsa = null;
    FSDataOutputStream ofso = null;

    public OutTxtRecordWriter() {
        super();
    }

    public OutTxtRecordWriter(TaskAttemptContext context) {
        this();
        //获取上下文中的conf信息创建一个文件系统对象
        try {
            fs = FileSystem.get(context.getConfiguration());

            //打开输出流将内容分类写到2个路径下
            ofsa = fs.create(atguigu);
            ofso = fs.create(other);


        } catch (IOException e) {
            e.printStackTrace();
            out.println("没有找到配置信息");
        }
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {


        String contents = text.toString();
        if (contents.contains(".atguigu.")) {
            ofsa.write(contents.getBytes());
        } else {
            ofso.write(contents.getBytes());
        }

    }

    @Override
    public void close(TaskAttemptContext Context) throws IOException, InterruptedException {
        IOUtils.closeStream(ofsa);
        IOUtils.closeStream(ofso);
    }
}
