package com.zhengkw.outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ClassName:OutTxtOutputFormat
 * @author: zhengkw
 * @description:
 * @date: 20/02/27下午 4:16
 * @version:1.0
 * @since: jdk 1.8
 */
public class OutTxtOutputFormat extends FileOutputFormat<Text, NullWritable> {
    @Override
    public RecordWriter getRecordWriter(TaskAttemptContext Context) throws IOException, InterruptedException {
        OutTxtRecordWriter otrw = new OutTxtRecordWriter(Context);

        return otrw;
    }
}
