package com.zhengkw.userudtf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.Test;

import static org.junit.Assert.*;

public class BaseFieldUDFTest {

    @Test
    public void process() throws HiveException {
        String line = "[{\"ett\":\"1586595823882\",\"en\":\"ad\",\"kv\":{\"activityId\":\"1\",\"displayMills\":\"84470\",\"entry\":\"1\",\"action\":\"1\",\"contentType\":\"0\"}},{\"ett\":\"1586525960461\",\"en\":\"active_background\",\"kv\":{\"active_source\":\"3\"}}]\n";
        Object[] arr = new String[1];
        arr[0] = line;
        new EventJsonUDTF().process(arr);

    }


}