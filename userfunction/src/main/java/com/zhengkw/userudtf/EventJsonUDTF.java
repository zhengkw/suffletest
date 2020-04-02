package com.zhengkw.userudtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @ClassName:EventJsonUDTF
 * @author: zhengkw
 * @description: 自定义UDTF函数将数据展开
 * @date: 20/04/02下午 2:07
 * @version:1.0
 * @since: jdk 1.8
 */
public class EventJsonUDTF extends GenericUDTF {
    //  ①检查入参  ②返回UDTF返回的每行的每列的类型检查器
    //  Inspector： 检查器，检查员
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

        //获取UDTF传入的参数引用
        List<? extends StructField> inputFields = argOIs.getAllStructFieldRefs();

        //只能传入1个参数
        if (inputFields.size() != 1) {
            throw new UDFArgumentException("参数个数只能是1个！");
        }

        //检查类型是否是String类型  string指的是hive中的string类型
        if (!"string".equals(inputFields.get(0).getFieldObjectInspector().getTypeName())) {

            throw new UDFArgumentException("参数类型必须是string类型!");
        }

        // 返回的一行中的每一列的临时字段名
        List<String> fieldNames = new ArrayList<>();

        fieldNames.add("event_name");
        fieldNames.add("event_json");

        // 声明返回的一行中的每一列的字段类型检测器
        List<ObjectInspector> fieldOIs = new ArrayList<>();

        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);


        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
                fieldOIs);

    }

    // 完成函数的计算逻辑  b(line) 返回   2列N(参数中事件对象的个数)行
    // 参数： [{},{},{}]
    @Override
    public void process(Object[] objects) throws HiveException {

        //检查是否传入了参数
        if (objects[0] == null || objects.length == 0) {
            return;
        }

        //有参数，开始解析

        try {

            JSONArray jsonArray = new JSONArray(objects[0].toString());

            //取出JSON array中的每一个事件，取出事件中的每一个事件名称和事件的字符串
            for (int i = 0; i < jsonArray.length(); i++) {
                //声明一个数组，存放UDTF返回的结果
                try {
                    String[] result = new String[2];

                    //取出事件对应的json object对象
                    JSONObject eventJO = jsonArray.getJSONObject(i);

                    //获取事件名
                    result[0] = eventJO.getString("en");

                    result[1] = eventJO.toString();
                    //测试打印
//                 System.out.println(Arrays.asList(result));
                    forward(result);

                } catch (Exception e) {
                    // e.printStackTrace();
                    continue;
                }

            }
        } catch (JSONException e) {
            // e.printStackTrace();

        }

    }

    //最后关闭时，执行操作
    @Override
    public void close() throws HiveException {

    }
}
