package com.zhengkw.userudf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * @ClassName:BaseFieldUDF
 * @author: zhengkw
 * @description: 自定义UDF函数解析公共字段
 * @date: 20/04/02下午 2:06
 * @version:1.0
 * @since: jdk 1.8
 */
public class BaseFieldUDF extends UDF {
    public String evaluate(String line, String key) throws JSONException, JSONException {

        String[] log = line.split("\\|");

        if (log.length != 2 || StringUtils.isBlank(log[1])) {
            return "";
        }

        JSONObject baseJson = new JSONObject(log[1].trim());

        String result = "";

        // 获取服务器时间
        if ("st".equals(key)) {
            result = log[0].trim();
        } else if ("et".equals(key)) {
            // 获取事件数组
            if (baseJson.has("et")) {
                result = baseJson.getString("et");
            }
        } else {
            JSONObject cm = baseJson.getJSONObject("cm");

            // 获取key对应公共字段的value
            if (cm.has(key)) {
                result = cm.getString(key);
            }
        }

        return result;
    }

}
