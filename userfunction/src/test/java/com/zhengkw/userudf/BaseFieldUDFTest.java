package com.zhengkw.userudf;

import org.json.JSONException;
import org.junit.Test;

import static org.junit.Assert.*;

public class BaseFieldUDFTest {

    @Test
    public void evaluate() throws JSONException {
        String line = "1583776223469|{\"cm\":{\"ln\":\"-48.5\",\"sv\":\"V2.5.7\",\"os\":\"8.0.9\",\"g\":\"6F76AVD5@gmail.com\",\"mid\":\"0\",\"nw\":\"4G\",\"l\":\"pt\",\"vc\":\"3\",\"hw\":\"750*1134\",\"ar\":\"MX\",\"uid\":\"0\",\"t\":\"1583707297317\",\"la\":\"-52.9\",\"md\":\"sumsung-18\",\"vn\":\"1.2.4\",\"ba\":\"Sumsung\",\"sr\":\"V\"},\"ap\":\"app\",\"et\":[{\"ett\":\"1583705574227\",\"en\":\"display\",\"kv\":{\"goodsid\":\"0\",\"action\":\"1\",\"extend1\":\"1\",\"place\":\"0\",\"category\":\"63\"}},{\"ett\":\"1583760986259\",\"en\":\"loading\",\"kv\":{\"extend2\":\"\",\"loading_time\":\"4\",\"action\":\"3\",\"extend1\":\"\",\"type\":\"3\",\"type1\":\"\",\"loading_way\":\"1\"}},{\"ett\":\"1583746639124\",\"en\":\"ad\",\"kv\":{\"activityId\":\"1\",\"displayMills\":\"111839\",\"entry\":\"1\",\"action\":\"5\",\"contentType\":\"0\"}},{\"ett\":\"1583758016208\",\"en\":\"notification\",\"kv\":{\"ap_time\":\"1583694079866\",\"action\":\"1\",\"type\":\"3\",\"content\":\"\"}},{\"ett\":\"1583699890760\",\"en\":\"favorites\",\"kv\":{\"course_id\":4,\"id\":0,\"add_time\":\"1583730648134\",\"userid\":7}}]}";

        String mid = new BaseFieldUDF().evaluate(line, "nw");

        System.out.println(mid);
    }


}