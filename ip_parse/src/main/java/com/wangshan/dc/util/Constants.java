package com.wangshan.dc.util;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by uwangshan@163.com on 2019/1/29.
 */
public class Constants {
    public static final String geoIPFile= "geoip.mmdb";
    public static final String geollFile = "geo.csv";
    public static final Map<String,String> provinceCN = new HashMap<String,String>();

    static {
        provinceCN.put("AH","安徽");
        provinceCN.put("BJ","北京");
        provinceCN.put("CQ","重庆");
        provinceCN.put("FJ","福建");
        provinceCN.put("GD","广东");
        provinceCN.put("GS","甘肃");
        provinceCN.put("GX","广西");
        provinceCN.put("GZ","贵州");
        provinceCN.put("HA","河南");
        provinceCN.put("HB","湖北");
        provinceCN.put("HE","河北");
        provinceCN.put("HI","海南");
        provinceCN.put("HK","香港");
        provinceCN.put("HL","黑龙江");
        provinceCN.put("HN","湖南");
        provinceCN.put("JL","吉林");
        provinceCN.put("JS","江苏");
        provinceCN.put("JX","江西");
        provinceCN.put("LN","辽宁");
        provinceCN.put("MO","澳门");
        provinceCN.put("NM","内蒙古");
        provinceCN.put("NX","宁夏");
        provinceCN.put("QH","青海");
        provinceCN.put("SC","四川");
        provinceCN.put("SD","山东");
        provinceCN.put("SH","上海");
        provinceCN.put("SN","陕西");
        provinceCN.put("SX","山西");
        provinceCN.put("TJ","天津");
        provinceCN.put("TW","台湾");
        provinceCN.put("XJ","新疆");
        provinceCN.put("XZ","西藏");
        provinceCN.put("YN","云南");
        provinceCN.put("ZJ","浙江");
    }
}
