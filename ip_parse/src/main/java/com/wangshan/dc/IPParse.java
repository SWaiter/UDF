package com.wangshan.dc;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.*;
import com.wangshan.dc.util.Constants;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by uwangshan@163.com on 2019/1/31.
 */
public class IPParse extends UDF {
    private static DatabaseReader reader = null;
    private static List<String> charCoder = new ArrayList<>();


    static {
        try {
            InputStream is= IPParse.class.getResourceAsStream("/"+Constants.geoIPFile);
            reader = new DatabaseReader.Builder(is).build();
        } catch (Exception e) {
            e.printStackTrace();
        }
        charCoder.add("en");
        charCoder.add("zh-CN");
    }

    public String evaluate(String ip) {
        return evaluate(ip, "zh-CN");
    }

    public String evaluate(String ip, String charsetName) {
        if (!charCoder.contains(charsetName)) {
            return "该编码字符不支持";
        }
        InetAddress ipAddress = null;
        try {
            ipAddress = InetAddress.getByName(ip);
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return "UnknownHost" + "(" + ip + ")";
        }
        String charsetName_ch = "en";
        StringBuilder sb = new StringBuilder("");
        try {
            CityResponse response = reader.city(ipAddress);
            //获取大陆
            Continent continent = response.getContinent();
            if (continent != null) {
                String continentName = continent.getNames().get(charsetName);
                sb.append(continentName != null ? continentName : continent.getNames().get(charsetName_ch));
            }
            sb.append("|");
            //获取国家信息
            Country country = response.getCountry();
            if (country != null) {
                sb.append(country.getIsoCode());
            }
            sb.append("|");
            if (country != null) {
                sb.append(country.getGeoNameId());
            }
            sb.append("|");
            if (country != null) {
                String countryName = country.getNames().get(charsetName);
                sb.append(countryName != null ? countryName : country.getNames().get(charsetName_ch));
            }
            sb.append("|");
            Subdivision subdivision = response.getMostSpecificSubdivision();
            if (subdivision != null) {
                String subdivisionName = subdivision.getNames().get(charsetName);
                sb.append(subdivisionName != null ? subdivisionName : subdivision.getNames().get(charsetName_ch));
            }
            sb.append("|");
            City city = response.getCity();
            if (city != null) {
                String provinceName = city.getNames().get(charsetName);
                sb.append(provinceName != null ? provinceName : city.getNames().get(charsetName_ch));
            }
            sb.append("|");
            sb.append("null");
            sb.append("|");
            Location location = response.getLocation();
//			获取经纬度，暂时不需要
            sb.append(location.getLongitude());
            sb.append("|");
            sb.append(location.getLatitude());
            sb.append("|");
            sb.append("null");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            return sb.toString();
        }
    }

    public static void main(String[] args) {
        System.out.println(new IPParse().evaluate("78.26.70.208"));
        System.out.println(new IPParse().evaluate("61.174.15.215","en"));
    }
}
