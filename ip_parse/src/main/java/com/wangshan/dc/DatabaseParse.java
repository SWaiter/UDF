package com.wangshan.dc;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.Country;
import com.wangshan.dc.util.Constants;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

/**
 * Created by uwangshan@163.com on 2019/1/31.
 */
public class DatabaseParse extends UDF {
    //    private static Logger log = Logger.getLogger(IP2Region.class);

    private static DatabaseReader reader = null;

    static {
        String path = "src/test/resources/GeoIP/";
//        String path = "/home/datacenter/";
        try {
            File dbfile = new File(path + Constants.geoIPFile);
            reader = new DatabaseReader.Builder(dbfile).build();
        } catch (IOException e) {
//            log.error(e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @SuppressWarnings("finally")
    public static String evaluate(String ip) {
        try {
            InetAddress ipAddress = InetAddress.getByName(ip);

// Replace "city" with the appropriate method for your database, e.g.,
// "country".
            CityResponse response = reader.city(ipAddress);

            Country country = response.getCountry();
            System.out.println(country.getIsoCode());            // 'US'
            System.out.println(country.getName());               // 'United States'
            System.out.println(country.getNames().get("zh-CN")); // '美国'
//            return country.getIsoCode() + "|" + country.getNames().get("zh-CN");

        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

//    public static void main(String[] args) {
//        System.out.println(evaluate("78.26.70.208"));
//    }
    public static void main(String[] args) {
        System.out.println(evaluate("61.174.15.215"));
    }
}
