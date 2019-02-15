package com.wangshan.dc;

import com.maxmind.geoip.LookupService;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.File;
import java.io.IOException;

/**
 * Created by uwangshan@163.com on 2019/1/29.
 */
public class IP2Region extends UDF {
    //    private static Logger log = Logger.getLogger(IP2Region.class);

    private static LookupService  cl = null;

    static {
        String path = "src/test/resources/GeoIP/";
//        String path = "/home/datacenter/";
        try {
//            File dbfile = new File(path + Constants.geoIPFile);
            File dbfile = new File(path + "GeoIP.dat");
            cl = new LookupService(dbfile,
                    LookupService.GEOIP_MEMORY_CACHE | LookupService.GEOIP_CHECK_CACHE);
        } catch (IOException e) {
//            log.error(e.getMessage());
            e.printStackTrace();
        }

    }

    @SuppressWarnings("finally")
    public static String evaluate(String ip) {
        try {
            System.out.println(cl.getCountry(ip).getCode());
            System.out.println(cl.getCountry(ip).getName());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return cl.getCountry(ip).getCode()+"|"+cl.getCountry(ip).getName();
    }

    public static void main(String[] args) {
        System.out.println(evaluate("2404:6800:8005::68")) ;
    }
}
