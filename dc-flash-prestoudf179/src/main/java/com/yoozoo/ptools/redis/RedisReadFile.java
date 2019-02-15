package com.yoozoo.ptools.redis;

import com.yoozoo.ptools.service.IpwenwenService;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.commons.lang.StringUtils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public final class RedisReadFile
{
  private static String ADDR = "10.7.32.86";
  private static Integer PORT = Integer.valueOf(6379);
  private static String AUTH = null;
  private static String IP_WENWEN_FILE_KEY = "PRESTO_UDF_IP_youzu_single_wgs.dat";
  private static int TIME_OUT = 315360000;
  private static Integer MAX_TOTAL = Integer.valueOf(1024);
  private static Integer MAX_IDLE = Integer.valueOf(200);
  private static Integer MAX_WAIT_MILLIS = Integer.valueOf(10000);
  private static Integer TIMEOUT = Integer.valueOf(30000);
  private static Boolean TEST_ON_BORROW = Boolean.valueOf(true);
  private static JedisPool jedisPool = null;
  
  static
  {
    try
    {
      JedisPoolConfig config = new JedisPoolConfig();
      
      config.setMaxTotal(MAX_TOTAL.intValue());
      config.setMaxIdle(MAX_IDLE.intValue());
      config.setMaxWaitMillis(MAX_WAIT_MILLIS.intValue());
      config.setTestOnBorrow(TEST_ON_BORROW.booleanValue());
      jedisPool = new JedisPool(config, ADDR, PORT.intValue(), TIMEOUT.intValue(), AUTH);
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }
  
  public static synchronized Jedis getJedis()
  {
    try
    {
      if (jedisPool != null) {
        return jedisPool.getResource();
      }
      return null;
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
    return null;
  }
  
  public static byte[] getRedisFile()
  {
    Jedis jedis = getJedis();
    byte[] ret = jedis.get(IP_WENWEN_FILE_KEY.getBytes());
    returnResource(jedis);
    return ret;
  }
  
  public static void putRedisFile(byte[] value)
  {
    Jedis jedis = getJedis();
    jedis.set(IP_WENWEN_FILE_KEY.getBytes(), value);
    returnResource(jedis);
  }
  
  public static void delRedisValue()
  {
    Jedis jedis = getJedis();
    jedis.del(IP_WENWEN_FILE_KEY.getBytes());
    returnResource(jedis);
  }
  
  public static void returnResource(Jedis jedis)
  {
    if (jedis != null) {
      jedisPool.returnResource(jedis);
    }
  }
  
  public static byte[] readFileToByte(File file) {
      Long filelength = file.length();     //获取文件长度
      byte[] filecontent = new byte[filelength.intValue()];
      try {
          FileInputStream in = new FileInputStream(file);
          in.read(filecontent);
          in.close();
      } catch (FileNotFoundException e) {
          e.printStackTrace();
      } catch (IOException e) {
          e.printStackTrace();
      }
      return filecontent;
  }
  
  public static void main(String[] argc)
  {
    IpwenwenService ipwenwenService = IpwenwenService.getInstance();
    String locate = ipwenwenService.locate_ip("61.174.15.215");
    System.out.println("locate:" + locate);
//	  //read redis file
//	  byte[] in = RedisReadFile.getRedisFile();
//	  byte[] in = RedisReadFile.readFileToByte(new File("/Users/zengrongjun/Desktop/youzuspace/ipwenwen/IP_youzu_single_wgs.dat"));
//	  String result = new String(in);
//	  if(StringUtils.isNotBlank(result)) {
//		  System.out.println("length:"+result.length());
//	  } else {
//		  System.out.println("redis file is empty");
//	  }
	  //delete redis file
	  //RedisReadFile.delRedisValue();
	  //put redis file
	  //RedisReadFile.putRedisFile(in);
  }
}
