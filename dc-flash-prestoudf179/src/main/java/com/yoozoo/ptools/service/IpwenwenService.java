package com.yoozoo.ptools.service;

import com.yoozoo.ptools.redis.RedisReadFile;
import java.io.File;
import java.io.FileInputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IpwenwenService
{
  public static class ResourceHolder
  {
    public static final IpwenwenService ipipService = new IpwenwenService();
  }
  
  private long base_len = 64L;
  private long offset_addr = 0L;
  private long offset_owner = 0L;
  private byte[] offset_infe;
  private static boolean isLoaded = false;
  private Pattern ip_re = Pattern.compile("^((25[0-5]|2[0-4]\\d|[01]?\\d\\d?)\\.){3}(25[0-5]|2[0-4]\\d|[01]?\\d\\d?)$");
  
  private String loadIpWenwenData()
  {
    if (isLoaded) {
      return "Loaded";
    }
    try
    {
      byte[] bytes = RedisReadFile.getRedisFile();
      
      byte[] asse = new byte[bytes.length - 16];
      byte[] asse1 = new byte[8];
      byte[] asse2 = new byte[8];
      System.arraycopy(bytes, 16, asse, 0, bytes.length - 16);
      System.arraycopy(bytes, 0, asse1, 0, 8);
      System.arraycopy(bytes, 8, asse2, 0, 8);
      this.offset_addr = bytesToLong(asse1);
      this.offset_owner = bytesToLong(asse2);
      this.offset_infe = asse;
      isLoaded = true;
      return "success";
    }
    catch (Exception e)
    {
      e.printStackTrace();
      return "Exception " + e;
    }
  }
  
  public static IpwenwenService getInstance()
  {
    return ResourceHolder.ipipService;
  }
  
  public long ipToLong(String ipAddress)
  {
    long result = 0L;
    String[] ipAddressInArray = ipAddress.split("\\.");
    for (int i = 3; i >= 0; i--)
    {
      long ip = Long.parseLong(ipAddressInArray[(3 - i)]);
      result |= ip << i * 8;
    }
    return result;
  }
  
  public static String bytetoChar(byte[] bytes, Integer src, Integer dst)
  {
    byte[] asses = new byte[dst.intValue()];
    System.arraycopy(bytes, src.intValue(), asses, 0, dst.intValue());
    String retuchr = new String(asses);
    return retuchr;
  }
  
  public static Integer bytetoInt(byte[] bytes, Integer src, Integer dst)
  {
    byte[] asses = new byte[dst.intValue()];
    System.arraycopy(bytes, src.intValue(), asses, 0, dst.intValue());
    Integer restu = Integer.valueOf(byteArrayToInt(asses));
    return restu;
  }
  
  public static int byteArrayToInt(byte[] b)
  {
    return b[0] & 0xFF | (b[1] & 0xFF) << 8 | (b[2] & 0xFF) << 16 | (b[3] & 0xFF) << 24;
  }
  
  public static long bytesToLong(byte[] b)
  {
    long l = b[7] << 56 | (b[6] & 0xFF) << 48 | (b[5] & 0xFF) << 40 | (b[4] & 0xFF) << 32 | (b[3] & 0xFF) << 24 | (b[2] & 0xFF) << 16 | (b[1] & 0xFF) << 8 | b[0] & 0xFF;
    
    return l;
  }
  
  public static long byteArrayToInt2(byte[] b)
  {
    long num = 0L;
    for (int ix = 3; ix >= 0; ix--)
    {
      num <<= 8;
      num |= b[ix] & 0xFF;
    }
    return num;
  }
  
  public static long bytetoInt2(byte[] bytes, Integer src, Integer dst)
  {
    byte[] asses = new byte[4];
    System.arraycopy(bytes, src.intValue(), asses, 0, 4);
    long restu = byteArrayToInt2(asses);
    return restu;
  }
  
  public String locate_ip(String ip)
  {
	  Matcher m = this.ip_re.matcher(ip);
      Long nip;
      if (m.find()) {
        nip = Long.valueOf(ipToLong(m.group(0)));
      } else {
        return "Error IP";
      }
      if (!isLoaded) {
        loadIpWenwenData();
      }
      long record_min = 0L;
      long record_max = this.offset_addr / this.base_len - 1L;
      long record_mid = (record_min + record_max) / 2L;
      while (record_max - record_min >= 0L) {
        long mult_re_ba_l = record_mid * this.base_len;
        Integer mult_re_ba = Integer.valueOf((int)mult_re_ba_l);
        
        Long minip = Long.valueOf(bytetoInt2(this.offset_infe, mult_re_ba, Integer.valueOf(4)));
        Long maxip = Long.valueOf(bytetoInt2(this.offset_infe, Integer.valueOf(mult_re_ba.intValue() + 4), Integer.valueOf(4)));
        if (nip.longValue() < minip.longValue())
        {
          record_max = record_mid - 1L;
        } else if ((nip.equals(minip)) | (nip > minip & nip < maxip) | (nip.equals(maxip))) {
            Integer addr_begin = bytetoInt(this.offset_infe, Integer.valueOf(mult_re_ba.intValue() + 8), Integer.valueOf(8));
            Integer addr_length = bytetoInt(this.offset_infe, Integer.valueOf(mult_re_ba.intValue() + 16), Integer.valueOf(8));
            Integer owner_begin = bytetoInt(this.offset_infe, Integer.valueOf(mult_re_ba.intValue() + 24), Integer.valueOf(8));
            Integer owner_length = bytetoInt(this.offset_infe, Integer.valueOf(mult_re_ba.intValue() + 32), Integer.valueOf(8));
            String wgs_lon = bytetoChar(this.offset_infe, Integer.valueOf(mult_re_ba.intValue() + 40), Integer.valueOf(12)).trim();
            String wgs_lat = bytetoChar(this.offset_infe, Integer.valueOf(mult_re_ba.intValue() + 52), Integer.valueOf(12)).trim();
            String addr_bundle = bytetoChar(this.offset_infe, addr_begin, addr_length).trim();
            String owner = bytetoChar(this.offset_infe, owner_begin, owner_length).trim();
            String sum_temp = addr_bundle + "|" + wgs_lon + "|" + wgs_lat + "|" + owner;
            return sum_temp;
        } else if (nip.longValue() > maxip.longValue()) {
            record_min = record_mid + 1L;
	     } else {
	        return "ERROR Case";
	     }
        record_mid = (record_min + record_max) / 2L;
      }
      return "Not Found.";
  }
  
  public static byte[] fileTobyte(String path)
  {
    try
    {
      FileInputStream in = new FileInputStream(new File(path));
      byte[] data = new byte[in.available()];
      in.read(data);
      in.close();
      return data;
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
    return null;
  }
}
