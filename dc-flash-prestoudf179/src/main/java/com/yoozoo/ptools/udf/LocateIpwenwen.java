package com.yoozoo.ptools.udf;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.yoozoo.ptools.service.IpwenwenService;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.commons.lang.StringUtils;

public final class LocateIpwenwen
{
  @Description("getlocation _FUNC_(ip) - return ip location if have ip location Otherwise return null.")
  @ScalarFunction("getlocation")
  @LiteralParameters({"x"})
  @SqlType("varchar")
  public static Slice locateIpWenwen(ConnectorSession session, @SqlType("varchar(x)") Slice ip)
  {
    if (ip == null) {
      return Slices.utf8Slice("Error IP");
    }
    String ipAdress = ip.toStringUtf8();
    IpwenwenService ipwenwenService = IpwenwenService.getInstance();
    String locate = ipwenwenService.locate_ip(ipAdress);
    if (StringUtils.isBlank(locate)) {
      Slices.utf8Slice("Not Found.");
    }
    return Slices.utf8Slice(locate);
  }
}
