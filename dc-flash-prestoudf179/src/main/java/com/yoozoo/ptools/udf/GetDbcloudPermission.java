package com.yoozoo.ptools.udf;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.alibaba.fastjson.JSON;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import com.yoozoo.ptools.udf.util.Util;

import io.airlift.slice.Slice;

public final class GetDbcloudPermission {
	public static final String DBCLOUD_PERMISSION = "dbcloud_permission";
	
	@Description("get_dbcloud_permission _FUNC_(db_name, table_name, field_name ,value) - return 1 if have permission to select "+ "this row,else 0.")
    @ScalarFunction("get_dbcloud_permission")
	@LiteralParameters({"x","y","z","w"})
    @SqlType(StandardTypes.INTEGER)
    public static long getDbcloudPermission(ConnectorSession session, @SqlType("varchar(x)") Slice db_name, @SqlType("varchar(y)") Slice table_name,
    		@SqlType("varchar(z)") Slice field_name, @SqlType("varchar(w)") Slice valuein)
    {
		if(db_name == null || table_name == null ||
				field_name == null || valuein == null){
			return 0;
		}
		String dbName = db_name.toStringUtf8();
		String tableName = table_name.toStringUtf8();
		String fieldName = field_name.toStringUtf8();
		String valueStr = valuein.toStringUtf8();
		if(StringUtils.isBlank(dbName) || StringUtils.isBlank(tableName) 
				|| StringUtils.isBlank(fieldName) || StringUtils.isBlank(valueStr)){
			return 0;
		}
		String var = dbName+Util.DOT+tableName+Util.DOT+Util.AUTH_PERMISSION_PREFIX+Util.UNDERLINE+fieldName;
        String permission = session.getSystemProperty(DBCLOUD_PERMISSION, String.class);
        try {
			permission = URLDecoder.decode(permission,"utf-8");
		} catch (UnsupportedEncodingException e) {
			//TODO
		}
        @SuppressWarnings("unchecked")
		Map<String,String> jsonObj = (Map<String,String>)JSON.parse(permission);
        String values = jsonObj.get(var);
        if(StringUtils.isNotBlank(values)){
			String[] list_values = values.split("\\"+Util.VERTICAL);
			for(String str:list_values){
				if(valueStr.equals(str)){
					return 1;
				}
			}
			//不在限制范围内
			return 0;
		} 
        return 1;
    }
}
