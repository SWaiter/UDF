package com.yoozoo.ptools.udf;

import static io.airlift.slice.Slices.utf8Slice;

import org.apache.commons.lang.StringUtils;

import com.alibaba.fastjson.JSONObject;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import com.yoozoo.ptools.udf.util.Util;

import io.airlift.slice.Slice;

public final class TestDbcloudPermission {
	public static final String DBCLOUD_PERMISSION = "dbcloud_permission";
	
	@Description("test_dbcloud_permission _FUNC_(db_name, table_name, field_name ,value) - return 1 if have permission to select "+ "this row,else 0.")
    @ScalarFunction("test_dbcloud_permission")
    @SqlType(StandardTypes.BIGINT)
    public static long getDbcloudPermission(@SqlType(StandardTypes.VARCHAR) Slice db_name, @SqlType(StandardTypes.VARCHAR) Slice table_name,
    		@SqlType(StandardTypes.VARCHAR) Slice field_name, @SqlType(StandardTypes.VARCHAR) Slice valuein)
    {
        return 1;
    }
	
	@Description("get_permission_value _FUNC_(db_name, table_name, field_name ,value) - return 1 if have permission to select "+ "this row,else 0.")
    @ScalarFunction("get_permission_value")
	@LiteralParameters({"x","y","z","w"})
    @SqlType(StandardTypes.VARCHAR)
    public static Slice getDbcloudPermissionValue(ConnectorSession session, @SqlType("varchar(x)") Slice db_name, @SqlType("varchar(y)") Slice table_name,
    		@SqlType("varchar(z)") Slice field_name, @SqlType("varchar(w)") Slice valuein)
    {
		if(db_name == null || table_name == null ||
				field_name == null || valuein == null){
			return utf8Slice("");
		}
		String dbName = db_name.toStringUtf8();
		String tableName = table_name.toStringUtf8();
		String fieldName = field_name.toStringUtf8();
		String valueStr = valuein.toStringUtf8();
		if(StringUtils.isBlank(dbName) || StringUtils.isBlank(tableName) 
				|| StringUtils.isBlank(fieldName) || StringUtils.isBlank(valueStr)){
			return utf8Slice("");
		}
		String var = dbName+Util.DOT+tableName+Util.DOT+Util.AUTH_PERMISSION_PREFIX+Util.UNDERLINE+fieldName;
        String persmission = session.getSystemProperty(DBCLOUD_PERMISSION, String.class);
        JSONObject jsonObj = (JSONObject) JSONObject.parse(persmission);
        String values = jsonObj.getString(var);
        return utf8Slice(values);
    }
	
	@Description("get_permission_key _FUNC_(db_name, table_name, field_name ,value) - return 1 if have permission to select "+ "this row,else 0.")
    @ScalarFunction("get_permission_key")
	@LiteralParameters({"x","y","z","w"})
    @SqlType(StandardTypes.VARCHAR)
    public static Slice getDbcloudPermissionKey(ConnectorSession session, @SqlType("varchar(x)") Slice db_name, @SqlType("varchar(y)") Slice table_name,
    		@SqlType("varchar(z)") Slice field_name, @SqlType("varchar(w)") Slice valuein)
    {
		if(db_name == null || table_name == null ||
				field_name == null || valuein == null){
			return utf8Slice("");
		}
		String dbName = db_name.toStringUtf8();
		String tableName = table_name.toStringUtf8();
		String fieldName = field_name.toStringUtf8();
		String valueStr = valuein.toStringUtf8();
		if(StringUtils.isBlank(dbName) || StringUtils.isBlank(tableName) 
				|| StringUtils.isBlank(fieldName) || StringUtils.isBlank(valueStr)){
			return utf8Slice("");
		}
		String var = dbName+Util.DOT+tableName+Util.DOT+Util.AUTH_PERMISSION_PREFIX+Util.UNDERLINE+fieldName;
        return utf8Slice(var);
    }
}
