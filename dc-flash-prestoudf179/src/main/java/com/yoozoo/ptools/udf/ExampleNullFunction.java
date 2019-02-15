package com.yoozoo.ptools.udf;

import static io.airlift.slice.Slices.utf8Slice;

import org.apache.commons.lang.StringUtils;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;

import io.airlift.slice.Slice;

public class ExampleNullFunction {
	public static final String DBCLOUD_PERMISSION = "dbcloud_permission";
	@Description("get_permission_str _FUNC_(db_name, table_name, field_name ,value) - return 1 if have permission to select "+ "this row,else 0.")
    @ScalarFunction("get_permission_str")
	@LiteralParameters({"x","y","z","w"})
    @SqlType(StandardTypes.VARCHAR)
    public static Slice getDbcloudPermissionStr(ConnectorSession session, @SqlType("varchar(x)") Slice db_name, @SqlType("varchar(y)") Slice table_name,
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
        String persmission = session.getSystemProperty(DBCLOUD_PERMISSION, String.class);
        
        return utf8Slice(persmission);
    }
}
