package com.yoozoo.ptools.plugin;

import java.util.Set;

import com.facebook.presto.spi.Plugin;
import com.google.common.collect.ImmutableSet;
import com.yoozoo.ptools.udf.GetDbcloudPermission;
import com.yoozoo.ptools.udf.LocateIpwenwen;

public class PrestoFunctionsPlugin implements Plugin{
	
	@Override
	public Set<Class<?>> getFunctions()
    {
        return ImmutableSet.<Class<?>>builder()
                .add(GetDbcloudPermission.class)
                .add(LocateIpwenwen.class)
//                .add(TestDbcloudPermission.class)
//                .add(ExampleNullFunction.class)
                .build();
    }
}
