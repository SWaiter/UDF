package com.yoozoo.ptools.udf.util;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import io.airlift.slice.Slice;
import static io.airlift.slice.Slices.utf8Slice;

public class TestUtil {

	public static void main(String[] args) {
		
		String aa = "_end.*_bbb";
		System.out.println("_endaaa_bbb".matches(aa));
	}

}
