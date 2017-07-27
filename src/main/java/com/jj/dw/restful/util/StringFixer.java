package com.jj.dw.restful.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by weizh on 2016/8/16.
 */
public class StringFixer {
    public static Object structFixer(String str) {
        Map<Object, Object> map  = new HashMap<Object, Object>();
        if(str.contains(":struct") && str.contains("{")){
            //str = str.replaceAll("\\s+"," ");
            str = str.replaceAll(":.*\\{",":");
            String [] arr = str.split(":");
            String [] subArr = arr[1].replaceAll("\\}","").split(",");
            List<Object> list = new ArrayList<Object>();
            for(String substr:subArr) {
                String[] sa = substr.split("\\s+");
                Map<String, String> tmpmap = new HashMap<String, String>();
                tmpmap.put("type",sa[1]);
                tmpmap.put("name",sa[2]);
                list.add(tmpmap);
            }
            map.put(arr[0],list);
        }


        return map;
    }

    public static void main(String [] args ) {
        String s = "partitionColumns:struct partition_columns { string year, string month, string day}";
        structFixer(s);
    }
}
