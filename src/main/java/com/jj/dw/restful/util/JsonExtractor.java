package com.jj.dw.restful.util;

import com.google.gson.Gson;

import java.util.List;
import java.util.Map;

/**
 * Created by weizh on 2016/8/19.
 */
public class JsonExtractor {

    public static String getStrVal(String json, String key) {
        Gson gson = new Gson();
        Map<String, Object> map = gson.fromJson(json, Map.class);
        List<Object> ma = gson.fromJson(json, List.class);
        //Map<String, Object> map = gson.fromJson(json, new TypeToken<Map<String,Object>>(){}.getType());
        //ReceiveData rd = gson.fromJson()
        if(map.containsKey(key)){
            try{
            return (String) map.get(key);
            }catch (ClassCastException e) {
                return gson.toJson(map.get(key));
            }
        }else{
            return null;
        }


    }
    public static void main(String [] args ) {
        String a = "{\"aaa\":[\"1\",\"2\"]}";
        System.out.println(getStrVal(a,"aaa"));
    }
}
