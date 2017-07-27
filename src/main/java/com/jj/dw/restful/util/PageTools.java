package com.jj.dw.restful.util;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by weizh on 2016/8/18.
 */
public class PageTools {
    public static String printLog(String ip, String sql ){
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        StringBuffer res =  new StringBuffer();
        res.append("[" + df.format(new Date()) + "] "
                + ip + " " + sql );

        return res.toString();

    }

    public static String printInternalOps(String info ){
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        StringBuffer res =  new StringBuffer();
        res.append("[" + df.format(new Date()) + "] "
                + info );

        return res.toString();

    }

    public static String errorMsgMaker(String item, String item_name){
        String res = null;
        if(item.equals("subid")) {
            res = "Failed, Get subject : "+ item_name +"'s id failed!";

        }else if(item.equals("dbid")) {
            res = "Failed, Get subtitle : "+ item_name +"'s id failed!";
        }else if(item.equals("tabid")) {
            res = "Failed, Get table : " + item_name +"'s id failed!";
        }
        return res;
    }

    public static String getIpAddr(org.glassfish.grizzly.http.server.Request request) {
        String ip = request.getHeader(" x-forwarded-for ");
        if (ip == null || ip.length() == 0 || " unknown ".equalsIgnoreCase(ip)) {
            ip = request.getHeader(" Proxy-Client-IP ");
        }
        if (ip == null || ip.length() == 0 || " unknown ".equalsIgnoreCase(ip)) {
            ip = request.getHeader(" WL-Proxy-Client-IP ");
        }
        if (ip == null || ip.length() == 0 || " unknown ".equalsIgnoreCase(ip)) {
            ip = request.getRemoteAddr();
        }
        return ip;
    }
}
