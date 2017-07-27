package com.jj.dw.restful.resource;


import com.jj.dw.dbClient.*;
import com.jj.dw.restful.util.PageTools;

import javax.ws.rs.*;

import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.util.List;
import java.util.Map;

/**
 * Created by weizh on 2016/8/12.
 */

@Path("hive")
public class hive {
    String myurl = null;
    String mydb = null;
    String mytable = null;
    MyClient mj = new ClientFactory().createClient("hivejdbc") ;
    //MyClient mj = new MyJdbc();
    String driverName = "org.apache.hive.jdbc.HiveDriver";
    String user = "hdfs";
    String password = "1234";


    @GET
    @Path("{url}/{db}/{table}")
    @Produces(MediaType.TEXT_PLAIN)
    public String getIt(@PathParam("url") String url, @PathParam("db") String db, @PathParam("table") String table,
                        @Context org.glassfish.grizzly.http.server.Request request) {
        url = "jdbc:hive2://" + url;
        //String sql = "desc " + db + "." + table;
        //show table EXTENDED like match_detail
        String sql = "show table EXTENDED in " + db + " like " + table;
        String logInfo = url + " " + sql;
        List<Object> res  = mj.output(url,sql);
        Map<Object,List<Object>> map = AddJsonHead.addhead(res);
        System.out.println(PageTools.printLog(PageTools.getIpAddr(request), logInfo));
        return  ToJson.toJson(map)  ;
    }

    @GET
    @Path("{url}/{db}")
    @Produces(MediaType.TEXT_PLAIN)
    public String getDb(@PathParam("url") String url, @PathParam("db") String db,
                        @Context org.glassfish.grizzly.http.server.Request request) {
        url = "jdbc:hive2://" + url;
        String sql = "show tables in " + db;
        String logInfo = url + " " + sql;
        List<Object> res  = mj.output(url,sql);
        Map<Object,List<Object>> map = AddJsonHead.addhead(res);
        System.out.println(PageTools.printLog(PageTools.getIpAddr(request), logInfo));
        return  ToJson.toJson(map)  ;
    }

    @GET
    @Path("{url}")
    @Produces(MediaType.TEXT_PLAIN)
    public String getServer(@PathParam("url") String url, @Context org.glassfish.grizzly.http.server.Request request) {
        url = "jdbc:hive2://" + url;
        String sql = "show databases";
        String logInfo = url + " " + sql;
        List<Object> res  = mj.output(url,sql);
        Map<Object,List<Object>> map = AddJsonHead.addhead(res);
        System.out.println(PageTools.printLog(PageTools.getIpAddr(request), logInfo));
        return  ToJson.toJson(map)  ;
    }





}
