package com.jj.dw.restful.resource;

import com.jj.dw.dbClient.ClientFactory;
import com.jj.dw.dbClient.MyClient;
import com.jj.dw.dbClient.ToJson;
import com.jj.dw.restful.util.GlobalConf;
import com.jj.dw.restful.util.PageTools;
import org.apache.hadoop.conf.Configuration;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.util.List;

/**
 * Created by weizh on 2016/8/31.
 */
@Path("mysql")
@Produces(MediaType.TEXT_PLAIN)
public class mysql {
    GlobalConf gc = GlobalConf.getGlobalConf();
    Configuration conf = gc.conf;
    String mySqlUrl = conf.get("com.jj.dw.mysql","ip_instead_tmp:3306");
    String storeDb = conf.get("com.jj.dw.mysql.db", "WZH");
    String subjectTabName = conf.get("com.jj.dw.mysql.subjectTableName","subject");
    String virtual_dbTabName = conf.get("com.jj.dw.mysql.virtual_dbTableName","virtual_db");
    String tableTabName = conf.get("com.jj.dw.mysql.tableTableName","table");
    String url = "jdbc:mysql://" + mySqlUrl;

    MyClient mj = new ClientFactory().createClient("mysqljdbc") ;

    @GET
    @Path("{sql}")
    public String getIt(@PathParam("sql") String sql,
                        @Context org.glassfish.grizzly.http.server.Request request) {
        List res = mj.output(url, sql);
        System.out.println(PageTools.printLog(PageTools.getIpAddr(request),
                sql + " " + url));
        return ToJson.toJson(res);

    }

}
