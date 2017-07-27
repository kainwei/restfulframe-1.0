package com.jj.dw.restful.resource;

import com.jj.dw.ServiceLogic.TableOps;
import com.jj.dw.dbClient.ClientFactory;
import com.jj.dw.dbClient.MyClient;
import com.jj.dw.dbClient.ToJson;
import com.jj.dw.restful.util.GlobalConf;
import com.jj.dw.restful.util.JsonExtractor;
import com.jj.dw.restful.util.PageTools;
import org.apache.hadoop.conf.Configuration;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.util.List;

/**
 * Created by weizh on 2016/8/17.
 */

@Path("subject")
public class subject {
    GlobalConf gc = GlobalConf.getGlobalConf();
    Configuration conf = gc.conf;
    String mySqlUrl = conf.get("com.jj.dw.mysql","ip_instead_tmp:3306");
    String storeDb = conf.get("com.jj.dw.mysql.db", "WZH");
    String subjectTabName = conf.get("com.jj.dw.mysql.subjectTableName","subject");
    String virtual_dbTabName = conf.get("com.jj.dw.mysql.virtual_dbTableName","virtual_db");
    String tableTabName = conf.get("com.jj.dw.mysql.tableTableName","table");
    String url = "jdbc:mysql://" + mySqlUrl;

    MyClient mj = new ClientFactory().createClient("mysqljdbc") ;
    String errorInfo = null;

    /**
     * "subject" Table Structrue :
     * id | name | comment
     * restful sample:
     * [Path]/{"name":"product","comment":"all of the products"}
     **/
    @GET
    @Path("{subject}")
    @Produces(MediaType.APPLICATION_JSON)
    public String ShowSubject(@PathParam("subject") String subject,
                             @Context org.glassfish.grizzly.http.server.Request request) {


        String name = JsonExtractor.getStrVal(subject, "name");
        //String comment = JsonExtractor.getStrVal(subject, "comment");
        if(name == null) {
            errorInfo = "get subject name failed";
            return errorInfo;
        }

        String sql = "select * from " + storeDb + "." + subjectTabName + " where name='" + name + "'";
        List<Object> res  = mj.output(url,sql);
        String logInfo = mySqlUrl + " " + sql;
        System.out.println(PageTools.printLog(PageTools.getIpAddr(request), logInfo));
        return  ToJson.toJson(res)  ;

    }



    @POST
    @Path("{subject}")
    @Produces(MediaType.APPLICATION_JSON)
    public String AddSubject(@PathParam("subject") String subject,
                              @Context org.glassfish.grizzly.http.server.Request request) {

        String name = JsonExtractor.getStrVal(subject, "name");
        String comment = JsonExtractor.getStrVal(subject, "comment");
        if(comment == null ) {
            comment = "";
        }
        if(name == null) {
            errorInfo = "get subject name failed";
            return errorInfo;
        }

        String sql = "insert into " + storeDb + "." + subjectTabName + "(name, comment) values ( '"
                + name +"', '" + comment + "')";

        List<Object> res  = mj.output(url,sql);
        String logInfo = mySqlUrl + " " + sql;
        System.out.println(PageTools.printLog(PageTools.getIpAddr(request), logInfo));
        return  ToJson.toJson(res)  ;
    }

    @DELETE
    @Path("{subject}")
    @Produces(MediaType.APPLICATION_JSON)
    public String DelSubject(@PathParam("subject") String subject,
                           @Context org.glassfish.grizzly.http.server.Request request) {

        String name = JsonExtractor.getStrVal(subject, "name");
        if(name == null) {
            errorInfo = "get subject name failed";
            return errorInfo;
        }

        String sql = "delete from " + storeDb + "." + subjectTabName + " where name='" + name + "'";

        List<Object> res  = mj.output(url,sql);
        String logInfo = mySqlUrl + " " + sql;
        System.out.println(PageTools.printLog(PageTools.getIpAddr(request), logInfo));

        /***
         * Delete sub_db row
         */
        String subId = TableOps.NameToId(subjectTabName,"name", name);
        boolean delRes = new TableOps().DelSubToDb(subId);

        return  ToJson.toJson(res)  ;

    }




}
