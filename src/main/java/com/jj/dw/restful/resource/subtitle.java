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

@Path("subtitle")
public class subtitle {
    GlobalConf gc = GlobalConf.getGlobalConf();
    Configuration conf = gc.conf;
    String mySqlUrl = conf.get("com.jj.dw.mysql","ip_instead_tmp:3306");
    String storeDb = conf.get("com.jj.dw.mysql.db", "WZH");
    String subjectTabName = conf.get("com.jj.dw.mysql.subjectTableName","subject");
    String virtual_dbTabName = conf.get("com.jj.dw.mysql.virtual_dbTableName","virtual_db");
    String tableTabName = conf.get("com.jj.dw.mysql.tableTableName","table");
    String subDbTabName = conf.get("com.jj.dw.mysql.sub_dbTabName","sub_db");
    String dbTabTabName = conf.get("com.jj.dw.mysql.db_tableTabName","db_table");

    String url = "jdbc:mysql://" + mySqlUrl;

    MyClient mj = new ClientFactory().createClient("mysqljdbc") ;
    String errorInfo = null;




    /**
     * "virtual_db" Table Structrue :
     *   | id | name |
     *   restful sample:
     *   [Path]/{"name":"subtitle","subject":"subjectName"}
     **/
    @GET
    @Path("{virtual_db}")
    @Produces(MediaType.APPLICATION_JSON)
    public String ShowSubtitle(@PathParam("virtual_db") String virtual_db,
                                @Context org.glassfish.grizzly.http.server.Request request) {

        String name = JsonExtractor.getStrVal(virtual_db, "name");
        if(name == null){
            errorInfo = "Get subtitle name failed";
            return errorInfo;
        }



        String sql = "select * from " + storeDb + "." + virtual_dbTabName + " where name='" +
                name + "'";
        List<Object> res  = mj.output(url,sql);
        String logInfo = mySqlUrl + " " + sql;
        System.out.println(PageTools.printLog(PageTools.getIpAddr(request), logInfo));
        return  ToJson.toJson(res)  ;
    }


    @POST
    @Path("{virtual_db}")
    @Produces(MediaType.APPLICATION_JSON)
    public String AddSubtitle( @PathParam("virtual_db") String virtual_db,
                                @Context org.glassfish.grizzly.http.server.Request request) {

        String name = JsonExtractor.getStrVal(virtual_db, "name");
        String subject = JsonExtractor.getStrVal(virtual_db, "subject");
        if(name == null){
            errorInfo = "Get subtitle name failed";
            System.out.println(PageTools.printLog(PageTools.getIpAddr(request), errorInfo));
            return errorInfo;
        }
        if( subject == null ){
            errorInfo = "Get subject name failed, I need to know this subtitle belong to which subject ";
            System.out.println(PageTools.printLog(PageTools.getIpAddr(request), errorInfo));
            return errorInfo;
        }

        boolean res = false;
        try {
            res = TableOps.AddSubtitle(subject, name);
        } catch (Exception e) {
            //e.printStackTrace();
            errorInfo = e.toString();
        }
        if(!res) {
            errorInfo = "Add subtitle " + name +" failed, Plz check log "
            + "\n" + errorInfo;
        }else {
            errorInfo = "Add subtitle " + name +" successfully!";

        }
        System.out.println(PageTools.printLog(PageTools.getIpAddr(request), errorInfo));
        return  errorInfo  ;
    }

    @DELETE
    @Path("{virtual_db}")
    @Produces(MediaType.APPLICATION_JSON)
    public String DelSubtitle(@PathParam("virtual_db") String virtual_db,
                             @Context org.glassfish.grizzly.http.server.Request request) {
        boolean delRes = true;
        String name = JsonExtractor.getStrVal(virtual_db, "name");
        if(name == null){
            errorInfo = "Get subtitle name failed";
            return errorInfo;
        }

        String dbId = TableOps.NameToId(virtual_dbTabName, "name", name);
        String errorInfo = null;
        if( dbId == null ) {
            errorInfo = "Failed, Get subtitle : " + name + "id failed!";
            System.out.println(PageTools.printLog(PageTools.getIpAddr(request), errorInfo));
            return errorInfo;
        }
        TableOps ops = new TableOps();
        if(!ops.DelDbToSub(dbId)){
            delRes = false;
        }
        if(!ops.DelEntry(dbId, storeDb, virtual_dbTabName)){
            delRes = false;
        }


        if(delRes) {
            errorInfo ="Sucess";
        }else{
            errorInfo = "Failed";
        }
        System.out.println(PageTools.printLog(PageTools.getIpAddr(request), errorInfo));


        return errorInfo;
    }




}
