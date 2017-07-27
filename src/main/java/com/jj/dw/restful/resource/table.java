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

@Path("table")
public class table {
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
     * "tables" Table Structrue :
     * id | name | phy_src_location | table_meta |
     * restful sample:
     * [Path]/{"name":"table_name","dbserver":"ip_instead_tmp:10000","dbname":"DbName",
     * "subject":"Subject", "subtitle":"SubTitle", "tablemeta":JsonString }
     **/
    @GET
    @Path("{table}")
    @Produces(MediaType.APPLICATION_JSON)
    public String ShowTable(@PathParam("table") String table,
                                  @Context org.glassfish.grizzly.http.server.Request request) {
        String name = JsonExtractor.getStrVal(table, "name");
        String dbServer = JsonExtractor.getStrVal(table, "dbserver");
        String dbname = JsonExtractor.getStrVal(table, "dbname");
        if( name == null || dbServer == null || dbname == null ) {
            errorInfo = "Require {\"name\":\"table_name\",\"dbserver\":\"ip_instead_tmp:10000\",\"dbname\":\"DbName\"} ";
            return errorInfo;
        }

        String location = dbServer + ":" + dbname;

        String sql = "select * from " + storeDb + "." + tableTabName + " where name='" + name
                + "' and phy_src_location='" +  location + "'" ;

        List<Object> res  = mj.output(url,sql);
        String logInfo = mySqlUrl + " " + sql;
        System.out.println(PageTools.printLog(PageTools.getIpAddr(request), logInfo));
        return  ToJson.toJson(res)  ;
    }


    @POST
    @Path("{table}")
    @Produces(MediaType.APPLICATION_JSON)
    public String AddTable( @PathParam("table") String table,
                                @Context org.glassfish.grizzly.http.server.Request request) {

        System.out.println("Debug Info : " + table);
        String name = JsonExtractor.getStrVal(table, "name");
        System.out.println("Debug Info : " + name);
        String subject = JsonExtractor.getStrVal(table, "subject");
        String dbServer = JsonExtractor.getStrVal(table, "dbserver");
        String dbname = JsonExtractor.getStrVal(table, "dbname");
        System.out.println("Debug Info : " + dbname);
        String tableMeta = JsonExtractor.getStrVal(table, "tablemeta");
        //String tableMeta ="123456";
        String subtitle = JsonExtractor.getStrVal(table, "subtitle");

        System.out.println("Debug Info : " + tableMeta);

        if(name == null || subject == null || dbServer == null || dbname == null || tableMeta == null ){
            errorInfo = "Info is not complete! Require: {\"name\":\"table_name\"," +
                    "\"dbserver\":\"ip_instead_tmp:10000\",\"dbname\":\"DbName\",\n" +
                    "     * \"subject\":\"Subject\", \"subtitle\":\"SubTitle\", \"tablemeta\":JsonString }";
            return errorInfo;
        }

        boolean res = TableOps.AddTableRow(subject, subtitle, name, dbServer, dbname, tableMeta );

        if(!res) {
            errorInfo = "Add table" + name +" failed, Plz check log ";
        }else {
            errorInfo = "Add table " + name +" successfully!";

        }
        System.out.println(PageTools.printLog(PageTools.getIpAddr(request), errorInfo));
        return  errorInfo  ;



    }

    @DELETE
    @Path("{table}")
    @Produces(MediaType.APPLICATION_JSON)
    public String DelTable(@PathParam("table") String table,
                             @Context org.glassfish.grizzly.http.server.Request request) {

        String name = JsonExtractor.getStrVal(table, "name");
        String dbServer = JsonExtractor.getStrVal(table, "dbserver");
        String dbname = JsonExtractor.getStrVal(table, "dbname");
        if( name == null || dbServer == null || dbname == null ) {
            errorInfo = "Require {\"name\":\"table_name\",\"dbserver\":\"ip_instead_tmp:10000\",\"dbname\":\"DbName\"} ";
            return errorInfo;
        }

        String location = dbServer + ":" + dbname;
        String tabId = TableOps.GetTableId(name,location);
        if(tabId == null) {
            errorInfo = PageTools.errorMsgMaker("tabid", name);
            System.out.println(PageTools.printLog(PageTools.getIpAddr(request), errorInfo));
            return errorInfo;
        }

        String sql = "delete from " + storeDb + "." + tableTabName + " where table_id='" + tabId + "'";
        List<Object> res  = mj.output(url,sql);
        String logInfo = mySqlUrl + " " + sql;
        System.out.println(PageTools.printLog(PageTools.getIpAddr(request), logInfo));
        sql = "delete from " + storeDb + "." + tableTabName +" where id='" + tabId + "'";
        res  = mj.output(url,sql);
        logInfo = logInfo + "\n" + mySqlUrl + " " + sql;
        System.out.println(PageTools.printLog(PageTools.getIpAddr(request), logInfo));



        return ToJson.toJson(res);
    }


}
