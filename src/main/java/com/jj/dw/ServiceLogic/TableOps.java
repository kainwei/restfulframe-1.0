package com.jj.dw.ServiceLogic;

import com.jj.dw.dbClient.ClientFactory;
import com.jj.dw.dbClient.MyClient;
import com.jj.dw.dbClient.ToJson;
import com.jj.dw.restful.util.GlobalConf;
import com.jj.dw.restful.util.PageTools;
import org.apache.hadoop.conf.Configuration;

import java.util.List;

/**
 * Created by weizh on 2016/8/25.
 */
public class TableOps {
    static GlobalConf gc = GlobalConf.getGlobalConf();
    static Configuration conf = gc.conf;
    static String mySqlUrl = conf.get("com.jj.subject.mysql","ip_instead_tmp:3306");
    static String storeDb = conf.get("com.jj.subject.mysql.db", "WZH");
    static String subjectTabName = conf.get("com.jj.subject.mysql.subjectTableName","subject");
    static String virtual_dbTabName = conf.get("com.jj.subject.mysql.virtual_dbTableName","virtual_db");
    static String tableTabName = conf.get("com.jj.subject.mysql.tableTableName","table");
    static String sub_dbTabName = conf.get("com.jj.subject.mysql.sub_dbTabName", "sub_db");
    static String db_tableTabName = conf.get("com.jj.subject.mysql.TabName", "db_table");
    static String subDbTabName = conf.get("com.jj.dw.mysql.sub_dbTabName","sub_db");
    static String dbTabTabName = conf.get("com.jj.dw.mysql.db_tableTabName","db_table");
    static String url = "jdbc:mysql://" + mySqlUrl;
    static MyClient mj = new ClientFactory().createClient("mysqljdbc") ;



    public boolean DelDbToSub(String dbId) {
        String sql = "delete from " + storeDb + "." + sub_dbTabName + " where db_id='" + dbId + "'";
        try{
            mj.output(url, sql);
        }catch (RuntimeException e) {
            e.printStackTrace();
            return false;
        }
        DelDbToTab(dbId);
        return true;
    }

    public boolean DelSubToDb(String subId, String dbId) {
        String sql = "delete from " + storeDb + "." + sub_dbTabName + " where sub_id='" + subId + "' and db_id='"
                + dbId + "'";
        try{
            mj.output(url, sql);
        }catch (RuntimeException e) {
            e.printStackTrace();
            return false;
        }

        sql = "select * from " + storeDb + "." + sub_dbTabName + " where db_id='" +  dbId + "'";
        List leftDb = mj.output(url, sql);
        if(leftDb.isEmpty()){
            DelDbToTab(dbId);
        }

        return true;

    }
    public boolean DelSubToDb(String subId) {
        String sql = "select * from " + storeDb + "." + sub_dbTabName + " where sub_id='" +  subId + "'";
        List<Object> dbList = mj.outputId(url, sql, 3);

        sql = "delete from " + storeDb + "." + sub_dbTabName + " where sub_id='" + subId + "'";
        try{
            mj.output(url, sql);
        }catch (RuntimeException e) {
            e.printStackTrace();
            return false;
        }

        for(Object dbObjId :dbList){
            String dbId = (String) dbObjId;
            sql = "select * from " + storeDb + "." + sub_dbTabName + " where db_id='" +  dbId + "'";
            List<Object> leftDb =  mj.outputId(url, sql, 3);
            if(leftDb.isEmpty()){
                DelDbToTab(dbId);
            }

        }

        return true;

    }
    private boolean DelDbToTab(String dbid) {
        boolean res = true;
        String sql = "select * from " + storeDb + "." + db_tableTabName + " where db_id='" +  dbid + "'";
        List<Object> tabList = mj.outputId(url, sql, 3);
        if(!tabList.isEmpty()){
            for(Object idObj:tabList) {
                String tabid = (String) idObj;
                if(!DelTabOnlyInOneDb(dbid, tabid)) {
                    res = false;
                }
            }
        }
        sql = "delete from " + storeDb + "." + db_tableTabName + " where db_id='" + dbid + "'";
        //System.out.println("Debug Info : " + sql);
        try{
            mj.output(url, sql);
        }catch (RuntimeException e) {
            System.out.println(e.toString());
            res = false;
        }
        return res;

    }
    private boolean DelTabOnlyInOneDb(String dbid, String tabid){
        boolean res = true;
        String sql = "select * from " + storeDb + "." + db_tableTabName + " where table_id='" +  tabid
                + "' and  db_id!='" + dbid + "'";
        List tmplist = mj.outputId(url, sql, 3);
        if(tmplist.isEmpty()) {
            if(!DelEntry(tabid, storeDb, tableTabName)) {
                res = false;
            }
        }
        return res;

    }

    public boolean DelEntry(String id, String db, String tab){
        String sql = "delete from "  + db + "." + tab + " where id='" + id + "'";
        //System.out.println("Debug info : " + sql );
        try{
            mj.output(url, sql);
        }catch (RuntimeException e) {
            e.printStackTrace();
            return false;
        }
        return true;

    }




    public static String NameToId(String table_name, String columnName, String columnVal){
        GlobalConf gc = GlobalConf.getGlobalConf();
        Configuration conf = gc.conf;

        String mySqlUrl = conf.get("com.jj.dw.mysql", "ip_instead_tmp:3306");
        String storeDb = conf.get("com.jj.dw.mysql.db", "WZH");
        String subjectTabName = conf.get("com.jj.dw.mysql.subjectTableName", "subject");
        String virtual_dbTabName = conf.get("com.jj.dw.mysql.virtual_dbTableName", "virtual_db");
        String tableTabName = conf.get("com.jj.dw.mysql.tableTableName", "table");
        String url = "jdbc:mysql://" + mySqlUrl;

        return GetId(url, storeDb, table_name, columnName, columnVal);

    }


    public static String GetId(String url, String db_name, String table_name, String columnName, String columnVal){


        MyClient mj = new ClientFactory().createClient("mysqljdbc");

        String sql = "select id  from " + db_name + "." + table_name + " where " + columnName + "='" + columnVal + "'";
        System.out.println("Debug Info : " + sql);
        List resList = mj.outputId(url, sql, 1);

        //List resList = mj.output(url, sql);
        if(resList.isEmpty()) {
            return null;
        }
        String res = (String) resList.get(0);


        return res;

    }

    public static String GetTableId(String tableName, String location) {

        String sql = "select id  from " + storeDb + "." + tableTabName + " where name='" + tableName
                + "' and phy_src_location='" + location + "'";
        List resList = mj.outputId(url, sql, 1);
        String res = (String) resList.get(0);

        return res;

    }

    public static boolean AddSubtitle(String subject, String subtitle) throws Exception {
        String errorInfo = null;
        boolean res = true;

        String sql = "insert into " + storeDb + "." + virtual_dbTabName + " (name) values ( '"
                + subtitle +"')";
        if(IfRowExist(virtual_dbTabName, subtitle)){
            errorInfo = "Failed! subtitle: " + subtitle + " already exist!";
            System.out.println(PageTools.printInternalOps(errorInfo));
            return false;

        }

        List<Object> output  = mj.output(url,sql);
        String logInfo = mySqlUrl + " " + sql + " " + ToJson.toJson(output);
        System.out.println(PageTools.printInternalOps(logInfo));


        String subId = TableOps.NameToId(subjectTabName, "name", subject);
        String dbId = TableOps.NameToId(virtual_dbTabName, "name", subtitle);
        //System.out.println("Debug Info : subid is " + subId + " dbId is : " + dbId );

        if( subId == null ) {
            res = false;
            errorInfo = PageTools.errorMsgMaker("subid", subject);
        }

        if( dbId == null ) {
            res = false;
            errorInfo = PageTools.errorMsgMaker("dbid", subject);
        }
        if(IfIdTabRowExist(sub_dbTabName, subId, dbId)){
            errorInfo = "Failed! Relationship between " + subject + " & "
                    + subtitle + " already existed!";
            System.out.println(PageTools.printInternalOps(errorInfo));
            res = false;
        }

        if(!res) {
            sql = "delete from "  + storeDb + "." + virtual_dbTabName + " where name='" + subtitle + "'";
            //System.out.println("Debug info : " + sql );
            output = mj.output(url,sql);
            errorInfo = errorInfo + " undo add subtitle "+ output;
            System.out.println(PageTools.printInternalOps(errorInfo));
            return res;
        }

        sql = "insert into " + storeDb + "." + sub_dbTabName + " (sub_id, db_id) values ( '"
                + subId +"','" + dbId +"')";
        output  = mj.output(url,sql);
        logInfo = logInfo + "\n" + mySqlUrl + " " + sql + " " + ToJson.toJson(output);

        System.out.println(PageTools.printInternalOps(logInfo));
        return res;
    }
    public static boolean AddTableRow(String subject, String subtitle, String name,
                                      String dbserver, String dbname, String tableMeta ) {
        String location = dbserver + ":" + dbname;
        return AddTableRow(subject, subtitle, name, location, tableMeta);
    }

   public static boolean AddTableRow(String subject, String subtitle, String name, String location, String tableMeta) {
       String errorInfo = null;
       boolean res = true;
       String subId = null;

       if( subtitle == null ) {
           subId = TableOps.NameToId(subjectTabName, "name", subject);
           if(subId == null){
               errorInfo = PageTools.errorMsgMaker("subid", subject);
               System.out.println(PageTools.printInternalOps(errorInfo));
               return false;
           }
           subtitle = "default_" +subId;
           boolean addSubtitleRes = true;
           String addTabErrorInfo = null;
           try {
               addSubtitleRes = TableOps.AddSubtitle(subject, subtitle);
           } catch (Exception e) {
               addTabErrorInfo = e.toString();
               addSubtitleRes = false;
           }
           if(!addSubtitleRes) {
               addTabErrorInfo = "Add subtitle " + name +" failed, Plz check log "
                       + "\n" + addTabErrorInfo;
           }else {
               addTabErrorInfo = "Add subtitle " + name +" successfully!";

           }
           System.out.println(PageTools.printInternalOps(addTabErrorInfo));
       }
       String dbId = TableOps.NameToId(virtual_dbTabName, "name", subtitle);
       if(dbId == null ) {
           errorInfo = PageTools.errorMsgMaker("dbid", subtitle);
           System.out.println(PageTools.printInternalOps(errorInfo));
           return false;
       }
       if(!TableOps.IfRowExist(virtual_dbTabName, subtitle)) {
           errorInfo = "subtitle " + subtitle + " not exist!";
           System.out.println(PageTools.printInternalOps(errorInfo));
           return false;
       }
       if(!TableOps.IfIdTabRowExist(subDbTabName, subId, dbId)) {
           errorInfo = "Relationship between subject id " + subId + " and subtitle id: "
                   + dbId + " is not exist in " + subDbTabName;
           System.out.println(PageTools.printInternalOps(errorInfo));
           return false;
       }

       if(TableOps.IfIdTabRowExist(tableTabName, name, location )) {
           errorInfo = "Entry: " +  name + " physical locate in  " + location + " is already exist!";
           System.out.println(PageTools.printInternalOps(errorInfo));
           return false;
       }

       String sql = "insert into " + storeDb + "." + tableTabName + " (name, phy_src_location, table_meta) values ( '"
               + name + "','" + location +"','" + tableMeta +"')";

       List<Object> output  = mj.output(url,sql);
       String logInfo = mySqlUrl + " " + sql + " " + ToJson.toJson(output);
       String tabId = GetTableId(name, location) ;
       if(tabId == null){
           errorInfo = PageTools.errorMsgMaker("tabid", name);
           sql = "delete from "  + storeDb + "." + tableTabName + " where name='" + name
                   + "' and phy_src_location='" + location + "'";
           output = mj.output(url,sql);
           errorInfo = errorInfo + " undo add table "+ output;
           System.out.println(PageTools.printInternalOps(errorInfo));
           return false;
       }
       sql = "insert into " + storeDb + "." + db_tableTabName + " (db_id, table_id) values ( '"
               + dbId +"','" + tabId +"')";
       output  = mj.output(url,sql);
       logInfo = logInfo + "\n" + mySqlUrl + " " + sql + " " + ToJson.toJson(output);

       System.out.println(PageTools.printInternalOps(logInfo));
       return true;
   }

    public static boolean IfIdTabRowExist(String tab, String col1, String col2){
        String column1 = null;
        String column2 = null;
        if(tab.equals(sub_dbTabName)) {
            column1 = "sub_id";
            column2 = "db_id";

        }else if(tab.equals(db_tableTabName)) {
            column1 = "db_id";
            column2 = "table_id";
        }else if(tab.equals(tableTabName)) {
            column1 = "name";
            column2 = "phy_src_location";
        }

        String sql = "select * from " + storeDb + "." + tab
                + " where " + column1 + "='" + col1 + "' and " + column2
                + "='" + col2 + "'";
        //System.out.println ("Debug info : " + sql);
        List res = mj.outputId(url, sql, 1);
        if(!res.isEmpty()) {
            return true;
        }

        return false;
    }

    public static boolean IfRowExist(String tab, String col ){
        String sql = "select * from " + storeDb +"." + tab + " where name='" + col + "'";
        List res = mj.outputId(url, sql, 1);
        if(!res.isEmpty()) {
            return true;
        }

        return false;

    }

    public static void main(String [] args) {
        String subId = TableOps.NameToId(subjectTabName, "name", "clothes");
        System.out.println(subId);
    }


}

