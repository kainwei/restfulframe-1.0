package com.jj.dw.dbClient;



import com.jj.dw.restful.util.StringFixer;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by weizh on 2016/8/12.
 */
public class MyJdbc implements MyClient {

    public List<Object> output(String url, String sql){
        String driverName = "org.apache.hive.jdbc.HiveDriver";
        String user = "hdfs";
        String password = "1234";
        return jdbc(driverName, user, password, url, sql);
    }

    public List<Object> outputId(String url, String sql, int col) {
        return null;
    }


    public List<Object> outputCol(String  driverName, String user, String password, String url, String sql, int col) {
        ResultSet res;
        List list = new ArrayList<Object>();
        Connection conn = SqlConnection(driverName, user, password, url);
        try {
            Statement stmt = conn.createStatement();
            String judgeSql = sql.toLowerCase().split("\\s+")[0];
            res = stmt.executeQuery(sql);

            while (res.next()) {
                String str = res.getString(col);
                list.add(str);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

         return list;

    }

    public List<Object> jdbc(String driverName, String user, String password, String url, String sql ){

        ResultSet res;
        List list = new ArrayList<Object>();

        //Connection conn = SqlConnection(driverName, user, password, url);
        Connection conn = SingleMysqlConn.getInstance(driverName, user, password, url).conn;

        try {
            //conn = DriverManager.getConnection(url, user, password);
            Statement stmt = conn.createStatement();

            String judgeSql = sql.toLowerCase().split("\\s+")[0];

            if(judgeSql.contains("create") || judgeSql.contains("insert") || judgeSql.contains("delete")
                    || judgeSql.contains("drop") || judgeSql.contains("update") || judgeSql.contains("alter")){

                int eRes = stmt.executeUpdate(sql);
                list.add(eRes);
               /* boolean eRes = stmt.execute(sql);
                if(eRes){
                    list.add("Success");
                }else{
                    list.add("Failed");
                }*/


            }else if(judgeSql.contains("select") || judgeSql.contains("desc")){
                res = stmt.executeQuery(sql);
                while(res.next()){
                    int col = res.getMetaData().getColumnCount();
                    List tmplist = new ArrayList<String>();
                    for(int i=1;i<=col;i++){
                        tmplist.add(res.getString(i));
                    }
                    String entry = ToJson.toJson(tmplist);
                    list.add(entry);
                }

            }else{
                res = stmt.executeQuery(sql);
                while (res.next()) {
                    String str = res.getString(1);
                    if(str.contains(":struct") && str.contains("{")){
                        list.add(StringFixer.structFixer(str));
                    }else {
                        list.add(str);
                    }
                }
            }


        } catch (SQLException e) {
            e.printStackTrace();
        }finally{
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }


        return list;

    }

    private Connection SqlConnection(String driverName, String user, String password, String url) {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return connection;

    }



    public static void main(String [] args ) {
        String driverName = "org.apache.hive.jdbc.HiveDriver";
        //String sql = "desc abnormaluser.simuappear_abnormaluser_day_island_abusergroup";
        String sql = "show table EXTENDED in abnormaluser like simuappear_abnormaluser_day_island_abusergroup";
        String user = "hdfs";
        String password = "1234";
        String url = "jdbc:hive2://ip_instead_tmp:10000";
        //List<Object> list  = new MyJdbc().jdbc(driverName, user, password, url, sql);
        List<Object> list = new ClientFactory().createClient("hivejdbc").output(url, sql);

        System.out.println(ToJson.toJson(AddJsonHead.addhead(list)));

    }


}
