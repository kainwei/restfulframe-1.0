package com.jj.dw.dbClient;

import java.util.List;

/**
 * Created by weizh on 2016/8/18.
 */
public class HiveJdbc implements MyClient {
    public List<Object> output(String url, String sql){
        String driverName = "org.apache.hive.jdbc.HiveDriver";
        String user = "hdfs";
        String password = "1234";
        return new MyJdbc().jdbc(driverName, user, password, url, sql);
    }

    public List<Object> outputId(String url, String sql, int col) {
        return null;
    }

    public static void main(String [] args) {
        String url = "jdbc:hive2://ip_instead_tmp:10000";
        MyClient mj = new ClientFactory().createClient("hivejdbc") ;
        String sql = "show databases";
        List res = mj.output(url, sql);
        System.out.println(ToJson.toJson(res));
    }


}
