package com.jj.dw.dbClient;

import java.util.List;

/**
 * Created by weizh on 2016/8/18.
 */
public class MysqlJdbc implements MyClient{
    String driverName = "com.mysql.jdbc.Driver";
    String user = "username";
    String password = "passwd";

    public List<Object> output(String url, String sql){
        return new MyJdbc().jdbc(driverName, user, password, url, sql);
    }




    public List<Object>  outputId(String url, String sql, int col) {
        return new MyJdbc().outputCol(driverName, user, password, url, sql, col);
    }


    public static void main(String [] args ) {

        //String sql = "select * from WZH.table ";
        /*String sql = "create table WZH.subject " +
                "(id int(10) auto_increment not null, " +
                "name char(10) not null UNIQUE, " +
                "comment varchar(50)  , " +
                "PRIMARY KEY (id))";*/

        /*String sql = "create table WZH.virtual_db " +
                "(id int(10) auto_increment not null, " +
                "name char(10) not null UNIQUE, " +
                "PRIMARY KEY ( id ))";*/

        /*String sql = "create table WZH.table " +
                "(id int(10) auto_increment not null, " +
                "name char(10) not null, " +
                "phy_src_location varchar(255) not null, " +
                "table_meta text," +
                "PRIMARY KEY ( id ))";*/

        /*String sql = "create table WZH.sub_db " +
                "(id int(10) auto_increment not null, " +
                "sub_id int(10) not null, " +
                "db_id int(10) not null," +
                "PRIMARY KEY ( id ))";*/

         /*String sql = "create table WZH.db_table " +
                "(id int(10) auto_increment not null, " +
                "db_id int(10) not null, " +
                "table_id int(10) not null," +
                "PRIMARY KEY ( id ))";*/
         String sql = "show databases;";
        //String  sql = "drop table WZH.table";

        String url = "jdbc:mysql://ip_instead_tmp:3306";
        //String sql =  "delete from WZH.db_table where db_id='14'";
        //String sql = "select id  from WZH.subject where name='clothes'";
        //String sql = "desc WZH.subject";
        //String sql = "alter table WZH.table modify name char(255) not null";
        /*List<Object> list = null;

        try{
            list  = new MysqlJdbc().output(url, sql);
        } catch (RuntimeException e) {
            e.printStackTrace();
        }*/
        List<Object> list = new MysqlJdbc().output(url, sql);
        //List<Object> list = new MysqlJdbc().outputId(url, sql, 1);

        System.out.println(ToJson.toJson(list));
    }
}
