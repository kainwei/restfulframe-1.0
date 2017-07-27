package com.jj.dw.restful.util;

import org.glassfish.jersey.client.ClientConfig;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;

/**
 * Created by weizh on 2016/8/9.
 */
public class RestFulClient {
    ClientConfig config = new ClientConfig();

    Client client = ClientBuilder.newClient(config);

    WebTarget target = client.target(getBaseURI());
    public String Client(String path, String op, String text){
        try {
            text = URLEncoder.encode(text, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        System.out.println(text);
        path = path + "/" + text;
        String response;
        if(op.equals("add")){
            Form form = new Form();
            response = target.path(path).request().accept(MediaType.APPLICATION_JSON_TYPE).
                    post(Entity.entity(form,MediaType.APPLICATION_FORM_URLENCODED_TYPE)).toString();
        }else if(op.equals("del")){
            response = target.path(path).
                    request().
                    accept(MediaType.APPLICATION_JSON_TYPE).delete(String.class);
        }else{
            response = target.path(path).
                    request().
                    accept(MediaType.APPLICATION_JSON_TYPE).get(String.class).toString();
        }
        return response;
    }
    public static void main(String[] args) {


        /*String response = target.path("rest").
                path("hello").
                request().
                accept(MediaType.TEXT_PLAIN).
                get(Response.class)
                .toString();


        String plainAnswer =
                target.path("rest").path("hello").request().accept(MediaType.TEXT_PLAIN).get(String.class);
        String xmlAnswer =
                target.path("rest").path("hello").request().accept(MediaType.TEXT_XML).get(String.class);
        String htmlAnswer=
                target.path("rest").path("hello").request().accept(MediaType.TEXT_HTML).get(String.class);*/

        //String plainAnswer =
          //      target.path(path).request().accept(MediaType.TEXT_PLAIN).get(String.class);
        //String xmlAnswer =
        //        target.path(path).request().accept(MediaType.TEXT_XML).get(String.class);
        //String htmlAnswer=
                //target.path(path).request().accept(MediaType.TEXT_HTML).get(String.class);
        //System.out.println(plainAnswer);
        //System.out.println(xmlAnswer);
        //System.out.println(htmlAnswer);
        //String path = "subtitle";
        String path = "table";
        //String rString = "ip_instead_tmp.151:10000";
        //String rString = "{\"name\":\"clothes\",\"comment\":\"clothes\",\"theme id\":[\"1\",\"2\"]}";
        //String rString = "{\"name\":\"woman\",\"subject\":\"clothes\"}";
        //String rString = "{\"name\":\"default_1\",\"subject\":\"clothes\"}";
        String rString = "{\"name\":\"goldbrusher_day_island_fifthlevel3\"," +
                "\"dbserver\":\"ip_instead_tmp:10000\",\"dbname\":\"bill\"," +
                "\"subject\":\"clothes\",\"tablemeta\":{\"dbs\":[\"tableName:goldbrusher_day_island_fifthlevel1\"," +
                "\"owner:hdfs\",\"location:hdfs://ip_instead_tmp:8020/hive/warehouse/bill.db/goldbrusher_day_island_fifthlevel\"," +
                "\"inputformat:org.apache.hadoop.mapred.TextInputFormat\",\"outputformat:org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat\",{\"columns\":[{\"name\":\"mission_id\",\"type\":\"i32\"},{\"name\":\"user_id\",\"type\":\"i32\"},{\"name\":\"user_name\",\"type\":\"string\"},{\"name\":\"filter_time\",\"type\":\"i32\"},{\"name\":\"filter_score\",\"type\":\"i32\"},{\"name\":\"total_score\",\"type\":\"i32\"},{\"name\":\"filter_per\",\"type\":\"float\"},{\"name\":\"accept_num\",\"type\":\"i32\"},{\"name\":\"present_num\",\"type\":\"i32\"}]},\"partitioned:true\",{\"partitionColumns\":[{\"name\":\"year\",\"type\":\"string\"},{\"name\":\"month\",\"type\":\"string\"},{\"name\":\"day\",\"type\":\"string\"}]},\"totalNumberFiles:1076\",\"totalFileSize:1195087\",\"maxFileSize:22312\",\"minFileSize:0\",\"lastAccessTime:1472603784865\",\"lastUpdateTime:1472603787369\",\"\"]}}";
        /*String rString = "{\"name\":\"goldbrusher_day5\"," +
                "\"dbserver\":\"ip_instead_tmp.151:10000\",\"dbname\":\"bill\"," +
                "\"subject\":\"clothes\",\"tablemeta\":{\"aaa\":[\"1\",\"2\"]}}";*/
        /*String rString = "{\"name\":\"goldbrusher_day_island_fifthlevel\"," +
                "\"dbserver\":\"ip_instead_tmp.151:10000\",\"dbname\":\"bill\"," +
                "\"subject\":\"clothes\",\"tablemeta\":\"12345\"}";*/
        String op = "add";
        String res = new RestFulClient().Client(path, op, rString);

        System.out.println(res);

    }

    private static URI getBaseURI() {
        return UriBuilder.fromUri("http://localhost:8080/").build();
    }
}
