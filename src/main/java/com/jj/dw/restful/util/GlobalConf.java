package com.jj.dw.restful.util;

import org.apache.hadoop.conf.Configuration;

/**
 * Created by weizh on 2016/8/25.
 */
public class GlobalConf {
    public Configuration conf ;
    private volatile static GlobalConf globalConf;
    private GlobalConf(){
        conf = new Configuration();
    }
    public static GlobalConf getGlobalConf(){
        if( globalConf == null) {
            synchronized(GlobalConf.class){
                if( globalConf == null ){
                    globalConf = new GlobalConf();
                }
            }
        }
        return globalConf;
    }

}
