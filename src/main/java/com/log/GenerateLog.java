package com.log;


import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Date;

/**
 * Created by Administrator on 2018/2/25.
 */
public class GenerateLog {

    public GenerateLog(){

    }

    public static void main(String[] args) throws InterruptedException {
        Logger logger = LogManager.getLogger("testLog");
        int i=0;

        do{
            logger.info((new Date()).toString()+"-----------------");
            ++i;
            Thread.sleep(500L);
        }while(i<=1000000);

    }
}
