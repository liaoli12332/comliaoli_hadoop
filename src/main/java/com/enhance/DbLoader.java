package com.enhance;

import java.sql.*;
import java.util.Map;

/**
 * Created by Administrator on 2018/3/2.
 */
public class DbLoader {

    public static void dbLoad(Map<String,String> ruleMap){

        Connection conn=null;
        Statement st=null;
        ResultSet res=null;

        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn=DriverManager.getConnection("jdbc:mysql://localhost:3306/url_rule","root","123456");
            st = conn.createStatement();
            res=st.executeQuery("select url,content from url_rule");
            while(res.next()){
                ruleMap.put(res.getString(1),res.getString(2));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
                try {
                    if(res!=null){
                        res.close();
                    }
                    if(st!=null){
                        st.close();
                    }
                    if(conn!=null){
                        conn.close();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

}
