package com.steven.db.mysql;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MySqlHelper {
    //	jdbc:mysql://192.168.1.209:3306/contact_info --username root --password FSdev*mysql
    public static final String url = "jdbc:mysql://192.168.1.209:3306/contact_info";
    public static final String name = "com.mysql.jdbc.Driver";
    public static final String user = "root";
    public static final String password = "FSdev*mysql";

    public Connection conn = null;
    public PreparedStatement pst = null;

    public MySqlHelper() {
        try {
            Class.forName(name);//指定连接类型  
            conn = DriverManager.getConnection(url,user,password);//获取连接  
//            pst = conn.prepareStatement(sql);//准备执行语句  
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 执行sql语句，返回一个 ResultSet
     * @param sql
     * @return
     */
    public ResultSet execResultSet(String sql) {
        try {
            Class.forName(name);//指定连接类型  
            conn = DriverManager.getConnection(url,user,password);//获取连接  
            Statement statement = conn.createStatement();
            ResultSet rs = statement.executeQuery(sql);

//            pst = conn.prepareStatement(sql);//准备执行语句
            return rs;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public void close() {
        try {
            this.conn.close();
            this.pst.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}