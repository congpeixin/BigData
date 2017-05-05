package com.Pool;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by cluster on 2017/5/4.
 * @ClassName: JdbcUtils_C3P0
 * @Description: 获取数据库连接工具类
 *
 */

public class JdbcUtils_C3P0 {

    //创建存储数据源接口池
    private static ComboPooledDataSource ds = null;
    //在静态代码块中创建数据库连接池
    static {
        try {
            //通过代码创建C3P0数据库连接池
//            ds = new ComboPooledDataSource();
//            ds.setDriverClass("com.mysql.jdbc.Driver");
//            ds.setJdbcUrl("jdbc:mysql://192.168.31.7:3306/cpeixinTest");
//            ds.setUser("root");
//            ds.setPassword("dp12345678");
//            ds.setInitialPoolSize(10);
//            ds.setMinPoolSize(5);
//            ds.setMaxPoolSize(20);
          ds = new ComboPooledDataSource("MySQL");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    /**
     * @Method: getConnection
     * @Description: 从数据源中获取数据库连接
     *
     * @return Connection
     * @throws SQLException
     */
    public static Connection getConnection() throws SQLException{
        //从数据源中获取数据库连接
        return ds.getConnection();
    }
    /**
     * @Method: release
     * @Description: 释放资源，
     * 释放的资源包括Connection数据库连接对象，负责执行SQL命令的Statement对象，存储查询结果的ResultSet对象
     * @param conn
     * @param st
     * @param rs
     */
    public static void release(Connection conn, Statement st, ResultSet rs){
        if (rs != null){
            try {
                //关闭存储查询结果的ResultSet对象
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            rs = null;
        }
        if (st != null){
            try {
                st.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (conn != null){
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
