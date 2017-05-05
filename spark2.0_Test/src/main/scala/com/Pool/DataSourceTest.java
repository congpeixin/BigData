package com.Pool;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by cluster on 2017/5/4.
 */
public class DataSourceTest {
    public static void main(String[] args){
        Connection conn = null;
        PreparedStatement st = null;
        ResultSet rs = null;

        try {
            conn = JdbcUtils_C3P0.getConnection();
            String sql = "select user_fansNum from weibo_content2";
            st = conn.prepareStatement(sql);
            //executeQuery: 适用于select查询语句
            //executeUpdate: 适用于INSERT、UPDATE 或 DELETE
            rs = st.executeQuery();
            if (rs.next()){
                System.out.println(rs.getString("user_fansNum"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            JdbcUtils_C3P0.release(conn,st,rs);
        }

    }
}
