package com.hncy58.ds;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.hadoop.io.IOUtils;

public class DSPoolUtil {

	private static BasicDataSource dataSource = new BasicDataSource();

	// 配置数据源
	static {
		DataSourceConfig();
	}

	private static void DataSourceConfig() {

		Properties props = new Properties();
		InputStream is = null;

		try {
			is = DSPoolUtil.class.getClassLoader().getResourceAsStream("db_dbcp.properites");
			props.load(is);
			props.forEach((k, v) -> System.out.println(k + ":" + v));
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(is);
		}

		dataSource.setDriverClassName(props.getProperty("driverClass", "com.mysql.jdbc.Driver"));
		dataSource.setUsername(props.getProperty("username", "scott"));
		dataSource.setPassword(props.getProperty("password", "tiger"));
		dataSource.setUrl(props.getProperty("url",
				"jdbc:mysql://localhost:3306/bigdata?useUnicode=true&characterEncoding=UTF-8&allowMultiQueries=true"));
		dataSource.setInitialSize(Integer.parseInt(props.getProperty("initialSize", "5")));
		dataSource.setMaxTotal(Integer.parseInt(props.getProperty("maxTotal", "20")));
		dataSource.setMaxIdle(Integer.parseInt(props.getProperty("maxIdle", "10")));
		dataSource.setMinIdle(Integer.parseInt(props.getProperty("minIdle", "3")));
		dataSource.setMaxWaitMillis(Integer.parseInt(props.getProperty("maxWaitMillis", "5000")));
	}

	// 获得连接对象
	public static Connection getConnection() {
		Connection conn = null;
		try {
			conn = dataSource.getConnection();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}

	public static void release(Connection conn, Statement st, ResultSet rs) {
		if (rs != null) {
			try {
				// 关闭存储查询结果的ResultSet对象
				rs.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			rs = null;
		}
		if (st != null) {
			try {
				st.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		if (conn != null) {
			try {
				// 将Connection连接对象还给数据库连接池
				conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void shutdownDataSource() throws SQLException {
		BasicDataSource bds = (BasicDataSource) dataSource;
		bds.close();
	}

	public static int[] batchUpdate(String sql, List<List<Object>> paramsList) throws SQLException {

		Connection con = getConnection();
		PreparedStatement ps = con.prepareStatement(sql);
		con.setAutoCommit(false);

		for(List<Object> params : paramsList) {
			if (params != null) {
				for (int i = 0; i < params.size(); i++) {
					ps.setObject(i + 1, params.get(i));
				}
			}
			ps.addBatch();
		}
		
		int[] ret = null;
		try {
			ret = ps.executeBatch();
			con.commit();
		} catch (Exception e) {
			con.rollback();
			throw e;
		} finally {
			release(con, ps, null);
		}

		return ret;
	}

	public static int update(String sql, Object... params) throws SQLException {

		Connection con = getConnection();
		PreparedStatement ps = con.prepareStatement(sql);
		if (params != null) {
			for (int i = 0; i < params.length; i++) {
				ps.setObject(i + 1, params[i]);
			}
		}

		int ret = 0;
		
		try {
			ret = ps.executeUpdate();
		} finally {
			release(con, ps, null);
		}

		return ret;
	}

	public static List<Map<String, Object>> query(String sql, Object... params) throws SQLException {

		ResultSet rs = null;
		Connection con = null;
		PreparedStatement ps = null;

		List<Map<String, Object>> data;
		try {
			con = getConnection();
			ps = con.prepareStatement(sql);
			if (params != null) {
				for (int i = 0; i < params.length; i++) {
					ps.setObject(i + 1, params[i]);
				}
			}

			rs = ps.executeQuery();

			ResultSetMetaData meta = rs.getMetaData();

			List<String> names = new ArrayList<>();

			for (int i = 0; i < meta.getColumnCount(); i++) {
				names.add(meta.getColumnLabel(i + 1));
			}

			data = new ArrayList<>();
			while (rs.next()) {
				Map<String, Object> map = new HashMap<>();
				for (String name : names) {
					map.put(name, rs.getObject(name));
				}
				data.add(map);
			}
		} catch (Exception e) {
			throw e;
		} finally {
			release(con, ps, rs);
		}

		return data;
	}
}
