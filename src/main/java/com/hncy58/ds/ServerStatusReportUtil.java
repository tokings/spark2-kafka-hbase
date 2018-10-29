package com.hncy58.ds;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerStatusReportUtil {

	static transient final Logger log = LoggerFactory.getLogger(ServerStatusReportUtil.class);

	public static int register(String agentSvrName, String agentSvrGroup, int agentSvrType, int agentSourceType, int agentDestType) {

		String querySql = "select * from agent_svr_info t where t.svr_name = ? and t.svr_group = ? and t.svr_type = ?";
		String groupQuerySql = "select * from agent_svr_info t where t.svr_group = ? and t.svr_type = ? and t.status = ?";
		String insertSql = "insert into agent_svr_info(id, svr_name, svr_group, svr_type, source_type, dest_type, status, create_time, update_time) values(?,?,?,?,?,?,?,now(), now()) ";
		String updateSql = "update agent_svr_info set status = ?, update_time = now() where svr_name = ? and svr_group = ? and svr_type = ?";

		try {
			List<Map<String, Object>> groupRs = DSPoolUtil.query(groupQuerySql, agentSvrGroup, agentSvrType, 1);
			// 如果已经有同组的服务注册且在正常运行当中，则启用为备用服务
			int svrStatus = 1;
			if(! groupRs.isEmpty()) {
				svrStatus = 2;
			}
			
			List<Map<String, Object>> rs = DSPoolUtil.query(querySql, agentSvrName, agentSvrGroup, agentSvrType);
			if (rs != null && !rs.isEmpty()) {
				int ret = DSPoolUtil.update(updateSql, svrStatus, agentSvrName, agentSvrGroup, agentSvrType);
				return ret > 0 ? svrStatus : -1;
			} else {
				int ret = DSPoolUtil.update(insertSql, null, agentSvrName, agentSvrGroup, agentSvrType, agentSourceType,
						agentDestType, svrStatus);
				return ret > 0 ? svrStatus : -1;
			}
		} catch (SQLException e) {
			log.error("注册失败，" + e.getMessage(), e);
		}

		return -1;
	}
	
	public static boolean reportSvrStatus(String agentSvrName, String agentSvrGroup, int agentSvrType, int status, String remark)
			throws SQLException {
		String updateSql = "update agent_svr_info set status = ?, remark = ?, update_time = now() where svr_name = ? and svr_group = ? and svr_type = ?";
		int ret = DSPoolUtil.update(updateSql, status, remark, agentSvrName, agentSvrGroup, agentSvrType);
		return ret > 0;
	}

	public static boolean reportAlarm(String agentSvrName, String agentSvrGroup, int agentSvrType, int alarm_type, int alarm_level, String remark)
			throws SQLException {
		String sql = "insert into anget_svr_alarm(id, svr_name, svr_group, svr_type, alarm_type, alarm_level, status, remark, create_time, update_time) values(?,?,?,?,?,?,?,?,now(), now()) ";
		int ret = DSPoolUtil.update(sql, null, agentSvrName, agentSvrGroup, agentSvrType, alarm_type, alarm_level, 0, remark);
		return ret > 0;
	}
}
