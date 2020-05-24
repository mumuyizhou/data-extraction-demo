package uestc.lab246.log;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.logging.StepLogTable;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.trans.TransMeta;

/**
 * @author ZhouPan
 * @date 2020-05-18
 */
public class LogManager {
	public void setStepLogTable(TransMeta transMeta, String connDbName, String tableName) {
		VariableSpace space = new Variables();
		// 将step日志数据库配置名加入到变量集中
		space.setVariable(Const.KETTLE_TRANS_LOG_DB, connDbName);
		space.initializeVariablesFrom(null);
		StepLogTable stepLogTable = StepLogTable.getDefault(space, transMeta);
		// 配置StepLogTable使用的数据库配置名称
		stepLogTable.setConnectionName(connDbName);
		// 设置Step日志的表名
		stepLogTable.setTableName(tableName);
		// 设置TransMeta的StepLogTable
		transMeta.setStepLogTable(stepLogTable);
	}
}
