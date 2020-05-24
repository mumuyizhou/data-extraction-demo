package uestc.lab246.input;

import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;

/**
 * @author ZhouPan
 * @date 2020-05-18
 */
public class TableInput {
	/**
	 * 设置表输入步骤
	 * @param transMeta
	 * @param registry
	 * @param sourceDbName
	 * @param sql
	 * @param stepName
	 * @return
	 */
	public StepMeta setTableInputStep(TransMeta transMeta, PluginRegistry registry, String sourceDbName, String sql,
									  String stepName) {
		// 创建表输入
		TableInputMeta tableInputMeta = new TableInputMeta();
		String pluginId = registry.getPluginId(StepPluginType.class, tableInputMeta);
		// 指定数据源数据库配置名
		DatabaseMeta source = transMeta.findDatabase(sourceDbName);
		tableInputMeta.setDatabaseMeta(source);
		tableInputMeta.setSQL(sql);
		// 将表输入添加到转换中
		StepMeta stepMeta = new StepMeta(pluginId, stepName, tableInputMeta);
		// 给步骤添加在spoon工具中的显示位置
		stepMeta.setDraw(true);
		stepMeta.setLocation(100, 100);
		// 将表输入添加到步骤中
		transMeta.addStep(stepMeta);
		return stepMeta;
	}
}
