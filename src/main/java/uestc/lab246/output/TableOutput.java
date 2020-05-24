package uestc.lab246.output;

import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.insertupdate.InsertUpdateMeta;
import org.pentaho.di.trans.steps.tableoutput.TableOutputMeta;

/**
 * @author ZhouPan
 * @date 2020-05-18
 */
public class TableOutput {
	/**
	 * 设置表输出步骤，用于整表抽取
	 * @param transMeta
	 * @param registry
	 * @param targetDbName
	 * @param targetTableName
	 * @param stepName
	 * @return
	 */
	public StepMeta setTableOutput(TransMeta transMeta, PluginRegistry registry, String targetDbName,
								   String targetTableName, String stepName) {
		// 创建表输出
		TableOutputMeta tableOutputMeta = new TableOutputMeta();
		String pluginId = registry.getPluginId(StepPluginType.class, tableOutputMeta);
		// 配置表输出的目标数据库配置名
		DatabaseMeta targetDb = transMeta.findDatabase(targetDbName);
		tableOutputMeta.setDatabaseMeta(targetDb);
		tableOutputMeta.setTableName(targetTableName);
		// 将表输出添加到转换中
		StepMeta stepMeta = new StepMeta(pluginId, stepName, tableOutputMeta);
		transMeta.addStep(stepMeta);
		return stepMeta;
	}

	/**
	 * 设置表插入与更新步骤，用于表中部分字段更新
	 * @param transMeta
	 * @param registry
	 * @param targetDbName
	 * @param targetTableName
	 * @param updatelookup lookup检索字段
	 * @param updateStream lookup更新字段
	 * @param updateStream2 lookup更新字段2
	 * @param conditions lookup条件
	 * @param updateOrNot lookup更新标记
	 * @param stepName
	 * @return
	 */
	public StepMeta setInsertUpdateMeta(TransMeta transMeta, PluginRegistry registry, String targetDbName,
										String targetTableName, String[] updatelookup, String[] updateStream, String[] updateStream2,
										String[] conditions, Boolean[] updateOrNot, String stepName) {
		// 创建插入与更新
		InsertUpdateMeta insertUpdateMeta = new InsertUpdateMeta();
		String pluginId = registry.getPluginId(StepPluginType.class, insertUpdateMeta);
		// 配置目标数据库配置名
		DatabaseMeta database_target = transMeta.findDatabase(targetDbName);
		insertUpdateMeta.setDatabaseMeta(database_target);
		// 设置目标表名
		insertUpdateMeta.setTableName(targetTableName);
		// 设置用来查询的关键字
		insertUpdateMeta.setKeyLookup(updatelookup);
		insertUpdateMeta.setKeyStream(updateStream);
		insertUpdateMeta.setKeyStream2(updateStream2);// 这一步不能省略
		insertUpdateMeta.setKeyCondition(conditions);
		// 设置要更新的字段
		insertUpdateMeta.setUpdateLookup(updatelookup);
		insertUpdateMeta.setUpdateStream(updateStream);
		insertUpdateMeta.setUpdate(updateOrNot);
		// 添加步骤到转换中
		StepMeta stepMeta = new StepMeta(pluginId, stepName, insertUpdateMeta);
		stepMeta.setDraw(true);
		stepMeta.setLocation(250, 100);
		transMeta.addStep(stepMeta);
		return stepMeta;
	}
}
