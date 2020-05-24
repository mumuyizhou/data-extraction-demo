package uestc.lab246;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.commons.io.FileUtils;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.impl.cluster.NamedClusterImpl;
import org.pentaho.big.data.impl.cluster.NamedClusterManager;
import org.pentaho.big.data.kettle.plugins.hdfs.trans.HadoopFileOutputMeta;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.NotePadMeta;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.plugins.PluginFolder;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.selectvalues.SelectValuesMeta;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;
import org.pentaho.di.trans.steps.tableoutput.TableOutputMeta;
import org.pentaho.di.trans.steps.textfileoutput.TextFileField;

/**
 * @author ZhouPan
 * @date 2020-05-24
 */
public class KettleDemo {
	/*存放读取到的xml字符串*/
	private static String[] databasesXML;
	/*kettle插件的位置*/
	private static final String KETTLE_PLUGIN_BASE_FOLDER = "C:/code/data-extraction-demo/plugins";

	public static void main(String[] args) throws KettleException, IOException {
		// 这几句必须有, 官网例子是错的, 用来加载插件的
		System.setProperty("hadoop.home.dir", "/");
		StepPluginType.getInstance().getPluginFolders().add(new PluginFolder(KETTLE_PLUGIN_BASE_FOLDER, true, true));
		EnvUtil.environmentInit();
		KettleEnvironment.init();
		// 加载db目录下的所有存放数据库配置的xml文件
		// 这个在官方例子也是没有的, 自己写的, 而且没给出xml的例子, google到的一篇博客里面的, 坑死了
		String rootPath = KettleDemo.class.getResource("/").getPath();
		File dbDir = new File(rootPath, "db");
		List<String> xmlStrings = new ArrayList<>();
		for (File xml : Objects.requireNonNull(dbDir.listFiles())) {
			if (xml.isFile() && xml.getName().endsWith(".xml")) {
				xmlStrings.add(FileUtils.readFileToString(new File(xml.getAbsolutePath())));
			}
		}
		databasesXML = xmlStrings.toArray(new String[0]);
		// 调用下面的方法, 创建一个复制数据库表的Transform任务
		TransMeta transMeta = buildCopyTable(
				"trans",
				"source",
				"stuInfo",
				new String[]{"id", "name", "gender"},
				"target",
				"stuInfo",
				new String[]{"id", "name", "gender"}
		);
//		TransMeta transMeta = buildCopyTableToHDFS(
//				"trans",
//				"235test",
//				"user",
//				new String[]{"id", "name"}
//		);
		// 把以上transform保存到文件, 这样可以用Spoon打开,检查下有没有问题
		String fileName = "C:/Users/zhoup/Desktop/test.ktr";
		String xml = transMeta.getXML();
		DataOutputStream dos = new DataOutputStream(new FileOutputStream(new File(fileName)));
		dos.write(xml.getBytes("UTF-8"));
		dos.close();
		System.out.println("Saved transformation to file: " + fileName);
		// 生成SQL,用于创建表(如果不存在的话)
		String sql = transMeta.getSQLStatementsString();
		// 执行以上SQL语句创建表
		Database targetDatabase = new Database(transMeta.findDatabase("target"));
		targetDatabase.connect();
		targetDatabase.execStatements(sql);
		// 执行transformation...
		Trans trans = new Trans(transMeta);
		trans.execute(new String[]{"id", "name", "gender"});
		trans.waitUntilFinished();
		//  断开数据库连接
		targetDatabase.disconnect();
	}

	/**
	 * Creates a new Transformation using input parameters such as the tablename to read from.
	 *
	 * @param transformationName The name of the transformation
	 * @param sourceDatabaseName 数据源, 对应xml里面的name字段
	 * @param sourceTableName    The name of the table to read from
	 * @param sourceFields       The field names we want to read from the source table
	 * @param targetDatabaseName 复制的去向, 对应xml里面的name字段
	 * @param targetTableName    The name of the target table we want to write to
	 * @param targetFields       The names of the fields in the target table (same number of fields as sourceFields)
	 * @return A new transformation metadata object
	 * @throws KettleException In the rare case something goes wrong
	 */
	private static TransMeta buildCopyTable(String transformationName,
											String sourceDatabaseName, String sourceTableName, String[] sourceFields,
											String targetDatabaseName, String targetTableName, String[] targetFields)
			throws KettleException {
		try {
			// 创建transformation...
			TransMeta transMeta = new TransMeta();
			transMeta.setName(transformationName);
			// 增加数据库连接的元数据
			for (String aDatabasesXML : databasesXML) {
				DatabaseMeta databaseMeta = new DatabaseMeta(aDatabasesXML);
				transMeta.addDatabase(databaseMeta);
			}
			DatabaseMeta sourceDBInfo = transMeta.findDatabase(sourceDatabaseName);
			DatabaseMeta targetDBInfo = transMeta.findDatabase(targetDatabaseName);
			// 增加备注
			String note = "Reads information from table [" + sourceTableName + "] on database [" + sourceDBInfo + "]" + Const.CR + "After that, it writes the information to table [" + targetTableName + "] on database [" + targetDBInfo + "]";
			NotePadMeta ni = new NotePadMeta(note, 150, 10, -1, -1);
			transMeta.addNote(ni);
			// 创建读数据库的step
			String fromStepName = "read from [" + sourceTableName + "]";
			TableInputMeta tii = new TableInputMeta();
			tii.setDatabaseMeta(sourceDBInfo);
			tii.setSQL("SELECT " + Const.CR + String.join(",", sourceFields) + " " + "FROM " + sourceTableName);
			StepMeta fromStep = new StepMeta(fromStepName, tii);
			//以下几句是给Spoon看的, 用处不大
			fromStep.setLocation(150, 100);
			fromStep.setDraw(true);
			fromStep.setDescription("Reads information from table [" + sourceTableName + "] on database [" + sourceDBInfo + "]");
			transMeta.addStep(fromStep);
			// 创建一个修改字段名的step
			SelectValuesMeta svi = new SelectValuesMeta();
			//配置字段名修改的规则, 这里跟官方例子差别很大, 坑不少
			svi.allocate(sourceFields.length, 0, 0);
//			for (int i = 0; i < sourceFields.length; i++) {
//				svi.getSelectName()[i] = sourceFields[i];
//				svi.getSelectRename()[i] = targetFields[i];
//			}
			svi.setSelectName(sourceFields);
			svi.setSelectRename(targetFields);
			String selStepName = "Rename field names";
			StepMeta selStep = new StepMeta(selStepName, svi);
			//以下几句是给Spoon看的, 用处不大
			selStep.setLocation(350, 100);
			selStep.setDraw(true);
			selStep.setDescription("Rename field names");
			transMeta.addStep(selStep);
			//建立读数据库step与修改字段名step的连接,增加到transformation中
			TransHopMeta shi = new TransHopMeta(fromStep, selStep);
			transMeta.addTransHop(shi);
			// 创建一个输出到表的step
			String toStepName = "write to [" + targetTableName + "]";
			TableOutputMeta toi = new TableOutputMeta();
			toi.setDatabaseMeta(targetDBInfo);
			toi.setTablename(targetTableName);
			toi.setCommitSize(200);
			toi.setTruncateTable(true);
			toi.setSchemaName("student2");
			toi.setTruncateTable(false);
			StepMeta toStep = new StepMeta(toStepName, toi);
			//以下几句是给Spoon看的, 用处不大
			toStep.setLocation(550, 100);
			toStep.setDraw(true);
			toStep.setDescription("Write information to table [" + targetTableName + "] on database [" + targetDBInfo + "]");
			transMeta.addStep(toStep);
			// 建立修改字段名step到输出到数据库step的连接
			transMeta.addTransHop(new TransHopMeta(selStep, toStep));
			// 返回
			return transMeta;
		} catch (Exception e) {
			throw new KettleException("An unexpected error occurred creating the new transformation", e);
		}
	}

	private static TransMeta buildCopyTableToHDFS(String transformationName,
												  String sourceDatabaseName, String sourceTableName, String[] sourceFields)
			throws KettleException {
		try {
			// 创建transformation...
			TransMeta transMeta = new TransMeta();
			transMeta.setName(transformationName);
			// 增加数据库连接的元数据
			for (String aDatabasesXML : databasesXML) {
				DatabaseMeta databaseMeta = new DatabaseMeta(aDatabasesXML);
				transMeta.addDatabase(databaseMeta);
			}
			DatabaseMeta sourceDBInfo = transMeta.findDatabase(sourceDatabaseName);
			// 增加备注
			String note = "Reads information from table [" + sourceTableName + "] on database [" + sourceDBInfo + "]" + Const.CR + "After that, it writes the information to HDFS ]";
			NotePadMeta ni = new NotePadMeta(note, 150, 10, -1, -1);
			transMeta.addNote(ni);
			// 创建读数据库的step
			String fromStepName = "read from [" + sourceTableName + "]";
			TableInputMeta tii = new TableInputMeta();
			tii.setDatabaseMeta(sourceDBInfo);
			tii.setSQL("SELECT " + Const.CR + String.join(",", sourceFields) + " " + "FROM " + sourceTableName);
			StepMeta fromStep = new StepMeta(fromStepName, tii);
			//以下几句是给Spoon看的, 用处不大
			fromStep.setLocation(150, 100);
			fromStep.setDraw(true);
			fromStep.setDescription("Reads information from table [" + sourceTableName + "] on database [" + sourceDBInfo + "]");
			transMeta.addStep(fromStep);
			NamedClusterManager clusterManager = new NamedClusterManager();
			NamedCluster cluster = new NamedClusterImpl();
			cluster.setStorageScheme("hdfs");
			cluster.setHdfsHost("bitest01");
			cluster.setHdfsPort("8020");
			cluster.setName("cloudera");
			cluster.setHdfsUsername("");
			cluster.setHdfsPassword("");
			clusterManager.setClusterTemplate(cluster);
//            transMeta.setNamedClusterServiceOsgi();
//            clusterManager.getClusterTemplate().setHdfsHost("bitest01");
			HadoopFileOutputMeta hadoopOut = new HadoopFileOutputMeta(clusterManager, null, null);
//                    new RuntimeTestActionServiceImpl(null, null),
//                    new RuntimeTesterImpl(null, null, "test"));
			hadoopOut.setOutputFields(new TextFileField[]{});
			hadoopOut.setFilename("hdfs://bitest01:8020/tmp/aa");
			hadoopOut.setExtension("txt");
			hadoopOut.setFileCompression("None");
			hadoopOut.setSourceConfigurationName("Cloudera");
			hadoopOut.setSeparator(",");
			hadoopOut.setFileFormat("UNIX");
			hadoopOut.setEncoding("UTF-8");
			StepMeta hadoopStep = new StepMeta("HDFSOutput", hadoopOut);
			hadoopStep.setLocation(550, 100);
			hadoopStep.setDraw(true);
			transMeta.addStep(hadoopStep);
			TransHopMeta hhm = new TransHopMeta(fromStep, hadoopStep);
			transMeta.addTransHop(hhm);
			// 返回
			return transMeta;
		} catch (Exception e) {
			throw new KettleException("An unexpected error occurred creating the new transformation", e);
		}
	}
}
