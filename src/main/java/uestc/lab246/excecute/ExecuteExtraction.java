package uestc.lab246.excecute;

import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;

/**
 * @author ZhouPan
 * @date 2020-05-18
 */
public class ExecuteExtraction {
	/**
	 * 执行抽取
	 * @param transMeta
	 * @param targetDbName
	 */
	public void executeTrans(TransMeta transMeta, String targetDbName) {
		try {
			Database database = new Database(null, transMeta.findDatabase(targetDbName));
			database.connect();
			Trans trans = new Trans(transMeta);
			trans.execute(new String[] { "start..." });
			trans.waitUntilFinished();
			// 关闭数据库连接
			database.disconnect();
			if (trans.getErrors() > 0) {
				throw new RuntimeException("There were errors during transformation execution.");
			}
		} catch (KettleDatabaseException e) {
			e.printStackTrace();
		} catch (KettleException e) {
			e.printStackTrace();
		}
	}
}
