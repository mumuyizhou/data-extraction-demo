package uestc.lab246.trans;

import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.trans.TransMeta;
/**
 * @author ZhouPan
 * @date 2020-05-17
 */
public class InitTransMeta {
	public TransMeta buildTransMeta(String metaName, String... transXML) throws KettleXMLException {
		TransMeta transMeta = new TransMeta();
		// 设置转化元的名称
		transMeta.setName(metaName);
		// 添加转换的数据库连接
		for (int i = 0; i < transXML.length; i++   ) {
			transMeta.addDatabase(new DatabaseMeta(transXML[i]));
		}
		return transMeta;
	}
}
