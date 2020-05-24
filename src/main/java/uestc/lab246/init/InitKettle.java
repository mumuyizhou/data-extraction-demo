package uestc.lab246.init;

import java.io.File;

import javax.servlet.http.HttpServletRequest;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;

/**
 * @author ZhouPan
 * @date 2020-05-18
 */
public class InitKettle {
	public void initKettleEnvironment(HttpServletRequest request) throws KettleException {
		if (KettleEnvironment.isInitialized()) {
			return;
		}
		/**
		 * 为避免在部分网络环境中无法完成初始化，需要自行处理
		 */
		if (request == null) {
			// 运行环境初始化
			KettleEnvironment.init();
		} else {
			String userDir = System.getProperty("user.dir");
			String kettleHome = request.getSession().getServletContext().getRealPath(File.separator+"WEB-INF");
			// 设置用户路径和系统环境，包括用户路径和主目录
			System.setProperty("user.dir", kettleHome);
			System.setProperty("KETTLE_HOME", kettleHome);
			// 运行环境初始化
			KettleEnvironment.init();
			// 避免造成影响其他程序的运行，还原用户路径
			System.setProperty("user.dir", userDir);
		}
	}
}
