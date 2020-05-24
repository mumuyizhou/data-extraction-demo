package uestc.lab246.register;

import org.pentaho.di.core.plugins.PluginRegistry;

/**
 * @author ZhouPan
 * @date 2020-05-24
 */
public class Registry {
	public PluginRegistry getRegistry() {
		// 插件注册，用于注册转换中需要用到的插件
		return PluginRegistry.getInstance();
	}
}
