package uestc.lab246.bind;

import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;

/**
 * @author ZhouPan
 * @date 2020-05-18
 */
public class BindInout {
	/**
	 * 用于将表输入步骤与第二步骤绑定
	 * @param transMeta
	 * @param from
	 * @param to
	 */
	public void addTransHop(TransMeta transMeta, StepMeta from, StepMeta to) {
		transMeta.addTransHop(new TransHopMeta(from, to));
	}
}
