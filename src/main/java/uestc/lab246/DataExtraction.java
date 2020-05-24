package uestc.lab246;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;

import uestc.lab246.bind.BindInout;
import uestc.lab246.excecute.ExecuteExtraction;
import uestc.lab246.init.InitKettle;
import uestc.lab246.input.TableInput;
import uestc.lab246.output.TableOutput;
import uestc.lab246.register.Registry;
import uestc.lab246.trans.InitTransMeta;

/**
 * @author ZhouPan
 * @date 2020-05-18
 */
public class DataExtraction {
	public static void main(String[] args) {

		try {
			InitKettle client = new InitKettle();
			InitTransMeta metaInitiator = new InitTransMeta();
			Registry registor = new Registry();
			TableInput input = new TableInput();
			TableOutput output = new TableOutput();
			BindInout bind = new BindInout();
			ExecuteExtraction extraction = new ExecuteExtraction();
			client.initKettleEnvironment(null);
			//  此处为上例的数据库配置
			String transXML = "source.xml";
			TransMeta meta = metaInitiator.buildTransMeta("kettle", transXML);
			PluginRegistry registry = registor.getRegistry();
			StepMeta step1 = input.setTableInputStep(meta, registry, "kettle", "select * from test1", "table input");
			StepMeta step2 = output.setTableOutput(meta, registry, "kettle", "test2", "table insert");
			bind.addTransHop(meta, step1, step2);

			extraction.executeTrans(meta, "kettle");
		} catch (KettleException e) {
			e.printStackTrace();
		}
	}
}
