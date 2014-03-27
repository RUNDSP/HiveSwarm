import com.livingsocial.hive.udf.*;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import junit.framework.Assert;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;


public class UserAgentParserTest {

   @Test
   public void UserAgentParserTest() throws HiveException {
      UserAgentParser userAgentParser = new UserAgentParser();
      ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
      WritableStringObjectInspector resultInspector = (WritableStringObjectInspector) userAgentParser.initialize(new ObjectInspector[]{stringOI, stringOI});

      Text user_agent = new Text("Mozilla/5.0 (iPad; CPU OS 7_0_6 like Mac OS X) AppleWebKit/537.51.1 (KHTML, like Gecko) CriOS/33.0.1750.15 Mobile/11B651 Safari/9537.53");
      Text expected = new Text("Chrome Mobile iOS 33.0::::::iOS 7.0::::::iPad::::::mobile");
      Text device_data = new Text("rundsp_device_data");
      Object result = userAgentParser.evaluate(new DeferredObject[] { new DeferredJavaObject(user_agent), new DeferredJavaObject(device_data) });
      Assert.assertEquals(expected.toString(), result.toString());

      String user_agent_str = new String("Mozilla/5.0 (iPad; CPU OS 7_0_6 like Mac OS X) AppleWebKit/537.51.1 (KHTML, like Gecko) CriOS/33.0.1750.15 Mobile/11B651 Safari/9537.53");
      String expected_str = new String("Chrome Mobile iOS 33.0::::::iOS 7.0::::::iPad::::::mobile");
      String device_data_str = new String("rundsp_device_data");
      Object result_str = userAgentParser.evaluate(new DeferredObject[] { new DeferredJavaObject(user_agent_str), new DeferredJavaObject(device_data_str) });
      Assert.assertEquals(expected_str, result_str.toString());
   }

}  