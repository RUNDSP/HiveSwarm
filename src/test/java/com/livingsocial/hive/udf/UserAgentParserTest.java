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
   /* keep these tests in sync with the portal AdGearEventWorker spec */

   @Test
   public void UserAgentParserTest() throws HiveException {
      UserAgentParser userAgentParser = new UserAgentParser();
      ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
      WritableStringObjectInspector resultInspector = (WritableStringObjectInspector) userAgentParser.initialize(new ObjectInspector[]{stringOI, stringOI});

      /* combined field tests */ 
      Text user_agent = new Text("Mozilla/5.0 (iPad; CPU OS 7_0_6 like Mac OS X) AppleWebKit/537.51.1 (KHTML, like Gecko) CriOS/33.0.1750.15 Mobile/11B651 Safari/9537.53");
      Text expected = new Text("Chrome Mobile iOS 33.0.1750::::::iOS 7.0.6::::::iPad::::::mobile");
      Text device_data = new Text("rundsp_device_data");
      Object result = userAgentParser.evaluate(new DeferredObject[] { new DeferredJavaObject(user_agent), new DeferredJavaObject(device_data) });
      Assert.assertEquals(expected.toString(), result.toString());

      String user_agent_str = new String("Mozilla/5.0 (iPad; CPU OS 7_0_6 like Mac OS X) AppleWebKit/537.51.1 (KHTML, like Gecko) CriOS/33.0.1750.15 Mobile/11B651 Safari/9537.53");
      String expected_str = new String("Chrome Mobile iOS 33.0.1750::::::iOS 7.0.6::::::iPad::::::mobile");
      String device_data_str = new String("rundsp_device_data");
      Object result_str = userAgentParser.evaluate(new DeferredObject[] { new DeferredJavaObject(user_agent_str), new DeferredJavaObject(device_data_str) });
      Assert.assertEquals(expected_str, result_str.toString());


      String user_agent_windows = new String("Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; WOW64; Trident/6.0; MATBJS)");
      String expected_windows   = new String("IE 10.0::::::Windows 8::::::Windows 8::::::display");
      Object result_windows = userAgentParser.evaluate(new DeferredObject[] { new DeferredJavaObject(user_agent_windows), new DeferredJavaObject(device_data_str) });
      Assert.assertEquals(expected_windows, result_windows.toString());


      String user_agent_mac = new String("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_5) AppleWebKit/537.71 (KHTML, like Gecko) Version/6.1 Safari/537.71");
      String expected_mac   = new String("Safari 6.1::::::Mac OS X 10.7.5::::::Mac OS X::::::display");
      Object result_mac = userAgentParser.evaluate(new DeferredObject[] { new DeferredJavaObject(user_agent_mac), new DeferredJavaObject(device_data_str) });
      Assert.assertEquals(expected_mac, result_mac.toString());

      String user_agent_kindle = new String("Mozilla/5.0 (Linux; U; en-us; KFSOWI Build/JDQ39) AppleWebKit/535.19 (KHTML, like Gecko) Silk/3.8 Safari/535.19 Silk-Accelerated=true");
      String expected_kindle   = new String("Amazon Silk 3.8::::::Android::::::Kindle Fire HD 7\" WiFi::::::mobile");
      Object result_kindle = userAgentParser.evaluate(new DeferredObject[] { new DeferredJavaObject(user_agent_kindle), new DeferredJavaObject(device_data_str) });
      Assert.assertEquals(expected_kindle, result_kindle.toString());

      String user_agent_ie11 = new String("Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko");
      String expected_ie11   = new String("IE 11.0::::::Windows 7::::::Windows 7::::::display");
      Object result_ie11 = userAgentParser.evaluate(new DeferredObject[] { new DeferredJavaObject(user_agent_ie11), new DeferredJavaObject(device_data_str) });
      Assert.assertEquals(expected_ie11, result_ie11.toString());

      
      /* individual field tests */
      Text user_agent_ios = new Text("Mozilla/5.0 (iPad; CPU OS 7_0_6 like Mac OS X) AppleWebKit/537.51.1 (KHTML, like Gecko) CriOS/33.0.1750.15 Mobile/11B651 Safari/9537.53");
      expected = new Text("Chrome Mobile iOS 33.0.1750");
      Text rundsp_browser = new Text("rundsp_browser");
      result = userAgentParser.evaluate(new DeferredObject[] { new DeferredJavaObject(user_agent_ios), new DeferredJavaObject(rundsp_browser) });
      Assert.assertEquals(expected.toString(), result.toString());

      expected = new Text("iOS 7.0.6");
      Text rundsp_os = new Text("rundsp_os");
      result = userAgentParser.evaluate(new DeferredObject[] { new DeferredJavaObject(user_agent_ios), new DeferredJavaObject(rundsp_os) });
      Assert.assertEquals(expected.toString(), result.toString());

      expected = new Text("iPad");
      Text rundsp_device = new Text("rundsp_device");
      result = userAgentParser.evaluate(new DeferredObject[] { new DeferredJavaObject(user_agent_ios), new DeferredJavaObject(rundsp_device) });
      Assert.assertEquals(expected.toString(), result.toString());

      expected = new Text("mobile");
      Text rundsp_channel = new Text("rundsp_channel");
      result = userAgentParser.evaluate(new DeferredObject[] { new DeferredJavaObject(user_agent_ios), new DeferredJavaObject(rundsp_channel) });
      Assert.assertEquals(expected.toString(), result.toString());


      /* platform tests */ 
      String platform = new String("platform");
      String expected_windows_platform = new String("Windows 8");
      Object result_windows_platform = userAgentParser.evaluate(new DeferredObject[] { new DeferredJavaObject(user_agent_windows), new DeferredJavaObject(platform) });
      Assert.assertEquals(expected_windows_platform, result_windows_platform.toString());

      String expected_ipad_platform   = new String("iPad");
      Object result_ipad_platform = userAgentParser.evaluate(new DeferredObject[] { new DeferredJavaObject(user_agent_str), new DeferredJavaObject(platform) });
      Assert.assertEquals(expected_ipad_platform, result_ipad_platform.toString());

   }

}  