package akim.scd.test;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.streambase.sb.unittest.CSVTupleMaker;
import com.streambase.sb.unittest.Enqueuer;
import com.streambase.sb.unittest.Expecter;
import com.streambase.sb.unittest.JSONSingleQuotesTupleMaker;
import com.streambase.sb.unittest.SBServerManager;
import com.streambase.sb.unittest.ServerManagerFactory;


public class SlidingTimeAggTest {

	private static SBServerManager server;
	private static Enqueuer qouteEnqueuer;
	private static Expecter statsExpecter;
	//private static TestQuoteTupleCSVGenerator tupleGenerator;

	@BeforeClass
	public static void setupServer() throws Exception {
		server = ServerManagerFactory.getEmbeddedServer();
		server.startServer();
		server.loadApp("SlidingTimeAgg.sbapp");
	}

	@AfterClass
	public static void stopServer() throws Exception {
		if (server != null) {
			server.shutdownServer();
			server = null;
		}
	}

	@Before
	public void startContainers() throws Exception {
		server.startContainers();
		qouteEnqueuer = server.getEnqueuer("InQuote");
		statsExpecter = new Expecter(server.getDequeuer("OutStats"));
		//tupleGenerator = new TestQuoteTupleCSVGenerator("AAA", "BBB", "CCC", "DDD", "EEE");
	}

	@Test
	public void testSingleSymbol() throws Exception {
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,1,10,2012-08-01 10:00:01.000-0500");
		statsExpecter.expectNothing();
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,2,11,2012-08-01 10:00:02.000-0500");
		statsExpecter.expect(JSONSingleQuotesTupleMaker.MAKER, "{'Symbol':'AAA','AvgPrice':1,'MaxPrice':1.0,'MinPrice':1.0,'StdPrice':null,'LastPrice':1.0,'LastQuantity':10,'LastTime':'2012-08-01 10:00:01.000-0500'}");
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,3,13,2012-08-01 10:00:03.000-0500");
		statsExpecter.expect(JSONSingleQuotesTupleMaker.MAKER, "{'Symbol':'AAA','AvgPrice':1.5,'MaxPrice':2.0,'MinPrice':1.0,'StdPrice':0.7071067811865476,'LastPrice':2.0,'LastQuantity':11,'LastTime':'2012-08-01 10:00:02.000-0500'}");

		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,4,14,2012-08-01 10:00:04.000-0500");
		statsExpecter.expect(JSONSingleQuotesTupleMaker.MAKER, "{'Symbol':'AAA','AvgPrice':2.0,'MaxPrice':3.0,'MinPrice':1.0,'StdPrice':1.0,'LastPrice':3.0,'LastQuantity':13,'LastTime':'2012-08-01 10:00:03.000-0500'}");
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,5,15,2012-08-01 10:00:05.000-0500");
		statsExpecter.expect(JSONSingleQuotesTupleMaker.MAKER, "{'Symbol':'AAA','AvgPrice':2.5,'MaxPrice':4.0,'MinPrice':1.0,'StdPrice':1.2909944487358056,'LastPrice':4.0,'LastQuantity':14,'LastTime':'2012-08-01 10:00:04.000-0500'}");
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,6,16,2012-08-01 10:00:06.000-0500");
		statsExpecter.expect(JSONSingleQuotesTupleMaker.MAKER, "{'Symbol':'AAA','AvgPrice':3.0,'MaxPrice':5.0,'MinPrice':1.0,'StdPrice':1.5811388300841898,'LastPrice':5.0,'LastQuantity':15,'LastTime':'2012-08-01 10:00:05.000-0500'}");
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,7,17,2012-08-01 10:00:07.000-0500");
		statsExpecter.expect(JSONSingleQuotesTupleMaker.MAKER, "{'Symbol':'AAA','AvgPrice':3.5,'MaxPrice':6.0,'MinPrice':1.0,'StdPrice':1.8708286933869707,'LastPrice':6.0,'LastQuantity':16,'LastTime':'2012-08-01 10:00:06.000-0500'}");
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,8,18,2012-08-01 10:00:08.000-0500");
		statsExpecter.expect(JSONSingleQuotesTupleMaker.MAKER, "{'Symbol':'AAA','AvgPrice':4.0,'MaxPrice':7.0,'MinPrice':1.0,'StdPrice':2.160246899469287,'LastPrice':7.0,'LastQuantity':17,'LastTime':'2012-08-01 10:00:07.000-0500'}");
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,9,19,2012-08-01 10:00:09.000-0500");	
		statsExpecter.expect(JSONSingleQuotesTupleMaker.MAKER, "{'Symbol':'AAA','AvgPrice':4.5,'MaxPrice':8.0,'MinPrice':1.0,'StdPrice':2.449489742783178,'LastPrice':8.0,'LastQuantity':18,'LastTime':'2012-08-01 10:00:08.000-0500'}");
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,10,20,2012-08-01 10:00:10.000-0500");	
		statsExpecter.expect(JSONSingleQuotesTupleMaker.MAKER, "{'Symbol':'AAA','AvgPrice':5.0,'MaxPrice':9.0,'MinPrice':1.0,'StdPrice':2.7386127875258306,'LastPrice':9.0,'LastQuantity':19,'LastTime':'2012-08-01 10:00:09.000-0500'}");

		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,10,20,2012-08-01 10:00:11.000-0500");	
		statsExpecter.expect(JSONSingleQuotesTupleMaker.MAKER, "{'Symbol':'AAA','AvgPrice':5.5,'MaxPrice':10.0,'MinPrice':1.0,'StdPrice':3.0276503540974917,'LastPrice':10.0,'LastQuantity':20,'LastTime':'2012-08-01 10:00:10.000-0500'}");
		
	}
	

	@After
	public void stopContainers() throws Exception {
		server.stopContainers();
	}


}
