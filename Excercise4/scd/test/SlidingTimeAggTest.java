package scd.test;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.streambase.sb.unittest.CSVTupleMaker;
import com.streambase.sb.unittest.Enqueuer;
import com.streambase.sb.unittest.Expecter;
import com.streambase.sb.unittest.ObjectArrayTupleMaker;
import com.streambase.sb.unittest.SBServerManager;
import com.streambase.sb.unittest.ServerManagerFactory;

/**
 * 
 * @author Alfonso Kim
 */
public class SlidingTimeAggTest {

	private static SBServerManager server;
	private static Enqueuer qouteEnqueuer;
	private static Expecter statsExpecter;
	
	private final static int SECOND = 1;

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
	}
	
	/**
	 * This will test the aggregate functions (avg, max, min, std...) for a single simbol
	 */
	@Test
	public void testSingleQuoteAggregateFunctions() throws Exception{
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,10.0,10,2012-10-10 12:00:00.000-0600");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,15.0,10,2012-10-10 12:00:01.000-0600");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,25.0,15,2012-10-10 12:00:02.000-0600");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,30.0,15,2012-10-10 12:00:03.000-0600");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,40.0,25,2012-10-10 12:00:04.000-0600");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,45.0,30,2012-10-10 12:00:05.000-0600");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,55.0,30,2012-10-10 12:00:06.000-0600");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,60.0,35,2012-10-10 12:00:07.000-0600");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,70.0,45,2012-10-10 12:00:08.000-0600");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,80.0,50,2012-10-10 12:00:09.000-0600");
		statsExpecter.expectNothing();
		// send tuple
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,90.0,50,2012-10-10 12:00:10.000-0600");
		statsExpecter.expect(ObjectArrayTupleMaker.MAKER, new Object[]{
				"AAA",		// Symbol 
				43.0,		// AvgPrice
				80.0,		// MaxPrice
				10.0,		// MinPrice
				23.357131,	// StdPrice
				80.0,		// LastPrice
				50,			// LastQunatity
				"2012-10-10 12:00:09.000-0600",	// LastTime
		});
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,100.0,55,2012-10-11 12:00:10.000-0600");
		statsExpecter.expect(ObjectArrayTupleMaker.MAKER, new Object[]{
				"AAA",		// Symbol 
				51.0,		// AvgPrice
				90.0,		// MaxPrice
				15.0,		// MinPrice
				24.472206,	// StdPrice
				90.0,		// LastPrice
				50,			// LastQunatity
				"2012-10-10 12:00:10.000-0600",	// LastTime
		});
	}
	
	
	/**
	 * This also will test aggregate functions with 2 symbols
	 * Price and quantity not increasing monotonically
	 */
	@Test
	public void testMultipleQuoteAggregateFunctions() throws Exception{
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,148.3,290,2012-10-10 12:00:00.000-0600");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "BBB,90.0,50,2012-10-10 12:00:01.000-0600");
		statsExpecter.expectNothing();
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,328.5,120,2012-10-10 12:00:02.000-0600");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "BBB,80.0,50,2012-10-10 12:00:03.000-0600");
		statsExpecter.expectNothing();
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,54.2,920,2012-10-10 12:00:04.000-0600");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "BBB,70.0,45,2012-10-10 12:00:05.000-0600");
		statsExpecter.expectNothing();
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,312.0,32,2012-10-10 12:00:06.000-0600");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "BBB,60.0,35,2012-10-10 12:00:07.000-0600");
		statsExpecter.expectNothing();
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,443.1,54,2012-10-10 12:00:08.000-0600");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "BBB,55.0,30,2012-10-10 12:00:09.000-0600");
		statsExpecter.expectNothing();
		
		// send tuple
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,9.1,970,2012-10-10 12:00:10.000-0600");
		statsExpecter.expect(ObjectArrayTupleMaker.MAKER, new Object[]{
				"AAA",		// Symbol 
				257.22,		// AvgPrice
				443.1,		// MaxPrice
				54.2,		// MinPrice
				154.688193,	// StdPrice
				443.1,		// LastPrice
				54,			// LastQunatity
				"2012-10-10 12:00:08.000-0600",	// LastTime
		});
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "BBB,15.0,10,2012-10-10 12:00:11.000-0600");
		statsExpecter.expect(ObjectArrayTupleMaker.MAKER, new Object[]{
				"BBB",		// Symbol 
				71.0,		// AvgPrice
				90.0,		// MaxPrice
				55.0,		// MinPrice
				14.317821,	// StdPrice
				55.0,		// LastPrice
				30,			// LastQunatity
				"2012-10-10 12:00:09.000-0600",	// LastTime
		});
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,987.1,422,2012-10-10 12:00:12.000-0600");
		statsExpecter.expect(ObjectArrayTupleMaker.MAKER, new Object[]{
				"AAA",		// Symbol 
				229.38,		// AvgPrice
				443.1,		// MaxPrice
				9.1,		// MinPrice
				188.107940,	// StdPrice
				9.1,		// LastPrice
				970,			// LastQunatity
				"2012-10-10 12:00:10.000-0600",	// LastTime
		});

	}
	

	@Test
	public void testSingleSymbol() throws Exception {
		
		TestQuoteTupleMaker maker = new TestQuoteTupleMaker("AAA");
		qouteEnqueuer.enqueue(maker, new NextTuple("AAA", 0, 0, SECOND)); 	// open window; t=0
		statsExpecter.expectNothing();
		
		qouteEnqueuer.enqueue(maker, new NextTuple("AAA", 0, 0, 2*SECOND));	// t+3
		statsExpecter.expectNothing();
		
		qouteEnqueuer.enqueue(maker, new NextTuple("AAA", 0, 0, 3*SECOND));	// t+6 
		statsExpecter.expectNothing();
		
		qouteEnqueuer.enqueue(maker, new NextTuple("AAA", 0, 0, 4*SECOND)); // t+10, emit window
		statsExpecter.expectNothing();
		
		String lastDate = maker.getLastTupleDateTime();
		qouteEnqueuer.enqueue(maker, new NextTuple("AAA", 0, 0, SECOND));
		statsExpecter.expect(ObjectArrayTupleMaker.MAKER, maker.buildGenericResultTupleObject("AAA", lastDate));
		
	}
	
	@After
	public void stopContainers() throws Exception {
		server.stopContainers();
	}


}
