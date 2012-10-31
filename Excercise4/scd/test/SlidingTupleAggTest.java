package scd.test;
import java.util.Random;

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


public class SlidingTupleAggTest {

	private static SBServerManager server;
	private static Enqueuer qouteEnqueuer;
	private static Expecter statsExpecter;
	private static TestQuoteTupleMaker maker;
	private final static int WINDOW_SIZE = 10;
	private final static int SECOND = 1;

	@BeforeClass
	public static void setupServer() throws Exception {
		server = ServerManagerFactory.getEmbeddedServer();
		server.startServer();
		server.loadApp("SlidingTupleAgg.sbapp");
		
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
		maker = new TestQuoteTupleMaker("AAA", "BBB", "CCC", "DDD", "EEE");
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
		// send tuple
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,80.0,50,2012-10-10 12:00:09.000-0600");
		statsExpecter.expect(ObjectArrayTupleMaker.MAKER, new Object[]{
				"AAA",		// Symbol 
				43.0,		// AvgPrice
				80.0,		// MaxPrice
				10.0,		// MinPrice
				23.357130721806467,		// StdPrice
				80.0,		// LastPrice
				50,			// LastQunatity
				"2012-10-10 12:00:09.000-0600",	// LastTime
		});
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,90.0,50,2012-10-10 12:00:10.000-0600");
		statsExpecter.expect(ObjectArrayTupleMaker.MAKER, new Object[]{
				"AAA",		// Symbol 
				51.0,		// AvgPrice
				90.0,		// MaxPrice
				15.0,		// MinPrice
				24.472206,		// StdPrice
				90.0,		// LastPrice
				50,			// LastQunatity
				"2012-10-10 12:00:10.000-0600",	// LastTime
		});
	}
	
	
	/**
	 * This also will test aggregate functions with 2 symbols
	 * given price and quantity not increasing monotonically
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
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,213.9,652,2012-10-10 12:00:10.000-0600");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "BBB,45.0,30,2012-10-10 12:00:11.000-0600");
		statsExpecter.expectNothing();
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,901.2,219,2012-10-10 12:00:12.000-0600");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "BBB,40.0,25,2012-10-10 12:00:13.000-0600");
		statsExpecter.expectNothing();
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,43.9,12,2012-10-10 12:00:14.000-0600");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "BBB,30.0,15,2012-10-10 12:00:15.000-0600");
		statsExpecter.expectNothing();
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,539.0,321,2012-10-10 12:00:16.000-0600");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "BBB,25.0,15,2012-10-10 12:00:17.000-0600");
		statsExpecter.expectNothing();
		
		// send tuple
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,9.1,970,2012-10-10 12:00:18.000-0600");
		statsExpecter.expect(ObjectArrayTupleMaker.MAKER, new Object[]{
				"AAA",		// Symbol 
				299.32,		// AvgPrice
				901.2,		// MaxPrice
				9.1,		// MinPrice
				275.0849234529,		// StdPrice
				9.1,		// LastPrice
				970,			// LastQunatity
				"2012-10-10 12:00:18.000-0600",	// LastTime
		});
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "BBB,15.0,10,2012-10-10 12:00:19.000-0600");
		statsExpecter.expect(ObjectArrayTupleMaker.MAKER, new Object[]{
				"BBB",		// Symbol 
				51.0,		// AvgPrice
				90.0,		// MaxPrice
				15.0,		// MinPrice
				24.472206457303,		// StdPrice
				15.0,		// LastPrice
				10,			// LastQunatity
				"2012-10-10 12:00:19.000-0600",	// LastTime
		});
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,987.1,422,2012-10-10 12:00:20.000-0600");
		statsExpecter.expect(ObjectArrayTupleMaker.MAKER, new Object[]{
				"AAA",		// Symbol 
				383.2,		// AvgPrice
				987.1,		// MaxPrice
				9.1,		// MinPrice
				343.336760500,		// StdPrice
				987.1,		// LastPrice
				422,			// LastQunatity
				"2012-10-10 12:00:20.000-0600",	// LastTime
		});
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "BBB,10.0,10,2012-10-10 12:00:21.000-0600");
		statsExpecter.expect(ObjectArrayTupleMaker.MAKER, new Object[]{
				"BBB",		// Symbol 
				43.0,		// AvgPrice
				80.0,		// MaxPrice
				10.0,		// MinPrice
				23.35713072,		// StdPrice
				10.0,		// LastPrice
				10,			// LastQunatity
				"2012-10-10 12:00:21.000-0600",	// LastTime
		});
	}


	@Test
	public void testSingleSymbol() throws Exception {
		qouteEnqueuer.enqueue(maker, new NextTuple("AAA", 0, 0, SECOND));
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(maker, new NextTuple("AAA", 0, 0, SECOND));
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(maker, new NextTuple("AAA", 0, 0, SECOND));
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(maker, new NextTuple("AAA", 0, 0, SECOND));
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(maker, new NextTuple("AAA", 0, 0, SECOND));
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(maker, new NextTuple("AAA", 0, 0, SECOND));
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(maker, new NextTuple("AAA", 0, 0, SECOND));
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(maker, new NextTuple("AAA", 0, 0, SECOND));
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(maker, new NextTuple("AAA", 0, 0, SECOND));
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(maker, new NextTuple("AAA", 0, 0, SECOND));
		statsExpecter.expect(ObjectArrayTupleMaker.MAKER, maker.buildGenericResultTupleObject("AAA"));

	}
	
	/**
	 * Test sequential tuples. The 10th tuple must close and emit the window
	 */
	@Test
	public void testSequentialSingleSymbol() throws Exception {
		for (String symbol: maker.getRegisteredSymbols()){
			for (int i = 0; i < WINDOW_SIZE-1; i++){
				qouteEnqueuer.enqueue(maker, new NextTuple(symbol, 0, 0, SECOND));
				statsExpecter.expectNothing();
			}
			qouteEnqueuer.enqueue(maker, new NextTuple(symbol, 0, 0, SECOND));
			statsExpecter.expect(ObjectArrayTupleMaker.MAKER,  maker.buildGenericResultTupleObject(symbol));
		}
	}
	
	
	/**
	 * Test tuples randomly, a tuple must be received if the window has 10 or more tuples with the same symbol.
	 */
	@Test
	public void testShufflingSymbols() throws Exception {
		Object[] testingSymbols = maker.getRegisteredSymbols().toArray();
		Random random = new Random();
		for (int i = 0; i < 1000; i++){
			String symbol = (String)testingSymbols[random.nextInt(testingSymbols.length)];
			qouteEnqueuer.enqueue(maker, new NextTuple(symbol, 0, 0, SECOND));
			if (maker.getTupleCount(symbol) >= WINDOW_SIZE){
				statsExpecter.expect(ObjectArrayTupleMaker.MAKER, maker.buildGenericResultTupleObject(symbol));
			} else {
				statsExpecter.expectNothing();
			}
		}
	}

	@After
	public void stopContainers() throws Exception {
		server.stopContainers();
	}
	

}
