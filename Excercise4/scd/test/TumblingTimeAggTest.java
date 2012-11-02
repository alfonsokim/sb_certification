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

/**
 * @author Alfonso Kim
 */
public class TumblingTimeAggTest {

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
		server.loadApp("TumblingTimeAgg.sbapp");
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
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,80.0,50,2012-10-10 12:00:09.000-0600");
		statsExpecter.expectNothing();
		// send tuple
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,90.0,50,2012-10-10 12:00:10.000-0600");
		statsExpecter.expect(ObjectArrayTupleMaker.MAKER, new Object[]{
				"AAA",		// Symbol 
				47.272728,	// AvgPrice
				90.0,		// MaxPrice
				10.0,		// MinPrice
				26.302437,	// StdPrice
				90.0,		// LastPrice
				50,			// LastQunatity
				"2012-10-10 12:00:10.000-0600",	// LastTime
		});
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,100.0,100,2012-10-10 12:00:11.000-0600");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,90.0,95,2012-10-10 12:00:12.000-0600");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,80.0,90,2012-10-10 12:00:13.000-0600");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,70.0,80,2012-10-10 12:00:14.000-0600");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,60.0,80,2012-10-10 12:00:15.000-0600");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,50.0,70,2012-10-10 12:00:16.000-0600");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,40.0,65,2012-10-10 12:00:17.000-0600");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,30.0,60,2012-10-10 12:00:18.000-0600");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,20.0,60,2012-10-10 12:00:19.000-0600");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,10.0,50,2012-10-10 12:00:20.000-0600");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,5.0,55,2012-10-10 12:00:21.000-0600");
		statsExpecter.expect(ObjectArrayTupleMaker.MAKER, new Object[]{
				"AAA",		// Symbol 
				50.454545,	// AvgPrice
				100.0,		// MaxPrice
				5.0,		// MinPrice
				32.438753,	// StdPrice
				5.0,		// LastPrice
				55,			// LastQunatity
				"2012-10-10 12:00:21.000-0600",	// LastTime
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
		statsExpecter.expect(ObjectArrayTupleMaker.MAKER, new Object[]{
				"AAA",		// Symbol 
				250.0,		// AvgPrice
				443.1,		// MaxPrice
				54.2,		// MinPrice
				139.483055,	// StdPrice
				213.9,		// LastPrice
				652,			// LastQunatity
				"2012-10-10 12:00:10.000-0600",	// LastTime
		});
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "BBB,45.0,30,2012-10-10 12:00:11.000-0600");
		statsExpecter.expect(ObjectArrayTupleMaker.MAKER, new Object[]{
				"BBB",		// Symbol 
				66.666667,	// AvgPrice
				90.0,		// MaxPrice
				45.0,		// MinPrice
				16.63330,	// StdPrice
				45.0,		// LastPrice
				30,			// LastQunatity
				"2012-10-10 12:00:11.000-0600",	// LastTime
		});
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,901.2,219,2012-10-10 12:00:15.000-0600");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "BBB,40.0,25,2012-10-10 12:00:16.000-0600");
		statsExpecter.expectNothing();
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,43.9,12,2012-10-10 12:00:21.000-0600");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "BBB,30.0,15,2012-10-10 12:00:22.000-0600");
		statsExpecter.expectNothing();
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,539.0,321,2012-10-10 12:00:25.000-0600");
		statsExpecter.expect(ObjectArrayTupleMaker.MAKER, new Object[]{
				"AAA",		// Symbol 
				494.7,		// AvgPrice
				901.2,		// MaxPrice
				43.9,		// MinPrice
				430.363440,	// StdPrice
				539.0,		// LastPrice
				321,			// LastQunatity
				"2012-10-10 12:00:25.000-0600",	// LastTime
		});
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "BBB,25.0,15,2012-10-10 12:00:26.000-0600");
		statsExpecter.expect(ObjectArrayTupleMaker.MAKER, new Object[]{
				"BBB",		// Symbol 
				31.66667,	// AvgPrice
				40.0,		// MaxPrice
				25.0,		// MinPrice
				7.63763,	// StdPrice
				25.0,		// LastPrice
				15,			// LastQunatity
				"2012-10-10 12:00:26.000-0600",	// LastTime
		});
	}
	

	@Test
	public void testSingleSymbol() throws Exception {
		qouteEnqueuer.enqueue(maker, new NextTuple("AAA", 0, 0, SECOND));
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(maker, new NextTuple("AAA", 0, 0, 2*SECOND));
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(maker, new NextTuple("AAA", 0, 0, 3*SECOND));
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(maker, new NextTuple("AAA", 0, 0, 4*SECOND));
		
		qouteEnqueuer.enqueue(maker, new NextTuple("AAA", 0, 0, SECOND));
		statsExpecter.expect(ObjectArrayTupleMaker.MAKER, maker.buildGenericResultTupleObject("AAA"));
		
	}
	
	/**
	 * Test sequential tuples. The 10th tuple must close and emit the window
	 */
	@Test
	public void testSequentialSingleSymbol() throws Exception {
		for (String symbol: maker.getRegisteredSymbols()){
			for (int i = 0; i < WINDOW_SIZE; i++){
				qouteEnqueuer.enqueue(maker, new NextTuple(symbol, 0, 0, SECOND));
				statsExpecter.expectNothing();
			}
			qouteEnqueuer.enqueue(maker, new NextTuple(symbol, 0, 0, SECOND));
			statsExpecter.expect(ObjectArrayTupleMaker.MAKER, maker.buildGenericResultTupleObject(symbol));
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
			if (i > 0 && i % WINDOW_SIZE+1 == 0){
				statsExpecter.expect(ObjectArrayTupleMaker.MAKER, maker.buildGenericResultTupleObject(symbol));
			}
		}
	}

	@After
	public void stopContainers() throws Exception {
		server.stopContainers();
	}

}
