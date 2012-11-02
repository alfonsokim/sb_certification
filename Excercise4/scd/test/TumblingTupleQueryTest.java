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


public class TumblingTupleQueryTest {

	private static SBServerManager server;
	private static Enqueuer qouteEnqueuer;
	private static Expecter statsExpecter;
	private static TestQuoteTupleMaker maker;
	
	private static int WINDOW_SIZE = 10;
	private static int SECOND = 1;

	@BeforeClass
	public static void setupServer() throws Exception {
		server = ServerManagerFactory.getEmbeddedServer();
		server.startServer();
		server.loadApp("TumblingTupleQuery.sbapp");
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
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,90.0,50,2012-10-10 12:00:09.000-0600");
		statsExpecter.expect(ObjectArrayTupleMaker.MAKER, new Object[]{
				"AAA",		// Symbol 
				44.0,		// AvgPrice
				90.0,		// MaxPrice
				10.0,		// MinPrice
				25.254263,	// StdPrice
				90.0,		// LastPrice
				50,			// LastQunatity
				"2012-10-10 12:00:09.000-0600",	// LastTime
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
		statsExpecter.expect(ObjectArrayTupleMaker.MAKER, new Object[]{
				"AAA",		// Symbol 
				55.0,		// AvgPrice
				100.0,		// MaxPrice
				10.0,		// MinPrice
				30.276504,	// StdPrice
				10.0,		// LastPrice
				50,			// LastQunatity
				"2012-10-10 12:00:20.000-0600",	// LastTime
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
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,21.9,192,2012-10-10 12:00:10.000-0600");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "BBB,50.0,30,2012-10-10 12:00:11.000-0600");
		statsExpecter.expectNothing();
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,59.3,98,2012-10-10 12:00:12.000-0600");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "BBB,55.0,30,2012-10-10 12:00:13.000-0600");
		statsExpecter.expectNothing();
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,821.3,410,2012-10-10 12:00:14.000-0600");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "BBB,50.0,30,2012-10-10 12:00:15.000-0600");
		statsExpecter.expectNothing();
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,100.9,21,2012-10-10 12:00:16.000-0600");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "BBB,50.0,35,2012-10-10 12:00:17.000-0600");
		statsExpecter.expectNothing();
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,213.9,652,2012-10-10 12:00:18.000-0600");
		statsExpecter.expect(ObjectArrayTupleMaker.MAKER, new Object[]{
				"AAA",		// Symbol 
				250.34,		// AvgPrice
				821.3,		// MaxPrice
				21.9,		// MinPrice
				243.649804,	// StdPrice
				213.9,		// LastPrice
				652,		// LastQunatity
				"2012-10-10 12:00:18.000-0600",	// LastTime
		});
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "BBB,45.0,30,2012-10-10 12:00:19.000-0600");
		statsExpecter.expect(ObjectArrayTupleMaker.MAKER, new Object[]{
				"BBB",		// Symbol 
				60.5,		// AvgPrice
				90.0,		// MaxPrice
				45.0,		// MinPrice
				14.804279,	// StdPrice
				45.0,		// LastPrice
				30,			// LastQunatity
				"2012-10-10 12:00:19.000-0600",	// LastTime
		});
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,148.3,290,2012-10-10 12:00:20.000-0600");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "BBB,90.0,50,2012-10-10 12:00:21.000-0600");
		statsExpecter.expectNothing();
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,328.5,120,2012-10-10 12:00:22.000-0600");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "BBB,80.0,50,2012-10-10 12:00:23.000-0600");
		statsExpecter.expectNothing();
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,54.2,920,2012-10-10 12:00:24.000-0600");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "BBB,70.0,45,2012-10-10 12:00:25.000-0600");
		statsExpecter.expectNothing();
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,312.0,32,2012-10-10 12:00:26.000-0600");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "BBB,60.0,35,2012-10-10 12:00:27.000-0600");
		statsExpecter.expectNothing();
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,443.1,54,2012-10-10 12:00:28.000-0600");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "BBB,55.0,30,2012-10-10 12:00:29.000-0600");
		statsExpecter.expectNothing();
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,21.9,192,2012-10-10 12:00:30.000-0600");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "BBB,50.0,30,2012-10-10 12:00:31.000-0600");
		statsExpecter.expectNothing();
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,59.3,98,2012-10-10 12:00:32.000-0600");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "BBB,55.0,30,2012-10-10 12:00:33.000-0600");
		statsExpecter.expectNothing();
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,821.3,410,2012-10-10 12:00:34.000-0600");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "BBB,50.0,30,2012-10-10 12:00:35.000-0600");
		statsExpecter.expectNothing();
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,100.9,21,2012-10-10 12:00:36.000-0600");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "BBB,50.0,35,2012-10-10 12:00:37.000-0600");
		statsExpecter.expectNothing();
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,539.0,321,2012-10-10 12:00:38.000-0600");
		statsExpecter.expect(ObjectArrayTupleMaker.MAKER, new Object[]{
				"AAA",		// Symbol 
				282.85,		// AvgPrice
				821.3,		// MaxPrice
				21.9,		// MinPrice
				259.425597,	// StdPrice
				539.0,		// LastPrice
				321,			// LastQunatity
				"2012-10-10 12:00:38.000-0600",	// LastTime
		});
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "BBB,25.0,15,2012-10-10 12:00:39.000-0600");
		statsExpecter.expect(ObjectArrayTupleMaker.MAKER, new Object[]{
				"BBB",		// Symbol 
				58.5,		// AvgPrice
				90.0,		// MaxPrice
				25.0,		// MinPrice
				18.112304,	// StdPrice
				25.0,		// LastPrice
				15,			// LastQunatity
				"2012-10-10 12:00:39.000-0600",	// LastTime
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
			if (maker.getTupleCount(symbol) % WINDOW_SIZE == 0){
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
