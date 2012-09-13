package akim.scd.test;
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


public class TumblingTimeAggTest {

	private static SBServerManager server;
	private static Enqueuer qouteEnqueuer;
	private static Expecter statsExpecter;
	private static TestQuoteTupleCSVGenerator tupleGenerator;
	
	private final static int WINDOW_SIZE = 10;
	private final static int SECOND = 1000;

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
		tupleGenerator = new TestQuoteTupleCSVGenerator("AAA", "BBB", "CCC", "DDD", "EEE");
	}

	@Test
	public void testSingleSymbol() throws Exception {
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,1,10,2012-08-01 10:00:01.000-0500");
		statsExpecter.expectNothing();
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,2,20,2012-08-01 10:00:02.000-0500");
		statsExpecter.expectNothing();
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,3,30,2012-08-01 10:00:03.000-0500");
		statsExpecter.expectNothing();
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,4,40,2012-08-01 10:00:04.000-0500");
		statsExpecter.expectNothing();
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,5,50,2012-08-01 10:00:05.000-0500");
		statsExpecter.expectNothing();
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,6,60,2012-08-01 10:00:06.000-0500");
		statsExpecter.expectNothing();
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,7,70,2012-08-01 10:00:07.000-0500");
		statsExpecter.expectNothing();
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,8,80,2012-08-01 10:00:08.000-0500");
		statsExpecter.expectNothing();
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,9,90,2012-08-01 10:00:09.000-0500");
		statsExpecter.expectNothing();
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,10,20,2012-08-01 10:00:10.000-0500");
		statsExpecter.expectNothing();
		
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,11,21,2012-08-01 10:00:11.000-0500");
		statsExpecter.expect(ObjectArrayTupleMaker.MAKER, new Object[] { 
				"AAA",		// Symbol 
				"6",		// AvgPrice
				"11",		// MaxPrice
				"1",		// MinPrice
				"3.3166247903554",		// StdPrice
				"11",		// LastPrice
				"21",		// LastQunatity
				"2012-08-01 10:00:11.000-0500",		// LastTime
				} );
		
	}
	
	/**
	 * Test sequential tuples. The 10th tuple must close and emit the window
	 */
	@Test
	public void testSequentialSingleSymbol() throws Exception {
		for (String symbol: tupleGenerator.getSymbols()){
			for (int i = 0; i < WINDOW_SIZE; i++){
				qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, tupleGenerator.nextQuote(symbol, 0, 0, SECOND));
				statsExpecter.expectNothing();
			}
			qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, tupleGenerator.nextQuote(symbol, 0, 0, SECOND));
			statsExpecter.expect(ObjectArrayTupleMaker.MAKER, buildGenericResultTupleObject(symbol));
		}
	}
	
	/**
	 * Test tuples randomly, a tuple must be received if the window has 10 or more tuples with the same symbol.
	 */
	@Test
	public void testShufflingSymbols() throws Exception {
		Object[] testingSymbols = tupleGenerator.getSymbols().toArray();
		Random random = new Random();
		for (int i = 0; i < 1000; i++){
			String symbol = (String)testingSymbols[random.nextInt(testingSymbols.length)];
			qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, tupleGenerator.nextQuote(symbol, 0, 0, SECOND));
			if (i > 0 && i % WINDOW_SIZE+1 == 0){
				statsExpecter.expect(ObjectArrayTupleMaker.MAKER, buildGenericResultTupleObject(symbol));
			}
		}
	}

	@After
	public void stopContainers() throws Exception {
		server.stopContainers();
	}
	
	/**
	 * Builds a generic tuple with default values, given the symbol name
	 * 
	 * @param symbol	The symbol name of the generic tuple
	 * @return			Default values for the generic tuple:
	 * 						- average price = 10
	 * 						- max price = 10
	 * 						- min price = 10
	 * 						- standard deviation = 0
	 * 						- last price = 10
	 * 						- last quantity = 10
	 * 						- last time a tuple was fired
	 */
	private Object[] buildGenericResultTupleObject(String symbol){
		return new Object[] { 
				symbol,		// Symbol 
				"10",		// AvgPrice
				"10",		// MaxPrice
				"10",		// MinPrice
				"0",		// StdPrice
				"10",		// LastPrice
				"10",		// LastQunatity
				tupleGenerator.getLastTimeWithFormat(),		// LastTime
				};
	}

}
