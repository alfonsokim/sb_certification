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


public class SlidingTupleAggTest {

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
		tupleGenerator = new TestQuoteTupleCSVGenerator("AAA", "BBB", "CCC", "DDD", "EEE");
	}

	/**
	 * My first test method will test the min, max, standard deviation and 
	 * lastval(T) functions for given values. 
	 * 
	 * In further tests I will assume that you guys have tested those functions a 
	 * zillion times before. So next test's will check that the application fires 
	 * tuples when supposed to be fired. In other worlds, The test will focus in 
	 * receiving tuples when the window closes.
	 */
	@Test
	public void testSingleSymbol() throws Exception {
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,1,11,2012-08-01 11:00:00.000-0500");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,2,11,2012-08-01 12:00:00.000-0500");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,3,13,2012-08-01 13:00:00.000-0500");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,4,14,2012-08-01 14:00:00.000-0500");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,5,15,2012-08-01 15:00:00.000-0500");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,6,16,2012-08-01 16:00:00.000-0500");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,7,17,2012-08-01 17:00:00.000-0500");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,8,18,2012-08-01 18:00:00.000-0500");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,9,19,2012-08-01 19:00:00.000-0500");
		statsExpecter.expectNothing();
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, "AAA,10,20,2012-08-01 20:00:00.000-0500");
		statsExpecter.expect(ObjectArrayTupleMaker.MAKER, new Object[] { 
				"AAA",		// Symbol 
				"5.5",		// AvgPrice
				"10",		// MaxPrice
				"1",		// MinPrice
				"3.0276503540974917",		// StdPrice
				"10",		// LastPrice
				"20",		// LastQunatity
				"2012-08-01 20:00:00.000-0500",		// LastTime
				} );

	}
	
	/**
	 * Test sequential tuples. The 10th tuple must close and emit the window
	 */
	@Test
	public void testSequentialSingleSymbol() throws Exception {
		for (String symbol: tupleGenerator.getSymbols()){
			for (int i = 0; i < WINDOW_SIZE-1; i++){
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
			if (tupleGenerator.getCountSimbolFired(symbol) >= WINDOW_SIZE){
				statsExpecter.expect(ObjectArrayTupleMaker.MAKER, buildGenericResultTupleObject(symbol));
			} else {
				statsExpecter.expectNothing();
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
