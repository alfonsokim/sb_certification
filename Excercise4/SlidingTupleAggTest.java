import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.streambase.sb.unittest.Enqueuer;
import com.streambase.sb.unittest.Expecter;
import com.streambase.sb.unittest.CSVTupleMaker;
import com.streambase.sb.unittest.SBServerManager;
import com.streambase.sb.unittest.ServerManagerFactory;


public class SlidingTupleAggTest {

	private static SBServerManager server;
	private static Enqueuer qouteEnqueuer;
	private static Expecter statsExpecter;
	private static TestQuoteTupleCSVGenerator tupleGenerator;

	@BeforeClass
	public static void setupServer() throws Exception {
		server = ServerManagerFactory.getEmbeddedServer();
		server.startServer();
		server.loadApp("SlidingTupleAgg.sbapp");
		qouteEnqueuer = server.getEnqueuer("InQuote");
		statsExpecter = new Expecter(server.getDequeuer("OutStats"));
		tupleGenerator = new TestQuoteTupleCSVGenerator("AAA", "BBB", "CCC", "DDD", "EEE");
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
	}

	@Test
	public void testSingleSymbol() throws Exception {
		int i = 0;
		for(; i < 9; i++){
			qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, tupleGenerator.nextQuote("AAA", 1+i, 1+i, 10+i));
			statsExpecter.expectNothing();
		}
		qouteEnqueuer.enqueue(CSVTupleMaker.MAKER, tupleGenerator.nextQuote("AAA", 1+i, 1+i, 10+i));
		statsExpecter.expect();

		/*
		 * Example dequeuer using an alternate TupleMaker that converts Java Objects to
		 * Tuples. ObjectArrayTupleMaker maps Java Objects to Tuple field values
		 */
		//statsExpecter.expect(ObjectArrayTupleMaker.MAKER, new Object[] { "[REPLACE THIS]" });
	}

	@After
	public void stopContainers() throws Exception {
		// after each test, dispose of the container instances
		server.stopContainers();
	}

}
