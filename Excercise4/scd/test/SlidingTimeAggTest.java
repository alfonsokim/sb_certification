package scd.test;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.streambase.sb.unittest.Enqueuer;
import com.streambase.sb.unittest.Expecter;
import com.streambase.sb.unittest.ObjectArrayTupleMaker;
import com.streambase.sb.unittest.SBServerManager;
import com.streambase.sb.unittest.ServerManagerFactory;


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
