package scd.test;

/**
 * Runs all the unit tests
 */
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ SilidingTupleQueryTest.class, SlidingTimeAggTest.class,
				SlidingTupleAggTest.class, TumblingTimeAggTest.class,
				TumblingTupleAggTest.class, TumblingTupleQueryTest.class 
			  })
/**
 * Runner for all the test cases
 * 
 * @author Alfonso Kim
 */
public class AllTestsRunner {
	// JUnit will do the job :)
}
