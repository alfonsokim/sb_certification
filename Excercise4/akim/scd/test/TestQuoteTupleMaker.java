/**
 * 
 */
package akim.scd.test;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.streambase.sb.Schema;
import com.streambase.sb.StreamBaseException;
import com.streambase.sb.Tuple;
import com.streambase.sb.unittest.TupleMaker;

/**
 * Test tuple generator
 * 
 * Generates tuples according to QuoteSchema
 * 
 * @author Alfonso Kim
 */
public class TestQuoteTupleMaker implements TupleMaker<NextTuple> {
	
	private Map<String, QuoteData> quotes;	// handles all the symbols registered in the generator
	private Calendar timeCalendar;			// time is handled in the generator since the time increases monotonically with every tuple
	private Map<String, Object> fieldMap;	// to build the schema fields
	private final static SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");

	/**
	 * Constructor, receives a list of symbols that will be generated with every call
	 */
	public TestQuoteTupleMaker(String... symbols) {
		quotes = new HashMap<String, QuoteData>();
		for(String symbol : symbols){
			quotes.put(symbol, new QuoteData(symbol));
		}
		this.timeCalendar = Calendar.getInstance();
		fieldMap = new HashMap<String, Object>();
	}
	
	/**
	 * @param symbol 	The symbol to get
	 * @return			The times a symbol has been fired, -1 if the symbol is not registered
	 */
	public int getTupleCount(String symbol){
		QuoteData quote = quotes.get(symbol);
		return quote != null 
				? quote.tupleCount
				: -1;
	}
	
	
	/**
	 * @return The last time a tuple has been fired in this generator, in a string with format yyyy-MM-dd HH:mm:ss.SSSZ
	 */
	public String getLastTupleDateTime(){
		return SDF.format(timeCalendar.getTime());
	}
	
	
	/**
	 * @return All the symbols that this generator can use to create tuples
	 */
	public List<String> getRegisteredSymbols(){
		List<String> symList = new ArrayList<String>();
		symList.addAll(quotes.keySet());
		return symList;
	}

	
	/**
	 * @param symbol	The symbol to create the output tuple
	 * @param dateTime	The time this output tuple was emitted
	 * @return			An object array containing a default output tuple
	 * 					(all numeric values with zero)
	 */
	public Object[] buildGenericResultTupleObject(String symbol, String dateTime){
		return new Object[] { 
				symbol,		// Symbol 
				"00",		// AvgPrice
				"00",		// MaxPrice
				"00",		// MinPrice
				"0",		// StdPrice
				"00",		// LastPrice
				"00",		// LastQunatity
				dateTime,	// LastTime
				};
	}
	
	/**
	 * @param symbol	The symbol to create the output tuple
	 * @return			An object array containing a default output tuple
	 * 					(all numeric values with zero)
	 */
	public Object[] buildGenericResultTupleObject(String symbol){
		return buildGenericResultTupleObject(symbol, getLastTupleDateTime());
	}
	
	@Override
	public Tuple createTuple(Schema schema, NextTuple t)
			throws StreamBaseException {
		Tuple returnTuple = schema.createTuple();
		QuoteData symbolQuote = quotes.get(t.symbol);
		symbolQuote.update(t);
		symbolQuote.tupleCount += 1;
		timeCalendar.add(Calendar.SECOND, t.deltaSeconds);
		fieldMap.clear();
		// Let's hardcode the field names
		fieldMap.put("Symbol", symbolQuote.symbol);
		fieldMap.put("Price", symbolQuote.price);
		fieldMap.put("Quantity", symbolQuote.quantity);
		fieldMap.put("Time", timeCalendar.getTime());
		returnTuple.setFields(fieldMap);
		return returnTuple;
	}

	@Override
	public List<Tuple> createTuples(Schema schema, NextTuple... t) throws StreamBaseException {
		List<Tuple> returnTuples = new ArrayList<Tuple>();
		for(NextTuple nt : t){
			returnTuples.add(createTuple(schema, nt));
		}
		return returnTuples;
	}

	@Override
	public List<Tuple> createTuples(Schema schema, Collection<NextTuple> t) throws StreamBaseException {
		List<Tuple> returnTuples = new ArrayList<Tuple>();
		Iterator<NextTuple> nti = t.iterator();
		while (nti.hasNext()){
			returnTuples.add(createTuple(schema, nti.next()));
		}
		return returnTuples;
	}
	
	
	//**********************************************************************************************************
	
	/**
	 * Representation of a QuoteSchema 
	 */
	private class QuoteData {
		public String symbol;
		public double price;
		public int quantity;
		public int tupleCount;
		
		/**
		 * Creates a new QuoteData with given symbol and default price and quantity = 10
		 * creation time is assigned to time field
		 * 
		 * @param symbol
		 */
		private QuoteData(String symbol){
			this.symbol = symbol;
			this.price = 0D;
			this.quantity = 0;
			this.tupleCount = 0;
		}
		
		/**
		 * Increase a quote values
		 * 
		 * @param priceInc price increment
		 * @param quantityInc quantity increment
		 * @param secondsInc seconds increment
		 */
		private QuoteData update(NextTuple nt){
			this.price += nt.deltaPrice;
			this.quantity += nt.deltaQuantity;
			return this;
		}
		
		
	}

}
