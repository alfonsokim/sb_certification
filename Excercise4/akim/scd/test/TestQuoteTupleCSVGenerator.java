package akim.scd.test;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


/**
 * Utility class to generate test tuples following QuoteSchema
 * 
 * @author Alfonso Kim
 *
 */
public class TestQuoteTupleCSVGenerator {
	
	private Map<String, QuoteData> quotes;
	private Calendar timeCalendar;

	/**
	 * Creates a TestQuoteTupleCSVGenerator whit given symbols
	 * 
	 * @param symbols Symbols to use in the test
	 */
	public TestQuoteTupleCSVGenerator(String... symbols) {
		quotes = new HashMap<String, QuoteData>();
		for(String symbol : symbols){
			quotes.put(symbol, new QuoteData(symbol));
		}
		this.timeCalendar = Calendar.getInstance();
	}
	
	/**
	 * @return The declared symbols
	 */
	public Set<String> getSymbols(){
		return quotes.keySet();
	}
	
	/**
	 * @return Last time a tuple was returned
	 */
	public String getLastTimeWithFormat(){
		return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ").format(timeCalendar.getTime());
	}
	
	/**
	 * @param symbol The symbol name to check
	 * @return		 The times a symbol has been fired, -1 if the symbol name is not in declared
	 */
	public int getCountSimboleFired(String symbol){
		if (quotes.containsKey(symbol)){
			return quotes.get(symbol).tupleCount;
		}
		return -1;
	}
	
	
	/**
	 * Increases a quote values and returns its string representation
	 * 
	 * @param symbol symbol name to increment
	 * @param priceInc price increment
	 * @param quantityInc quantity increment
	 * @param secondsInc seconds increment
	 * 
	 * @return the CSV representation of the quote identified by symbol, empty string if symbol not found
	 */
	public String nextQuote(String symbol, double priceInc, int quantityInc, int millisecsInc){
		timeCalendar.add(Calendar.MILLISECOND, millisecsInc);
		if (quotes.containsKey(symbol)){
			QuoteData quote = quotes.get(symbol);
			quote.tupleCount += 1;
			return quote.increase(priceInc, quantityInc).toCSVTuple(timeCalendar.getTime());
		}
		throw new IllegalArgumentException(symbol + " was not declared in this generator");
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
			this.price = 10D;
			this.quantity = 10;
			this.tupleCount = 0;
		}
		
		/**
		 * Increase a quote values
		 * 
		 * @param priceInc price increment
		 * @param quantityInc quantity increment
		 * @param secondsInc seconds increment
		 */
		private QuoteData increase(double priceInc, int quantityInc){
			this.price += priceInc;
			this.quantity += quantityInc;
			return this;
		}
		
		/**
		 * @param sendDate The date for the time field
		 * @return the CSV representation of the symbol, with the time field updated to sendDate
		 */
		private String toCSVTuple(Date sendDate){
			return new StringBuilder()
			.append(symbol).append(",")
			.append(price).append(",")
			.append(quantity).append(",")
			.append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ").format(sendDate))
			.toString();
		}
		
		
	}
	

}