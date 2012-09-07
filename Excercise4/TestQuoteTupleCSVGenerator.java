import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;


/**
 * Utility class to generate test tuples following QuoteSchema
 * 
 * @author Alfonso Kim
 *
 */
public class TestQuoteTupleCSVGenerator {
	
	/**
	 * Representation of a QuoteSchema 
	 */
	private class QuoteData {
		public String symbol;
		public double price;
		public int quantity;
		public Calendar timeCalendar;
		
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
			this.timeCalendar = Calendar.getInstance();
		}
		
		/**
		 * Increase a quote values
		 * 
		 * @param priceInc price increment
		 * @param quantityInc quantity increment
		 * @param secondsInc seconds increment
		 */
		private void increase(double priceInc, int quantityInc, int secondsInc){
			this.price += priceInc;
			this.quantity += quantityInc;
			this.timeCalendar.add(Calendar.SECOND, secondsInc);
		}
		
		@Override
		public String toString(){
			return new StringBuilder()
				.append(symbol).append(",")
				.append(price).append(",")
				.append(quantity).append(",")
				.append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ").format(timeCalendar.getTime()))
				.toString();
		}
		
	}
	
	private Map<String, QuoteData> quotes;

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
	public String nextQuote(String symbol, double priceInc, int quantityInc, int secondsInc){
		if (quotes.containsKey(symbol)){
			QuoteData quote = quotes.get(symbol);
			quote.increase(priceInc, quantityInc, secondsInc);
			return quote.toString();
		}
		return "";
	}
	


}
