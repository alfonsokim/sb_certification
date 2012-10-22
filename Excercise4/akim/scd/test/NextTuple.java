/**
 * 
 */
package akim.scd.test;

/**
 * Represents the next tuple to send
 * 
 * @author Alfonso Kim
 */
public class NextTuple {
	
	public String symbol;
	public int deltaQuantity;
	public double deltaPrice;
	public int deltaSeconds;
	
	public NextTuple(String symbol, int deltaQuantity, double deltaPrice, int deltaSeconds) {
		this.symbol = symbol;
		this.deltaQuantity = deltaQuantity;
		this.deltaPrice = deltaPrice;
		this.deltaSeconds = deltaSeconds;
	}
	

}
