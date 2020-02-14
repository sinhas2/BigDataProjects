package tutorial;

import java.io.Serializable;

public class Data implements Serializable{
	private static final long serialVersionUID = 8757809179891063548L;
	private String symbol;
	private String timestamp;
	private PriceData priceData;
	public String getSymbol() {
		return symbol;
	}
	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}
	public String getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}
	public PriceData getPriceData() {
		return priceData;
	}
	public void setPriceData(PriceData priceData) {
		this.priceData = priceData;
	}
	
	
}
