package tutorial;

import java.io.Serializable;

public class PriceData implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private float close;
	private float high;
	private float low;
	private float open;
	private float volume;
	public float getClose() {
		return close;
	}
	public void setClose(float close) {
		this.close = close;
	}
	public float getHigh() {
		return high;
	}
	public void setHigh(float high) {
		this.high = high;
	}
	public float getLow() {
		return low;
	}
	public void setLow(float low) {
		this.low = low;
	}
	public float getOpen() {
		return open;
	}
	public void setOpen(float open) {
		this.open = open;
	}
	public float getVolume() {
		return volume;
	}
	public void setVolume(float volume) {
		this.volume = volume;
	}
	public static long getSerialversionuid() {
		return serialVersionUID;
	}
	
	
	
	
}
