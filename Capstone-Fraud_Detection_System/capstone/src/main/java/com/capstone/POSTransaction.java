package com.capstone;

import java.io.Serializable;

/*
 * this is used to store pos transaction details, acquired from kafka stream
 */
public class POSTransaction implements Serializable {

	private static final long serialVersionUID = 1L;

	private String card_id;
	private String member_id;
	private String amount;
	private String postcode;
	private String pos_id;
	private String transaction_dt;
	private String status;

	public String getCardId() {
		return this.card_id;
	}

	public String getMemberId() {
		return this.member_id;
	}

	public String getAmount() {
		return this.amount;
	}

	public String getPOSId() {
		return this.pos_id;
	}

	public String getPostCode() {
		return this.postcode;
	}

	public String getTransactionDate() {
		return this.transaction_dt;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getStatus() {
		return this.status;
	}

	@Override
	public String toString() {
		return "POS [card_id " + this.card_id + " member_id " + this.member_id + " amount " + this.amount + " pos_id "
				+ this.pos_id + " postcode " + this.postcode + " transaction_dt " + this.transaction_dt + " status "
				+ this.status + " ]";
	}

}
