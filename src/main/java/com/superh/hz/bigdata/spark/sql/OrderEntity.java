package com.superh.hz.bigdata.spark.sql;

public class OrderEntity {

	private int orderNo;
	private int orderTypeNo;
	private double amount;
	
	public OrderEntity(int orderNo, int orderTypeNo, double amount) {
		super();
		this.orderNo = orderNo;
		this.orderTypeNo = orderTypeNo;
		this.amount = amount;
	}
	
	public OrderEntity() {
		super();
	}
	
	public int getOrderTypeNo() {
		return orderTypeNo;
	}
	public void setOrderTypeNo(int orderTypeNo) {
		this.orderTypeNo = orderTypeNo;
	}
	
	public int getOrderNo() {
		return orderNo;
	}
	public void setOrderNo(int orderNo) {
		this.orderNo = orderNo;
	}

	public double getAmount() {
		return amount;
	}

	public void setAmount(double amount) {
		this.amount = amount;
	}
	
}
