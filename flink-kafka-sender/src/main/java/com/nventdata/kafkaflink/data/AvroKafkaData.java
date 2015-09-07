package com.nventdata.kafkaflink.data;

import java.io.Serializable;

public class AvroKafkaData implements Serializable{
	private static final long serialVersionUID = -3393124106801607313L;
	private Integer id;
	private Integer randNum;
	private String data;
	
	public AvroKafkaData(){}
	
	public AvroKafkaData(Integer id, Integer randNum, String data) {
		super();
		this.id = id;
		this.randNum = randNum;
		this.data = data;
	}

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public Integer getRandNum() {
		return randNum;
	}

	public void setRandNum(Integer randNum) {
		this.randNum = randNum;
	}

	public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}

	@Override
	public String toString() {
		return "{\"id\": " + id + ", \"random\": " + randNum + ", \"data\" :" + data + "}";
	}

	

}
