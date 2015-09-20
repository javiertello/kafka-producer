package com.kafka.producer;

public class Event {
	
	private int id;
	private int timestamp;
	private float value;
	private int property;
	private int plug_id;
	private int household_id;
	private int house_id;
	
	public Event (){ }

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(int timestamp) {
		this.timestamp = timestamp;
	}

	public double getValue() {
		return value;
	}

	public void setValue(float value) {
		this.value = value;
	}

	public int getProperty() {
		return property;
	}

	public void setProperty(int property) {
		this.property = property;
	}

	public int getPlug_id() {
		return plug_id;
	}

	public void setPlug_id(int plug_id) {
		this.plug_id = plug_id;
	}

	public int getHousehold_id() {
		return household_id;
	}

	public void setHousehold_id(int household_id) {
		this.household_id = household_id;
	}

	public int getHouse_id() {
		return house_id;
	}

	public void setHouse_id(int house_id) {
		this.house_id = house_id;
	}
	
	
}
