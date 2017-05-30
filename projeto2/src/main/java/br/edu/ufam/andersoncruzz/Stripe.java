package br.edu.ufam.andersoncruzz;

import java.util.ArrayList;
import java.util.List;

public class Stripe {
	String key;
	List<Key_value> listValue = new ArrayList<Key_value>();
	
	public void addKeyValue (String key, int value){
		Key_value obj = new Key_value(key, value);
		listValue.add(obj);
	}
	
	public void setKey(String key){
		this.key = key;
	}
	
	public String getKey(){
		return this.key;
	}
	public List<Key_value> getListValue(){
		return this.listValue;
	}
}
