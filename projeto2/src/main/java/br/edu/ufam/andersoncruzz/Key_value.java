package br.edu.ufam.andersoncruzz;

import java.awt.RenderingHints.Key;

public class Key_value {
	public String key;
	public int value;
	
	public Key_value(String key, int value){
		this.key = key;
		this.value = value;
	}
	public String getKey(){
		return this.key;
	}
	public int getValue(){
		return this.value;
	}
}
