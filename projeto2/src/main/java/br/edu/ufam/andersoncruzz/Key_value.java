package br.edu.ufam.andersoncruzz;

import java.awt.RenderingHints.Key;

public class Key_value {
	public String key;
	public double pmi;
	
	public Key_value(String key, double value){
		this.key = key;
		this.pmi = value;
	}
	public String getKey(){
		return this.key;
	}
	public double getPmi(){
		return this.pmi;
	}
}
