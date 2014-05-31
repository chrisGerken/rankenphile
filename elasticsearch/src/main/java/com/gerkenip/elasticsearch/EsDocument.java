package com.gerkenip.elasticsearch;

public class EsDocument {

	public String index;
	public String type;
	public String id;
	public String doc;
	
	public EsDocument(String index, String type, String id, String doc) {
		this.index = index;
		this.type = type;
		this.id = id;
		this.doc = doc;
	}

}
