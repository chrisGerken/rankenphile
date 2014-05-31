package com.gerkenip.elasticsearch.exception;


public class EsTypeNotDefinedException extends EsException {
	
	public EsTypeNotDefinedException(String index, String type) {
		super("Type '"+type+"' not defined in index '"+index+"'");
	}
	
}
