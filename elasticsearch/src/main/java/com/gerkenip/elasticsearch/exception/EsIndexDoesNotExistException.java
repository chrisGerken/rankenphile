package com.gerkenip.elasticsearch.exception;

public class EsIndexDoesNotExistException extends EsException {
	
	public EsIndexDoesNotExistException(String index) {
		super("Reference to index '"+index+"' that does not exist");
	}
	
}
