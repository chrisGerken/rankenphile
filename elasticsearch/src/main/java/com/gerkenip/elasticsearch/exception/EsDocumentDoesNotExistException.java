package com.gerkenip.elasticsearch.exception;

public class EsDocumentDoesNotExistException extends EsException {

	
	public EsDocumentDoesNotExistException(String index, String type, String id) {
		super("Document does not exist: index="+index+", type="+type+", id="+id);
	}

}
