package com.gerkenip.elasticsearch.exception;

public class EsServerException extends EsException {

	public EsServerException() {
		super("Server not started or at wrong version");
	}

}
