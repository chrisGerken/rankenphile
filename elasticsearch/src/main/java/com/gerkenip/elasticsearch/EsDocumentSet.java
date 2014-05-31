package com.gerkenip.elasticsearch;

import java.util.ArrayList;
import java.util.Iterator;

import org.codehaus.jettison.json.JSONObject;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;

import com.gerkenip.elasticsearch.exception.EsServerException;

/*
 * Represents a set of JSON documents that are to be sent in bulk to the server.  
 * 
 * This logic is factored out of EsClient to allow for multi-threaded bulk sends.
 * 
 */
public class EsDocumentSet {

	private EsClient esClient;

	private int	bulkInsertMaxCount = 2000;
	private int bulkInsertMaxSize = 1000000;
	private int bulkInsertCount = 0;
	private int bulkInsertSize = 0;
	private boolean sync = false;
	
	private ArrayList<EsDocument> docs = new ArrayList<EsDocument>();

	/*
	 * Creates a new EsDocumentSet for the given client.  Not to be called directly by users of this 
	 * component.  Use the {@link com.gerkenip.elasticsearch.EsClient#getDocumentSet(boolean) getDocumentSet(false)} method instead.
	 * 
	 */
	protected EsDocumentSet(EsClient esClient) {
		this.esClient = esClient;
		this.sync = false;
	}

	/*
	 * Creates a new EsDocumentSet for the given client.  Not to be called directly by users of this 
	 * component.  Use the {@link com.gerkenip.elasticsearch.EsClient#getDocumentSet(boolean) getDocumentSet(false)} method instead.
	 * 
	 */
	protected EsDocumentSet(EsClient esClient, boolean sync) {
		this.esClient = esClient;
		this.sync = sync;
	}

	/*
	 * Adds a JSON document for a given index, type and id to the set of documents
	 */
	public void putDocument(String index, String type, String id, JSONObject document ) throws EsServerException {
		putDocument(index,type,id,document.toString());
	}
	
	/*
	 * Adds the string representation of a JSON document for a given index, type and id to the set of documents
	 */
	public void putDocument(String index, String type, String id, String document ) throws EsServerException {
		
		if ((0 < bulkInsertCount) && (bulkInsertMaxSize <= (bulkInsertSize+document.length()))) {
			sendBulkInserts();
		}
		
		bulkInsertCount++;
		bulkInsertSize = bulkInsertSize + document.length();
		
		docs.add(new EsDocument(index, type, id, document));
		
		if ((bulkInsertCount >= bulkInsertMaxCount) || (bulkInsertMaxSize <= bulkInsertSize)) {
			sendBulkInserts();
		}
		
	}
	
	/* 
	 * Insert any pending documents
	 */
	public void close() {
		sendBulkInserts();
	}

	private void sendBulkInserts() {
		
		if (docs.isEmpty()) { return; }
		
		BulkRequestBuilder bulkRequestBuilder = null;
		try {

			synchronized (esClient) {
				TransportClient client = getClient();
				bulkRequestBuilder = client.prepareBulk();

				Iterator<EsDocument> iter = docs.iterator();
				while (iter.hasNext()) {
					EsDocument doc = iter.next();
					IndexRequestBuilder indexRequestBuilder = client.prepareIndex(doc.index,doc.type,doc.id).setSource(doc.doc);
					bulkRequestBuilder.add(indexRequestBuilder);
				}
			}
			ListenableActionFuture<BulkResponse> laf = bulkRequestBuilder.execute();
			if (sync) {
				laf.actionGet();
			}
			
		} catch (EsServerException e) {
			e.printStackTrace();
		}
		
		docs = new ArrayList<EsDocument>();
		bulkInsertCount = 0;
		bulkInsertSize = 0;
	}

	private TransportClient getClient() throws EsServerException {
		return esClient.getClient();
	}
}
