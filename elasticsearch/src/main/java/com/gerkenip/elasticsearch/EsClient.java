package com.gerkenip.elasticsearch;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.codehaus.jettison.json.JSONObject;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import com.gerkenip.elasticsearch.exception.EsDocumentDoesNotExistException;
import com.gerkenip.elasticsearch.exception.EsException;
import com.gerkenip.elasticsearch.exception.EsIndexDoesNotExistException;
import com.gerkenip.elasticsearch.exception.EsServerException;
import com.gerkenip.elasticsearch.exception.EsTypeNotDefinedException;

public class EsClient {

	private String host;
	private int port;
	private String cluster;
	
	private TransportClient _client = null;
	
	private ArrayList<InetSocketTransportAddress> otherHosts = new ArrayList<InetSocketTransportAddress>();

	private ArrayList<EsDocumentSet> docSets = new ArrayList<EsDocumentSet>();
	
	/*
	 * Construct a client for the named cluster running on the given host and port
	 */
	public EsClient(String host, int port, String cluster) {
		this.host = host;
		this.port = port;
		this.cluster = cluster;
	}
	
	/*
	 * Convenience constructor for the local, defaul cluster
	 */
	public static EsClient localClient() {
		return new EsClient("127.0.0.1", 9300, "elasticsearch");
	}
	
	/*
	 * Adds the host and port for another node in the cluster
	 */
	public void addOtherHost(String host, int port) {
		otherHosts.add(new InetSocketTransportAddress(host, port));
	}
	
	/*
	 * Closes all open document sets and then closes the client
	 */
	public void close() {
		Iterator<EsDocumentSet> iter = docSets.iterator();
		while (iter.hasNext()) {
			iter.next().close();
		}
		if (_client != null) {
			_client.close();
		}
	}
	
	/*
	 * Retrieves the document for the given index, type and id.  
	 */
	public String getDocument(String index, String type, String id) throws EsServerException, EsDocumentDoesNotExistException {
		
		GetResponse response = getClient().prepareGet(index, type, id).execute().actionGet();
		
		if (response.isExists()) {
			return response.getSourceAsString();
		}
		
		throw new EsDocumentDoesNotExistException(index, type, id);
	}
	
	/*
	 * Deletes the document for the given index, type and id
	 */
	public DeleteResponse deleteDocument(String index, String type, String id) throws ElasticSearchException, EsServerException {
		
		return getClient().prepareDelete(index, type, id).execute().actionGet();
		
	}
	
	/*
	 * Runs a query against the given index and type, then deletes all returned documents
	 */
	public DeleteByQueryResponse deleteDocuments(String index, String type, String querySource) throws ElasticSearchException, EsServerException {
		
		DeleteByQueryRequestBuilder rb = getClient().prepareDeleteByQuery(index);
		rb.setQuery(querySource);
		rb.setTypes(type);
		return rb.execute().actionGet();
		
	}
	
	/*
	 * Returns a new document set.  Specify whether or not bulk inserts are synchronous
	 */
	public EsDocumentSet getDocumentSet(boolean sync) {
		EsDocumentSet docset = new EsDocumentSet(this,sync);
		docSets.add(docset);
		return docset;
	}
	
	/*
	 * Return an EsCursor that can iterate over all documents in the given index that match the query string
	 */
	public EsCursor getDocuments(String index, String queryString) {
		return new EsCursor(this, index, queryString);
	}
	
	/*
	 * Return an EsCursor that can iterate over all documents in the given index and type that match the query string
	 */
	public EsCursor getDocuments(String index, String type, String queryString) {
		return new EsCursor(this, index, queryString, type);
	}
	
	/*
	 * Return an EsCursor that can iterate over all documents in the given index that match the query string.
	 * 
	 * The query string is passed in as an InputStream.
	 */
	public EsCursor getDocuments(String index, InputStream is, String type) {
		try {
			byte b[] = new byte[4000];
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			int read = is.read(b);
			while (read > -1) {
				baos.write(b,0,read);
				read = is.read(b);
			}
			return new EsCursor(this, index, baos.toString(), type);
		} catch (IOException e) {
			return null;
		}
	}
	

	public void createIndex(String indexName) throws EsServerException {		
		if (indexExists(indexName)) { return; }
		getClient().admin().indices().prepareCreate(indexName).execute().actionGet().isAcknowledged();
		reconnect();		
	}

	public void deleteIndex(String indexName) throws EsServerException {	
		if (!indexExists(indexName)) { return; }
		getClient().admin().indices().prepareDelete(indexName).execute().actionGet().isAcknowledged();
		reconnect();		
	}
	
	public String[] getIndexes() throws EsServerException {
		Map<String,IndexMetaData> mappings = getClient().admin().cluster().prepareState().execute().actionGet().getState().getMetaData().getIndices();
		Set<String> keys = mappings.keySet();
		String result[] = new String[keys.size()];
		keys.toArray(result);
		return result;
	}

	public boolean indexExists(String indexName) throws EsServerException {
		Map<String,IndexMetaData> mappings = getClient().admin().cluster().prepareState().execute().actionGet().getState().getMetaData().getIndices();
		return mappings.containsKey(indexName);
	}
	
	public void putMappingFromString(String index, String type, String mapping) throws EsServerException {
		IndicesAdminClient indicesAdminClient = getClient().admin().indices();
		PutMappingRequestBuilder pmrb = new PutMappingRequestBuilder(indicesAdminClient);
		pmrb.setIndices(index);
		pmrb.setType(type);
		pmrb.setSource(mapping);
		pmrb.execute().actionGet().isAcknowledged();
	}

	public void putMapping(String index, String type, InputStream is) throws EsServerException {

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			byte b[] = new byte[4000];
			int read = is.read(b);
			while (read > -1) {
				baos.write(b,0,read);				
				read = is.read(b);
			}
		} catch (IOException e) {
			return;
		}
		
		putMappingFromString(index, type, new String(baos.toByteArray()));

	}
	
	public String getMapping(String index, String type) throws EsException {
		IndexMetaData indexMetaData = getClient().admin().cluster().prepareState().setFilterIndices(index).execute().actionGet().getState().getMetaData().index(index);
		if (indexMetaData == null) {
			throw new EsIndexDoesNotExistException(index);
		}

		MappingMetaData mappingMetaData = indexMetaData.mapping(type);
		if (mappingMetaData == null) {
			throw new EsTypeNotDefinedException(index, type);
		}
		
		try {
			return mappingMetaData.source().string();
		} catch (IOException e) {
			throw new EsException(e);
		}

	}

	public void putDocument(String index, String type, String id, JSONObject document ) throws EsServerException {
		putDocument(index,type,id,document.toString());
	}
	
	public void putDocument(String index, String type, String id, String document ) throws EsServerException {

		boolean sync = true;
		
		IndexRequestBuilder indexRequestBuilder = getClient().prepareIndex(index,type,id).setSource(document);
		ListenableActionFuture<IndexResponse> laf = indexRequestBuilder.execute();
		if (sync) {
			laf.actionGet();
		}
		
	}
	
	public boolean typeExists(String index, String type) throws EsIndexDoesNotExistException, EsServerException {
		ClusterState cs = getClient().admin().cluster().prepareState().setFilterIndices(index).execute().actionGet().getState();
		IndexMetaData imd = cs.getMetaData().index(index);
		if (imd == null) {
			throw new EsIndexDoesNotExistException(index);
		}	
		return imd.getMappings().containsKey(type);		
	}

	public void deleteType(String index, String type) throws EsServerException {
		try {
			if (!indexExists(index)) { 
				return;
			}
			if (!typeExists(index, type)) {
				return;
			}
		} catch (EsIndexDoesNotExistException e) {
			return;
		}
		getClient().admin().indices().prepareDeleteMapping(index).setType(type).execute().actionGet();
	}
	
	protected SearchResponse performQuery(JSONObject queryObj, String index, String type) throws EsServerException {
		SearchRequestBuilder srb = getClient().prepareSearch(index);
		srb.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
		srb.setSource(queryObj.toString());
		if (type != null) {
			srb.setTypes(type);
		}
		return srb.execute().actionGet();
	}
	
	protected TransportClient getClient() throws EsServerException {
		if (_client == null) {
			try {
				connect();
				_client.admin().cluster().prepareState().execute();
			} catch (NoNodeAvailableException e) {
				throw new EsServerException();
			} catch (Throwable t) {
				System.out.println(t);
			}
		}
		return _client;
	}
	
	private void connect() {
		Builder builder = ImmutableSettings.settingsBuilder();
		builder.put("cluster.name", cluster);
		builder.put("client.transport.ping_timeout", "30s");

		TransportClient aClient = new TransportClient(builder.build());
		aClient.addTransportAddress(new InetSocketTransportAddress(host, port));	
		Iterator<InetSocketTransportAddress> iter = otherHosts.iterator();
		while (iter.hasNext()) {
			aClient.addTransportAddress(iter.next());	
		}
		
		_client = aClient;
	}

	private void reconnect() throws EsServerException {
		if (_client != null) {

			Iterator<EsDocumentSet> iter = docSets.iterator();
			while (iter.hasNext()) {
				iter.next().close();
			}

			_client.close();
			_client = null;

		}
		getClient();
	}

	public void refresh(String index) throws ElasticSearchException, EsServerException {
	
		getClient().admin().indices().refresh(new RefreshRequest(index)).actionGet();
		
	}

	
}
