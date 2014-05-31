package com.gerkenip.elasticsearch;

import java.util.Iterator;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import com.gerkenip.elasticsearch.exception.EsServerException;

public class EsCursor implements Iterator<String> {

	private EsClient	client;
	private JSONObject	queryObj;
	private String		query;
	private String		index;
	private String 		type = null;
	
	private SearchHit	searchHit[] = new SearchHit[0];
	private int			nextResult = 0;
	private boolean		endOfResults = false;
	private	int			pageStart = 0;
	public  int			pageSize = 2000;
	private int         totalHits;
	
	private JSONObject facets = null;
	
	protected EsCursor(EsClient client, String index, String query) {
		this.client = client;
		this.query  = query;
		this.index  = index;
		this.pageSize = 2000;
	}
	
	protected EsCursor(EsClient client, String index, String query, String type) {
		this.client = client;
		this.query  = query;
		this.index  = index;
		this.pageSize = 2000;
		this.type 	= type;
	}
	
	protected EsCursor(EsClient client, String index, String query, int pageSize) {
		this.client = client;
		this.query  = query;
		this.index  = index;
		this.pageSize = pageSize;
	}

	@Override
	public boolean hasNext() {
		return moreToReturn();
	}

	@Override
	public String next() {

		if (!moreToReturn()) {
			return null; 
		}
		
		return nextSearchHit().sourceAsString();

	}
	
	public JSONObject nextJsonObject() throws JSONException {

		if (!moreToReturn()) {
			return null; 
		}

		return (JSONObject) (new JSONObject(next()));
		
	}
	
	public SearchHit nextSearchHit() {

		if (!moreToReturn()) {
			return null; 
		}
		
		SearchHit hit = searchHit[nextResult];
		nextResult++;
		return hit;
		
	}

	@Override
	public void remove() {
		
	}
	
	private boolean moreToReturn() {
		
		if (endOfResults && (nextResult >= searchHit.length)) {return false; }
		
		if (nextResult >= searchHit.length) {
			try { 
				queryPage(); 
			} catch (Exception e) { 
				return false; 
			}
		}
		
		return (!endOfResults || (nextResult < searchHit.length));
		
	}
	
	public int getTotalHits() {
		hasNext();
		return totalHits;
	}

	private void queryPage() throws JSONException, EsServerException {
		
		if (queryObj == null) {
			queryObj = new JSONObject(query);
		}

		nextResult = 0;
		
		queryObj.put("from", pageStart);
		queryObj.put("size", pageSize);
		queryObj.put("version", true);
		
		SearchResponse sr = client.performQuery(queryObj, index, type);
		
		JSONObject response = new JSONObject(sr.toString());
		SearchHits searchHits = sr.getHits();
		totalHits = response.getJSONObject("hits").getInt("total");
		searchHit = searchHits.getHits();
		try {
			facets = response.getJSONObject("facets");
		} catch (Exception e) {
			facets = null;
		}
		
		pageStart = pageStart + searchHit.length;
		
		if (searchHit.length == 0) {
			endOfResults = true;
		}

	}

	public JSONObject facet(String name) throws JSONException {
	
		if (facets == null) {
			try {
				queryPage();
			} catch (EsServerException e) {
				e.printStackTrace();
				return new JSONObject();
			}
		}
		
		if (facets == null) {
			return new JSONObject();
		}
		
		return facets.getJSONObject(name);
		
	}

}
