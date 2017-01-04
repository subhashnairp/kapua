/*******************************************************************************
 * Copyright (c) 2011, 2017 Eurotech and/or its affiliates and others
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *  
 * Contributors:
 *     Eurotech - initial API and implementation
 *******************************************************************************/
package org.eclipse.kapua.service.datastore.internal.elasticsearch;

import java.net.UnknownHostException;
import java.util.Map;

import org.eclipse.kapua.service.datastore.internal.setting.DatastoreSettingKey;
import org.eclipse.kapua.service.datastore.internal.setting.DatastoreSettings;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ElasticsearchClient {
	
	@SuppressWarnings("unused")
	private static final Logger   s_logger = LoggerFactory.getLogger(ElasticsearchClient.class);
	
	private static final int DEFAULT_PORT = 9300; 
	
	private static Client client;

	public synchronized static Client getInstance() 
			throws EsClientUnavailableException 
	{
		if (client == null) 
		{
	        DatastoreSettings config = DatastoreSettings.getInstance();
	        Map<String, String> map = config.getMap(String.class, DatastoreSettingKey.ELASTICSEARCH_NODES, "[0-9]+");
	        String[] esNodes = new String[] {};
	        if (map != null)
	            esNodes = map.values().toArray(new String[] {});
	        
	        if (esNodes == null || esNodes.length == 0)
	        	throw new EsClientUnavailableException("No elasticsearch nodes found");
	        
	        String[] nodeParts = getNodeParts(esNodes[0]);
	        String esHost = null;
	        int esPort = DEFAULT_PORT;
	        
	        if (nodeParts.length > 0)
	            esHost = nodeParts[0];
	        
	        if (nodeParts.length > 1) {
	        	try {
	        		Integer.parseInt(nodeParts[1]);
	        	} catch (NumberFormatException e) {
	        		throw new EsClientUnavailableException("Could not parse elasticsearch port: " + nodeParts[1]);
	        	}
	        }
	        
	        Client theClient = null;
			try {
				theClient = EsUtils.getEsClient(esHost, esPort, config.getString(DatastoreSettingKey.ELASTICSEARCH_CLUSTER));
			} catch (UnknownHostException e) {
				throw new EsClientUnavailableException("Unknown elasticsearch node host", e);
			}
			
			client = theClient;
		}
		return client;
	}
	
	private static String[] getNodeParts(String node)
	{
	    if (node==null)
	        return new String[] {};
	    
	    String[] split = node.split(":");
	    return split;
	}
}
