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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Date;

import org.eclipse.kapua.service.datastore.model.ClientInfo;
import org.eclipse.kapua.service.datastore.model.ClientInfoCreator;
import org.eclipse.kapua.service.datastore.model.StorableId;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.hash.Hashing;

public class ClientInfoXContentBuilder
{

    @SuppressWarnings("unused")
    private static final Logger   s_logger = LoggerFactory.getLogger(ClientInfoXContentBuilder.class);
    
    private String                clientId;
    private XContentBuilder       clientBuilder;

    private void init()
    {
        clientId = null;
        clientBuilder = null;
    }

    private static String getHashCode(String aString)
    {
        byte[] hashCode = Hashing.sha256()
                                 .hashString(aString, StandardCharsets.UTF_8)
                                 .asBytes();

        return Base64.encodeBytes(hashCode);
        // return aString;
    }

    private void setClientBuilder(XContentBuilder esClient)
    {
        this.clientBuilder = esClient;
    }
    
    /*
     * If the id is null then it is generated
     */
    public static String getOrDeriveId(StorableId id, String accountName, String clientId)
    {      
		if (id == null)
			return getClientKey(accountName, clientId);
		else
			return id.toString();
    }

    private static String getClientKey(String accountName, String clientName)
    {
        String clientFullName = String.format("%s/%s", accountName, clientName);
        String clientHashCode = getHashCode(clientFullName);
        return clientHashCode;
    }

    private XContentBuilder getClientBuilder(String clientId, String msgId, Date msgTimestamp, String account)
        throws EsDocumentBuilderException
    {
    	try {
	        XContentBuilder builder = XContentFactory.jsonBuilder()
             .startObject()
                 .field(EsSchema.CLIENT_ID, clientId)
                 .field(EsSchema.CLIENT_MESSAGE_ID, msgId)
                 .field(EsSchema.CLIENT_TIMESTAMP, msgTimestamp)
                 .field(EsSchema.CLIENT_ACCOUNT, account)
             .endObject();
	        return builder;
    	} catch (IOException e) {
	    	throw new EsDocumentBuilderException(String.format("Unable to build client info document"), e);
    	}
    }

    public ClientInfoXContentBuilder clear()
    {
        this.init();
        return this;
    }

    public ClientInfoXContentBuilder build(ClientInfoCreator clientInfo) 
    		throws EsDocumentBuilderException
    {
    	String scopeName = clientInfo.getAccount();
        String clientId = clientInfo.getClientId();
        StorableId msgId = clientInfo.getLastMessageId();
        Date msgTimestamp = clientInfo.getLastMessageTimestamp();
         
        XContentBuilder clientBuilder;
		clientBuilder = this.getClientBuilder(clientId, msgId.toString(), msgTimestamp, scopeName);
        
        this.setClientId(getClientKey(scopeName, clientId));
        this.setClientBuilder(clientBuilder);
        
        return this;
    }

    public ClientInfoXContentBuilder build(ClientInfo clientInfo) 
    		throws EsDocumentBuilderException
    {
    	String accountName = clientInfo.getAccount();
        String clientId = clientInfo.getClientId();
        StorableId msgId = clientInfo.getMessageId();
        Date msgTimestamp = clientInfo.getMessageTimestamp();
     
		clientBuilder = this.getClientBuilder(clientId, msgId.toString(), msgTimestamp, accountName);
        
        this.setClientId(getOrDeriveId(clientInfo.getId(), accountName, clientId));
        this.setClientBuilder(clientBuilder);
        
        return this;
    }

    public String getClientId()
    {
        return clientId;
    }

    private void setClientId(String esClientId)
    {
        this.clientId = esClientId;
    }

    public XContentBuilder getClientBuilder()
    {
        return clientBuilder;
    }
}
