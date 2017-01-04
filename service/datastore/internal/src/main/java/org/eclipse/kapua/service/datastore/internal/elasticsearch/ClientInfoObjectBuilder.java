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

import java.util.Date;
import java.util.Map;

import org.eclipse.kapua.service.datastore.internal.model.ClientInfoImpl;
import org.eclipse.kapua.service.datastore.internal.model.StorableIdImpl;
import org.eclipse.kapua.service.datastore.model.ClientInfo;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;

public class ClientInfoObjectBuilder {

	private ClientInfoImpl clientInfo;
	
	public ClientInfoObjectBuilder build(SearchHit searchHit) 
			throws EsObjectBuilderException 
	{	
		String idStr = searchHit.getId();

		Map<String, SearchHitField> fields = searchHit.getFields();
		String clientId = fields.get(ClientInfoField.CLIENT_ID.field()).getValue();
		String timestampStr = fields.get(ClientInfoField.TIMESTAMP.field()).getValue();
		String account = fields.get(ClientInfoField.ACCOUNT.field()).getValue();
		String messageId = fields.get(ClientInfoField.MESSAGE_ID.field()).getValue();
		
		this.clientInfo = new ClientInfoImpl(account, new StorableIdImpl(idStr));
		this.clientInfo.setClientId(clientId);
        this.clientInfo.setLastMessageId(new StorableIdImpl(messageId));

    	Date timestamp = (Date) EsUtils.convertToKapuaObject("date", timestampStr);
		this.clientInfo.setLastMessageTimestamp(timestamp);

        return this;
	}
	
	public ClientInfo getClientInfo() {
		return this.clientInfo;
	}
}
