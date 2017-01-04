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
package org.eclipse.kapua.service.datastore.internal.model;

import java.util.Date;

import org.eclipse.kapua.service.datastore.model.ClientInfoCreator;
import org.eclipse.kapua.service.datastore.model.StorableId;

public class ClientInfoCreatorImpl implements ClientInfoCreator
{
	private String account;
	private String clientId;
	private StorableId lastMessageId;
	private Date lastMessageTimestamp;
	
	public ClientInfoCreatorImpl(String account) {
		this.account = account;
	}

	@Override
	public String getAccount() {
		return account;
	}
	
	@Override
	public String getClientId() {
		return clientId;
	}

	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	@Override
	public StorableId getLastMessageId() {
		return lastMessageId;
	}

	public void setLastMessageId(StorableId lastMessageId) {
		this.lastMessageId = lastMessageId;
	}

	@Override
	public Date getLastMessageTimestamp() {
		return lastMessageTimestamp;
	}

	@Override
	public void setLastMessageTimestamp(Date lastMessageTimestamp) {
		this.lastMessageTimestamp = lastMessageTimestamp;
	}
}
