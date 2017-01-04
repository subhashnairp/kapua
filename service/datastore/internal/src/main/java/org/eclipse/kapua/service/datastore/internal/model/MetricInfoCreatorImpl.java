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

import org.eclipse.kapua.service.datastore.model.MetricInfoCreator;
import org.eclipse.kapua.service.datastore.model.StorableId;

public class MetricInfoCreatorImpl implements MetricInfoCreator
{
	private String account;
	private String clientId;
	private String channel;
	private StorableId lastMessageId;
	private Date lastMessageTimestamp;
	private String name;
	private String type;
	private Object value;

	public MetricInfoCreatorImpl(String account) {
		this.account = account;
	}
	
	@Override
	public String getAccount() {
		return account;
	}
	
	@Override
	public String getClientId()
	{
		return this.clientId;
	}
	
	public void setDevice(String clientId)
	{
		this.clientId = clientId;
	}

	@Override
	public String getChannel() {
		return channel;
	}
	
	@Override
	public void setChannel(String channel) {
		this.channel = channel;
	}

	@Override
	public StorableId getLastMessageId() {
		return lastMessageId;
	}

	@Override
	public void setLastMessageId(StorableId lastMessageId) {
		this.lastMessageId = lastMessageId;
	}

	@Override
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public String getType() {
		return type;
	}

	@Override
	public void setType(String type) {
		this.type = type;
	}

	@Override
	public <T> T getValue(Class<T> clazz) {
		return clazz.cast(value);
	}

	@Override
	public <T> void setValue(T value) {
		this.value = value;
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
