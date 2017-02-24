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

import org.eclipse.kapua.service.datastore.model.StorableId;
import org.eclipse.kapua.service.datastore.model.ChannelInfo;

public class ChannelInfoImpl implements ChannelInfo
{
    private StorableId id;
    private String account;
    private String clientId;
    private StorableId msgId;
    private Date       msgTimestamp;
    private String channel;
    private Date       lastMsgTimestamp;

    public ChannelInfoImpl(String scope)
    {
        this.account= scope;
    }

    public ChannelInfoImpl(String scope, StorableId id)
    {
        this(scope);
        this.id = id;
    }
    
    @Override
    public StorableId getId()
    {
        return id;
    }

    public void setId(StorableId id)
    {
        this.id = id;
    }

    @Override
    public String getAccount()
    {
        return account;
    }

    @Override
    public String getClientId()
    {
    	return clientId;
    }
    
    public void setClientId(String clientId)
    {
    	this.clientId = clientId;
    }
    
    @Override
    public StorableId getMessageId()
    {
        return this.msgId;
    }

    public void setMessageId(StorableId msgId)
    {
        this.msgId = msgId;
    }

    @Override
    public Date getMessageTimestamp()
    {
        return msgTimestamp;
    }

    public void setMessageTimestamp(Date msgTimestamp)
    {
        this.msgTimestamp = msgTimestamp;
    }

    @Override
    public String getChannel()
    {
        return channel;
    }
    
    public void setChannel(String channel)
    {
        this.channel = channel;
    }

    @Override
    public Date getLastMessageTimestamp()
    {
        return lastMsgTimestamp;
    }

    public void setLastMessageTimestamp(Date lastMsgTimestamp)
    {
        this.lastMsgTimestamp = lastMsgTimestamp;
    }

}
