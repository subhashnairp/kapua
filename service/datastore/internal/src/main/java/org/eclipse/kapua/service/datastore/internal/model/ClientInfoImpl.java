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

import org.eclipse.kapua.service.datastore.model.ClientInfo;
import org.eclipse.kapua.service.datastore.model.StorableId;

public class ClientInfoImpl implements ClientInfo
{
    private StorableId id;
    private String account;
    private String clientId;
    private Date lastMessageTimestamp;
    private StorableId lastMessageId;
    
    public ClientInfoImpl(String account)
    {
        this.account = account;
    }
   
    public ClientInfoImpl(String account, StorableId id)
    {
        this(account);
        this.id = id;
    }

    @Override
    public String getAccount()
    {
        return account;
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
    public String getClientId()
    {
        return clientId;
    }

    public void setClientId(String clientId)
    {
        this.clientId = clientId;
    }

    @Override
    public StorableId getLastMessageId()
    {
        return lastMessageId;
    }

    public void setLastMessageId(StorableId lastMessageId)
    {
        this.lastMessageId = lastMessageId;
    }

    @Override
    public Date getLastMessageTimestamp()
    {
        return lastMessageTimestamp;
    }

    public void setLastMessageTimestamp(Date lastMessageTimestamp)
    {
        this.lastMessageTimestamp = lastMessageTimestamp;
    }
}
