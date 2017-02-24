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
package org.eclipse.kapua.service.datastore.model;

import java.util.Date;

public interface ClientInfo extends Storable
{
    public StorableId getId();
    
    public String getAccount();
    
    public String getClientId();
    
    public void setClientId(String clientId);
    
    public StorableId getMessageId();

    public void setMessageId(StorableId messageId);

    public Date getMessageTimestamp();

    public void setMessageTimestamp(Date messageTimestamp);

    /**
     * Transient data field (the last publish timestamp should get from the message table by the find service)
     * 
     * @return
     */
    public Date getLastMessageTimestamp();

    public void setLastMessageTimestamp(Date lastMessageTimestamp);
}
