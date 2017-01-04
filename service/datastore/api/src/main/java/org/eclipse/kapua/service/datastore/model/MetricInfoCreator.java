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

public interface MetricInfoCreator extends StorableCreator<MetricInfo>
{
    public String getAccount();

    public String getClientId();

    public String getChannel();

    public void setChannel(String channel);

    public String getName();

    public void setName(String name);

    public String getType();

    public void setType(String type);

    public <T> T getValue(Class<T> clazz);

    public <T> void setValue(T value);

    public StorableId getLastMessageId();

    public void setLastMessageId(StorableId lastMessageId);

    public Date getLastMessageTimestamp();

    public void setLastMessageTimestamp(Date lastMessageTimestamp);
}
