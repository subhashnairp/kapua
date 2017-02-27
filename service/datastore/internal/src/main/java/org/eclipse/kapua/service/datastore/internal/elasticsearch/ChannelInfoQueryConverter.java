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

import org.eclipse.kapua.service.datastore.model.ChannelInfo;
import org.eclipse.kapua.service.datastore.model.query.StorableFetchStyle;
import org.eclipse.kapua.service.datastore.model.query.ChannelInfoQuery;

public class ChannelInfoQueryConverter extends AbstractStorableQueryConverter<ChannelInfo, ChannelInfoQuery>
{

    @Override
    protected String[] getIncludes(StorableFetchStyle fetchStyle)
    {
        return new String[] { "" };
    }

    @Override
    protected String[] getExcludes(StorableFetchStyle fetchStyle)
    {
        return new String[] { "*" };
    }

    @Override
    protected String[] getFields()
    {
        return new String[] { ChannelInfoField.CHANNEL.field(),
                              ChannelInfoField.TIMESTAMP.field(),
                              ChannelInfoField.MESSAGE_ID.field(),
                              ChannelInfoField.CLIENT_ID.field(),
                              ChannelInfoField.ACCOUNT.field() };
    }

}
