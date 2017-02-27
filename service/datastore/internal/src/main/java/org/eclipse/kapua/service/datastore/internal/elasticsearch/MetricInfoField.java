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

import org.eclipse.kapua.service.datastore.model.query.StorableField;

public enum MetricInfoField implements StorableField
{
    ACCOUNT(EsSchema.METRIC_ACCOUNT),
    CLIENT_ID(EsSchema.METRIC_CLIENT_ID),
    CHANNEL(EsSchema.METRIC_CHANNEL),
    NAME_FULL(EsSchema.METRIC_MTR_NAME_FULL),
    TYPE_FULL(EsSchema.METRIC_MTR_TYPE_FULL),
    VALUE_FULL(EsSchema.METRIC_MTR_VALUE_FULL),
    TIMESTAMP_FULL(EsSchema.METRIC_MTR_TIMESTAMP_FULL),
    MESSAGE_ID_FULL(EsSchema.METRIC_MTR_MSG_ID_FULL);

    private String field;

    private MetricInfoField(String name)
    {
        this.field = name;
    }

    @Override
    public String field()
    {
        return field;
    }
}
