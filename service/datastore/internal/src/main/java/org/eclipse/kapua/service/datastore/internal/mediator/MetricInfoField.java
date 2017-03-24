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
package org.eclipse.kapua.service.datastore.internal.mediator;

import org.eclipse.kapua.service.datastore.internal.schema.MetricInfoSchema;
import org.eclipse.kapua.service.datastore.model.MetricInfo;
import org.eclipse.kapua.service.datastore.model.MetricInfoCreator;
import org.eclipse.kapua.service.datastore.model.StorableId;
import org.eclipse.kapua.service.datastore.model.query.StorableField;

/**
 * This enumeration defines the fields names used in the {@link MetricInfo} Elasticsearch schema
 * 
 * @since 1.0
 *
 */
public enum MetricInfoField implements StorableField {
    /**
     * Account name
     */
    ACCOUNT(MetricInfoSchema.METRIC_ACCOUNT),
    /**
     * Client identifier
     */
    CLIENT_ID(MetricInfoSchema.METRIC_CLIENT_ID),
    /**
     * Channel
     */
    CHANNEL(MetricInfoSchema.METRIC_CHANNEL),
    /**
     * Full metric name (so with the metric type suffix)
     */
    NAME_FULL(MetricInfoSchema.METRIC_MTR_NAME_FULL),
    /**
     * Metric type full name (not the acronym)
     */
    TYPE_FULL(MetricInfoSchema.METRIC_MTR_TYPE_FULL),
    /**
     * Metric value
     */
    VALUE_FULL(MetricInfoSchema.METRIC_MTR_VALUE_FULL),
    /**
     * Metric timestamp (derived from the message that published the metric)
     */
    TIMESTAMP_FULL(MetricInfoSchema.METRIC_MTR_TIMESTAMP_FULL),
    /**
     * Message identifier
     */
    MESSAGE_ID_FULL(MetricInfoSchema.METRIC_MTR_MSG_ID_FULL);

    private String field;

    private MetricInfoField(String name) {
        this.field = name;
    }

    @Override
    public String field() {
        return field;
    }

    /**
     * Get the metric identifier (return the hash code of the string obtained by combining accountName, clientId, channel and the converted metricName and metricType).<br>
     * <b>If the id is null then it is generated.</b>
     * 
     * @param id
     * @param accountName
     * @param clientId
     * @param channel
     * @param metricName
     * @param metricType
     * @return
     */
    private static String getOrDeriveId(StorableId id, String accountName, String clientId, String channel, String metricName, String metricType) {
        if (id == null) {
            String metricMappedName = DatastoreUtils.getMetricValueQualifier(metricName, DatastoreUtils.convertToClientType(metricType));
            return DatastoreUtils.getHashCode(accountName, clientId, channel, metricMappedName);
        } else
            return id.toString();
    }

    /**
     * Get the metric identifier getting parameters from the metricInfoCreator. Then it calls {@link getOrDeriveId(StorableId id, String accountName, String clientId, String channel, String
     * metricName, String metricType)}
     * 
     * @param id
     * @param metricInfoCreator
     * @return
     */
    public static String getOrDeriveId(StorableId id, MetricInfoCreator metricInfoCreator) {
        return getOrDeriveId(id,
                metricInfoCreator.getAccount(),
                metricInfoCreator.getClientId(),
                metricInfoCreator.getChannel(),
                metricInfoCreator.getName(),
                metricInfoCreator.getType());
    }

    /**
     * Get the metric identifier getting parameters from the metricInfo. Then it calls {@link getOrDeriveId(StorableId id, String accountName, String clientId, String channel, String
     * metricName, String metricType)}
     * 
     * @param id
     * @param metricInfo
     * @return
     */
    public static String getOrDeriveId(StorableId id, MetricInfo metricInfo) {
        return getOrDeriveId(id,
                metricInfo.getAccount(),
                metricInfo.getClientId(),
                metricInfo.getChannel(),
                metricInfo.getName(),
                metricInfo.getType());
    }

}
