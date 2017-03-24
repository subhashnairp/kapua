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
package org.eclipse.kapua.service.datastore.internal;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.kapua.KapuaIllegalArgumentException;
import org.eclipse.kapua.commons.util.ArgumentValidator;
import org.eclipse.kapua.model.id.KapuaId;
import org.eclipse.kapua.service.datastore.client.BulkUpdateRequest;
import org.eclipse.kapua.service.datastore.client.BulkUpdateResponse;
import org.eclipse.kapua.service.datastore.client.ClientException;
import org.eclipse.kapua.service.datastore.client.ClientUnavailableException;
import org.eclipse.kapua.service.datastore.client.Client;
import org.eclipse.kapua.service.datastore.client.QueryMappingException;
import org.eclipse.kapua.service.datastore.client.TypeDescriptor;
import org.eclipse.kapua.service.datastore.client.UpdateRequest;
import org.eclipse.kapua.service.datastore.client.UpdateResponse;
import org.eclipse.kapua.service.datastore.internal.client.ClientFactory;
import org.eclipse.kapua.service.datastore.internal.mediator.ConfigurationException;
import org.eclipse.kapua.service.datastore.internal.mediator.MessageStoreConfiguration;
import org.eclipse.kapua.service.datastore.internal.mediator.MetricInfoField;
import org.eclipse.kapua.service.datastore.internal.mediator.MetricInfoRegistryMediator;
import org.eclipse.kapua.service.datastore.internal.model.MetricInfoListResultImpl;
import org.eclipse.kapua.service.datastore.internal.model.StorableIdImpl;
import org.eclipse.kapua.service.datastore.internal.model.query.AndPredicateImpl;
import org.eclipse.kapua.service.datastore.internal.model.query.IdsPredicateImpl;
import org.eclipse.kapua.service.datastore.internal.model.query.MetricInfoQueryImpl;
import org.eclipse.kapua.service.datastore.internal.schema.MetricInfoSchema;
import org.eclipse.kapua.service.datastore.internal.schema.Metadata;
import org.eclipse.kapua.service.datastore.internal.schema.SchemaUtil;
import org.eclipse.kapua.service.datastore.model.MetricInfo;
import org.eclipse.kapua.service.datastore.model.MetricInfoListResult;
import org.eclipse.kapua.service.datastore.model.StorableId;
import org.eclipse.kapua.service.datastore.model.query.MetricInfoQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Metric information registry facade
 * 
 * @since 1.0
 *
 */
public class MetricInfoRegistryFacade {

    private static final Logger logger = LoggerFactory.getLogger(MetricInfoRegistryFacade.class);

    private final MetricInfoRegistryMediator mediator;
    private final ConfigurationProvider configProvider;
    private Client client = null;

    /**
     * Constructs the metric info registry facade
     * 
     * @param configProvider
     * @param mediator
     * @throws ClientUnavailableException
     */
    public MetricInfoRegistryFacade(ConfigurationProvider configProvider, MetricInfoRegistryMediator mediator) throws ClientUnavailableException {
        this.configProvider = configProvider;
        this.mediator = mediator;
        client = ClientFactory.getInstance();
    }

    /**
     * Update the metric information after a message store operation (for a single metric)
     * 
     * @param scopeId
     * @param metricInfo
     * @return
     * @throws KapuaIllegalArgumentException
     * @throws ConfigurationException
     * @throws ClientException
     */
    public StorableId upstore(KapuaId scopeId, MetricInfo metricInfo)
            throws KapuaIllegalArgumentException,
            ConfigurationException, ClientException {
        ArgumentValidator.notNull(scopeId, "scopeId");
        ArgumentValidator.notNull(metricInfo, "metricInfoCreator");
        ArgumentValidator.notNull(metricInfo.getFirstPublishedMessageId(), "metricInfoCreator.firstPublishedMessageId");
        ArgumentValidator.notNull(metricInfo.getFirstPublishedMessageTimestamp(), "metricInfoCreator.firstPublishedMessageTimestamp");

        String metricInfoId = MetricInfoField.getOrDeriveId(metricInfo.getId(), metricInfo);

        // Store channel. Look up channel in the cache, and cache it if it doesn't exist
        if (!DatastoreCacheManager.getInstance().getMetricsCache().get(metricInfoId)) {
            if (!DatastoreCacheManager.getInstance().getChannelsCache().get(metricInfoId)) {
                Metadata metadata = mediator.getMetadata(scopeId, metricInfo.getFirstPublishedMessageTimestamp().getTime());
                String kapuaIndexName = metadata.getRegistryIndexName();

                UpdateRequest request = new UpdateRequest(new TypeDescriptor(metadata.getRegistryIndexName(), MetricInfoSchema.METRIC_TYPE_NAME), metricInfo.getId().toString(), metricInfo);
                client.upsert(request);

                logger.debug(String.format("Upsert on metric succesfully executed [%s.%s, %s]",
                        kapuaIndexName, MetricInfoSchema.METRIC_TYPE_NAME, metricInfoId));
                // Update cache if channel update is completed successfully
                DatastoreCacheManager.getInstance().getChannelsCache().put(metricInfoId, true);
            }
        }
        return new StorableIdImpl(metricInfoId);
    }

    /**
     * Update the metrics informations after a message store operation (for few metrics)
     * 
     * @param scopeId
     * @param metricInfos
     * @return
     * @throws KapuaIllegalArgumentException
     * @throws ConfigurationException
     * @throws ClientException
     */
    public BulkUpdateResponse upstore(KapuaId scopeId, MetricInfo[] metricInfos)
            throws KapuaIllegalArgumentException,
            ConfigurationException,
            ClientException {
        ArgumentValidator.notNull(scopeId, "scopeId");
        ArgumentValidator.notNull(metricInfos, "metricInfoCreator");

        BulkUpdateRequest bulkRequest = new BulkUpdateRequest();
        // Create a bulk request
        for (MetricInfo metricInfo : metricInfos) {
            // set id????? //TODO
            String metricInfoId = MetricInfoField.getOrDeriveId(metricInfo.getId(), metricInfo);

            if (DatastoreCacheManager.getInstance().getMetricsCache().get(metricInfoId))
                continue;

            // TODO
            // gli sto chiedendo il nome dell'indice data e' account-week mentre registry .account (invarianti nel tempo)
            Metadata metadata = mediator.getMetadata(scopeId, metricInfo.getFirstPublishedMessageTimestamp().getTime());

            bulkRequest.add(
                    new UpdateRequest(new TypeDescriptor(metadata.getRegistryIndexName(), MetricInfoSchema.METRIC_TYPE_NAME), metricInfo.getId().toString(), metricInfo));
        }

        // execute the upstore
        BulkUpdateResponse upsertResponse = null;
        try {
            upsertResponse = client.upsert(bulkRequest);
        } catch (ClientException e) {
            logger.trace(String.format("Upsert failed [%s]", e.getMessage()));
            throw e;
        }

        if (upsertResponse != null) {
            if (upsertResponse.getResponse().size() <= 0) {
                return upsertResponse;
            }
            for (UpdateResponse response : upsertResponse.getResponse()) {
                String index = response.getTypeDescriptor().getIndex();
                String type = response.getTypeDescriptor().getType();
                String id = response.getId();
                logger.debug(String.format("Upsert on channel metric succesfully executed [%s.%s, %s]",
                        index, type, id));

                if (DatastoreCacheManager.getInstance().getMetricsCache().get(id))
                    continue;

                // Update cache if channel metric update is completed
                // successfully
                DatastoreCacheManager.getInstance().getMetricsCache().put(id, true);
            }
        }
        return upsertResponse;
    }

    /**
     * Delete metric information by identifier
     * 
     * @param scopeId
     * @param id
     * @throws KapuaIllegalArgumentException
     * @throws ConfigurationException
     * @throws ClientException
     */
    public void delete(KapuaId scopeId, StorableId id)
            throws KapuaIllegalArgumentException,
            ConfigurationException,
            ClientException {
        ArgumentValidator.notNull(scopeId, "scopeId");
        ArgumentValidator.notNull(id, "id");

        // Do the find
        MessageStoreConfiguration accountServicePlan = configProvider.getConfiguration(scopeId);
        long ttl = accountServicePlan.getDataTimeToLiveMilliseconds();

        if (!accountServicePlan.getDataStorageEnabled() || ttl == MessageStoreConfiguration.DISABLED) {
            logger.debug("Storage not enabled for account {}, return", scopeId);
            return;
        }

        String indexName = SchemaUtil.getKapuaIndexName(scopeId);
        TypeDescriptor typeDescriptor = new TypeDescriptor(indexName, MetricInfoSchema.METRIC_TYPE_NAME);
        client.delete(typeDescriptor, id.toString());
    }

    /**
     * Find metric information by identifier
     * 
     * @param scopeId
     * @param id
     * @return
     * @throws KapuaIllegalArgumentException
     * @throws ConfigurationException
     * @throws QueryMappingException
     * @throws ClientException
     */
    public MetricInfo find(KapuaId scopeId, StorableId id)
            throws KapuaIllegalArgumentException,
            ConfigurationException,
            QueryMappingException,
            ClientException {
        ArgumentValidator.notNull(scopeId, "scopeId");
        ArgumentValidator.notNull(id, "id");

        MetricInfoQueryImpl idsQuery = new MetricInfoQueryImpl();
        idsQuery.setLimit(1);

        ArrayList<StorableId> ids = new ArrayList<StorableId>();
        ids.add(id);

        AndPredicateImpl allPredicates = new AndPredicateImpl();
        allPredicates.addPredicate(new IdsPredicateImpl(MetricInfoSchema.METRIC_TYPE_NAME, ids));

        String indexName = SchemaUtil.getKapuaIndexName(scopeId);
        TypeDescriptor typeDescriptor = new TypeDescriptor(indexName, MetricInfoSchema.METRIC_TYPE_NAME);
        return client.find(typeDescriptor, idsQuery, MetricInfo.class);
    }

    /**
     * Find metrics informations matching the given query
     * 
     * @param scopeId
     * @param query
     * @return
     * @throws KapuaIllegalArgumentException
     * @throws ConfigurationException
     * @throws QueryMappingException
     * @throws ClientException
     */
    public MetricInfoListResult query(KapuaId scopeId, MetricInfoQuery query)
            throws KapuaIllegalArgumentException,
            ConfigurationException,
            QueryMappingException,
            ClientException {
        // Argument Validation
        ArgumentValidator.notNull(scopeId, "scopeId");
        ArgumentValidator.notNull(query, "query");

        // Do the find
        MessageStoreConfiguration accountServicePlan = configProvider.getConfiguration(scopeId);
        long ttl = accountServicePlan.getDataTimeToLiveMilliseconds();

        if (!accountServicePlan.getDataStorageEnabled() || ttl == MessageStoreConfiguration.DISABLED) {
            logger.debug("Storage not enabled for account {}, returning empty result", scopeId);
            return new MetricInfoListResultImpl();
        }

        String indexNme = SchemaUtil.getKapuaIndexName(scopeId);
        MetricInfoListResult listResult = new MetricInfoListResultImpl();
        TypeDescriptor typeDescriptor = new TypeDescriptor(indexNme, MetricInfoSchema.METRIC_TYPE_NAME);
        List<MetricInfo> result = client.query(typeDescriptor, query, MetricInfo.class);

        listResult.addAll(result);
        return listResult;
    }

    /**
     * Get metrics informations count matching the given query
     * 
     * @param scopeId
     * @param query
     * @return
     * @throws KapuaIllegalArgumentException
     * @throws ConfigurationException
     * @throws QueryMappingException
     * @throws ClientException
     */
    public long count(KapuaId scopeId, MetricInfoQuery query)
            throws KapuaIllegalArgumentException,
            ConfigurationException,
            QueryMappingException,
            ClientException {
        ArgumentValidator.notNull(scopeId, "scopeId");
        ArgumentValidator.notNull(query, "query");

        // Do the find
        MessageStoreConfiguration accountServicePlan = configProvider.getConfiguration(scopeId);
        long ttl = accountServicePlan.getDataTimeToLiveMilliseconds();

        if (!accountServicePlan.getDataStorageEnabled() || ttl == MessageStoreConfiguration.DISABLED) {
            logger.debug("Storage not enabled for account {}, returning empty result", scopeId);
            return 0;
        }

        String indexName = SchemaUtil.getKapuaIndexName(scopeId);
        TypeDescriptor typeDescriptor = new TypeDescriptor(indexName, MetricInfoSchema.METRIC_TYPE_NAME);
        return client.count(typeDescriptor, query);
    }

    /**
     * Delete metrics informations count matching the given query
     * 
     * @param scopeId
     * @param query
     * @throws KapuaIllegalArgumentException
     * @throws ConfigurationException
     * @throws QueryMappingException
     * @throws ClientException
     */
    public void delete(KapuaId scopeId, MetricInfoQuery query)
            throws KapuaIllegalArgumentException,
            ConfigurationException,
            QueryMappingException,
            ClientException {
        ArgumentValidator.notNull(scopeId, "scopeId");
        ArgumentValidator.notNull(query, "query");

        // Do the find
        MessageStoreConfiguration accountServicePlan = configProvider.getConfiguration(scopeId);
        long ttl = accountServicePlan.getDataTimeToLiveMilliseconds();

        if (!accountServicePlan.getDataStorageEnabled() || ttl == MessageStoreConfiguration.DISABLED) {
            logger.debug("Storage not enabled for account {}, returning empty result", scopeId);
        }

        String indexName = SchemaUtil.getKapuaIndexName(scopeId);
        TypeDescriptor typeDescriptor = new TypeDescriptor(indexName, MetricInfoSchema.METRIC_TYPE_NAME);
        client.deleteByQuery(typeDescriptor, query);
    }
}
