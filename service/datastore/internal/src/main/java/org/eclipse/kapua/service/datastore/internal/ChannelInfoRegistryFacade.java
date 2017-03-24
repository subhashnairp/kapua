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
import org.eclipse.kapua.service.datastore.client.ClientException;
import org.eclipse.kapua.service.datastore.client.ClientUnavailableException;
import org.eclipse.kapua.service.datastore.client.Client;
import org.eclipse.kapua.service.datastore.client.QueryMappingException;
import org.eclipse.kapua.service.datastore.client.TypeDescriptor;
import org.eclipse.kapua.service.datastore.client.UpdateRequest;
import org.eclipse.kapua.service.datastore.client.UpdateResponse;
import org.eclipse.kapua.service.datastore.internal.client.ClientFactory;
import org.eclipse.kapua.service.datastore.internal.mediator.ChannelInfoField;
import org.eclipse.kapua.service.datastore.internal.mediator.ChannelInfoRegistryMediator;
import org.eclipse.kapua.service.datastore.internal.mediator.ConfigurationException;
import org.eclipse.kapua.service.datastore.internal.mediator.MessageStoreConfiguration;
import org.eclipse.kapua.service.datastore.internal.model.ChannelInfoListResultImpl;
import org.eclipse.kapua.service.datastore.internal.model.StorableIdImpl;
import org.eclipse.kapua.service.datastore.internal.model.query.AndPredicateImpl;
import org.eclipse.kapua.service.datastore.internal.model.query.ChannelInfoQueryImpl;
import org.eclipse.kapua.service.datastore.internal.model.query.IdsPredicateImpl;
import org.eclipse.kapua.service.datastore.internal.schema.ChannelInfoSchema;
import org.eclipse.kapua.service.datastore.internal.schema.Metadata;
import org.eclipse.kapua.service.datastore.internal.schema.SchemaUtil;
import org.eclipse.kapua.service.datastore.model.ChannelInfo;
import org.eclipse.kapua.service.datastore.model.ChannelInfoListResult;
import org.eclipse.kapua.service.datastore.model.StorableId;
import org.eclipse.kapua.service.datastore.model.query.ChannelInfoQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Channel information registry facade
 * 
 * @since 1.0
 *
 */
public class ChannelInfoRegistryFacade {

    private static final Logger logger = LoggerFactory.getLogger(ChannelInfoRegistryFacade.class);

    private final ChannelInfoRegistryMediator mediator;
    private final ConfigurationProvider configProvider;
    private final Object metadataUpdateSync;
    private Client client;

    /**
     * Constructs the channel info registry facade
     * 
     * @param configProvider
     * @param mediator
     * @throws ClientUnavailableException
     */
    public ChannelInfoRegistryFacade(ConfigurationProvider configProvider, ChannelInfoRegistryMediator mediator) throws ClientUnavailableException {
        this.configProvider = configProvider;
        this.mediator = mediator;
        this.metadataUpdateSync = new Object();
        client = ClientFactory.getInstance();
    }

    /**
     * Update the channel information after a message store operation
     * 
     * @param scopeId
     * @param channelInfo
     * @return
     * @throws KapuaIllegalArgumentException
     * @throws ConfigurationException
     * @throws ClientException
     */
    public StorableId upstore(KapuaId scopeId, ChannelInfo channelInfo)
            throws KapuaIllegalArgumentException,
            ConfigurationException, ClientException {
        //
        // Argument Validation
        ArgumentValidator.notNull(scopeId, "scopeId");
        ArgumentValidator.notNull(channelInfo, "channelInfoCreator");
        ArgumentValidator.notNull(channelInfo.getChannel(), "channelInfoCreator.getChannel");
        ArgumentValidator.notNull(channelInfo.getFirstPublishedMessageId(), "channelInfoCreator.messageId");
        ArgumentValidator.notNull(channelInfo.getFirstPublishedMessageTimestamp(), "channelInfoCreator.messageTimestamp");

        String channelInfoId = ChannelInfoField.getOrDeriveId(channelInfo.getId(), channelInfo);

        // Store channel. Look up channel in the cache, and cache it if it doesn't exist
        if (!DatastoreCacheManager.getInstance().getChannelsCache().get(channelInfoId)) {

            // The code is safe even without the synchronized block
            // Synchronize in order to let the first thread complete its
            // update then the others of the same type will find the cache
            // updated and skip the update.
            synchronized (this.metadataUpdateSync) {
                if (!DatastoreCacheManager.getInstance().getChannelsCache().get(channelInfoId)) {
                    UpdateResponse response = null;
                    Metadata metadata = mediator.getMetadata(scopeId, channelInfo.getFirstPublishedMessageTimestamp().getTime());
                    String registryIndexName = metadata.getRegistryIndexName();

                    UpdateRequest request = new UpdateRequest(new TypeDescriptor(metadata.getRegistryIndexName(), ChannelInfoSchema.CHANNEL_TYPE_NAME), channelInfo.getId().toString(), channelInfo);
                    response = client.upsert(request);

                    channelInfoId = response.getId();

                    logger.debug(String.format("Upsert on channel succesfully executed [%s.%s, %s]",
                            registryIndexName, ChannelInfoSchema.CHANNEL_TYPE_NAME, channelInfoId));

                    // Update cache if channel update is completed successfully
                    DatastoreCacheManager.getInstance().getChannelsCache().put(channelInfoId, true);
                }
            }
        }
        return new StorableIdImpl(channelInfoId);
    }

    /**
     * Delete channel information by identifier
     * 
     * @param scopeId
     * @param id
     * @throws KapuaIllegalArgumentException
     * @throws ConfigurationException
     * @throws QueryMappingException
     * @throws ClientException
     */
    public void delete(KapuaId scopeId, StorableId id)
            throws KapuaIllegalArgumentException,
            ConfigurationException,
            QueryMappingException,
            ClientException {
        //
        // Argument Validation
        ArgumentValidator.notNull(scopeId, "scopeId");
        ArgumentValidator.notNull(id, "id");

        //
        // Do the find
        MessageStoreConfiguration accountServicePlan = this.configProvider.getConfiguration(scopeId);
        long ttl = accountServicePlan.getDataTimeToLiveMilliseconds();

        if (!accountServicePlan.getDataStorageEnabled() || ttl == MessageStoreConfiguration.DISABLED) {
            logger.debug("Storage not enabled for account {}, return", scopeId);
            return;
        }

        String indexName = SchemaUtil.getKapuaIndexName(scopeId);

        ChannelInfo channelInfo = find(scopeId, id);

        this.mediator.onBeforeChannelInfoDelete(scopeId, channelInfo);

        TypeDescriptor typeDescriptor = new TypeDescriptor(indexName, ChannelInfoSchema.CHANNEL_TYPE_NAME);
        client.delete(typeDescriptor, id.toString());
    }

    /**
     * Find channel information by identifier
     * 
     * @param scopeId
     * @param id
     * @return
     * @throws KapuaIllegalArgumentException
     * @throws ConfigurationException
     * @throws QueryMappingException
     * @throws ClientException
     */
    public ChannelInfo find(KapuaId scopeId, StorableId id)
            throws KapuaIllegalArgumentException,
            ConfigurationException,
            QueryMappingException,
            ClientException {
        //
        // Argument Validation
        ArgumentValidator.notNull(scopeId, "scopeId");
        ArgumentValidator.notNull(id, "id");

        ChannelInfoQueryImpl idsQuery = new ChannelInfoQueryImpl();
        idsQuery.setLimit(1);

        ArrayList<StorableId> ids = new ArrayList<StorableId>();
        ids.add(id);

        AndPredicateImpl allPredicates = new AndPredicateImpl();
        allPredicates.addPredicate(new IdsPredicateImpl(ChannelInfoSchema.CHANNEL_TYPE_NAME, ids));

        String indexName = SchemaUtil.getKapuaIndexName(scopeId);
        TypeDescriptor typeDescriptor = new TypeDescriptor(indexName, ChannelInfoSchema.CHANNEL_TYPE_NAME);
        return client.find(typeDescriptor, idsQuery, ChannelInfo.class);
    }

    /**
     * Find channels informations matching the given query
     * 
     * @param scopeId
     * @param query
     * @return
     * @throws KapuaIllegalArgumentException
     * @throws ConfigurationException
     * @throws QueryMappingException
     * @throws ClientException
     */
    public ChannelInfoListResult query(KapuaId scopeId, ChannelInfoQuery query)
            throws KapuaIllegalArgumentException,
            ConfigurationException,
            QueryMappingException,
            ClientException {
        //
        // Argument Validation
        ArgumentValidator.notNull(scopeId, "scopeId");
        ArgumentValidator.notNull(query, "query");

        //
        // Do the find
        MessageStoreConfiguration accountServicePlan = this.configProvider.getConfiguration(scopeId);
        long ttl = accountServicePlan.getDataTimeToLiveMilliseconds();

        if (!accountServicePlan.getDataStorageEnabled() || ttl == MessageStoreConfiguration.DISABLED) {
            logger.debug("Storage not enabled for account {}, returning empty result", scopeId);
            return new ChannelInfoListResultImpl();
        }

        String indexName = SchemaUtil.getKapuaIndexName(scopeId);
        TypeDescriptor typeDescriptor = new TypeDescriptor(indexName, ChannelInfoSchema.CHANNEL_TYPE_NAME);
        List<ChannelInfo> result = client.query(typeDescriptor, query, ChannelInfo.class);

        ChannelInfoListResult listResult = new ChannelInfoListResultImpl();
        listResult.addAll(result);
        return listResult;
    }

    /**
     * Get channels informations count matching the given query
     * 
     * @param scopeId
     * @param query
     * @return
     * @throws KapuaIllegalArgumentException
     * @throws ConfigurationException
     * @throws QueryMappingException
     * @throws ClientException
     */
    public long count(KapuaId scopeId, ChannelInfoQuery query)
            throws KapuaIllegalArgumentException,
            ConfigurationException,
            QueryMappingException,
            ClientException {
        //
        // Argument Validation
        ArgumentValidator.notNull(scopeId, "scopeId");
        ArgumentValidator.notNull(query, "query");

        //
        // Do the find
        MessageStoreConfiguration accountServicePlan = this.configProvider.getConfiguration(scopeId);
        long ttl = accountServicePlan.getDataTimeToLiveMilliseconds();

        if (!accountServicePlan.getDataStorageEnabled() || ttl == MessageStoreConfiguration.DISABLED) {
            logger.debug("Storage not enabled for account {}, returning empty result", scopeId);
            return 0;
        }

        String indexName = SchemaUtil.getKapuaIndexName(scopeId);
        TypeDescriptor typeDescriptor = new TypeDescriptor(indexName, ChannelInfoSchema.CHANNEL_TYPE_NAME);
        return client.count(typeDescriptor, query);
    }

    /**
     * Delete channels informations count matching the given query
     * 
     * @param scopeId
     * @param query
     * @throws KapuaIllegalArgumentException
     * @throws QueryMappingException
     * @throws ConfigurationException
     * @throws ClientException
     */
    public void delete(KapuaId scopeId, ChannelInfoQuery query)
            throws KapuaIllegalArgumentException,
            QueryMappingException,
            ConfigurationException,
            ClientException {
        //
        // Argument Validation
        ArgumentValidator.notNull(scopeId, "scopeId");
        ArgumentValidator.notNull(query, "query");

        //
        // Do the find
        MessageStoreConfiguration accountServicePlan = this.configProvider.getConfiguration(scopeId);
        long ttl = accountServicePlan.getDataTimeToLiveMilliseconds();

        if (!accountServicePlan.getDataStorageEnabled() || ttl == MessageStoreConfiguration.DISABLED) {
            logger.debug("Storage not enabled for account {}, skipping delete", scopeId);
            return;
        }

        String indexName = SchemaUtil.getKapuaIndexName(scopeId);

        ChannelInfoListResult channels = this.query(scopeId, query);

        // TODO Improve performances
        for (ChannelInfo channelInfo : channels)
            this.mediator.onBeforeChannelInfoDelete(scopeId, channelInfo);

        TypeDescriptor typeDescriptor = new TypeDescriptor(indexName, ChannelInfoSchema.CHANNEL_TYPE_NAME);
        client.deleteByQuery(typeDescriptor, query);
    }

}
