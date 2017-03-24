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
import org.eclipse.kapua.service.datastore.internal.mediator.ClientInfoRegistryMediator;
import org.eclipse.kapua.service.datastore.internal.mediator.ConfigurationException;
import org.eclipse.kapua.service.datastore.internal.mediator.MessageStoreConfiguration;
import org.eclipse.kapua.service.datastore.internal.model.ClientInfoListResultImpl;
import org.eclipse.kapua.service.datastore.internal.model.StorableIdImpl;
import org.eclipse.kapua.service.datastore.internal.model.query.AndPredicateImpl;
import org.eclipse.kapua.service.datastore.internal.model.query.ClientInfoQueryImpl;
import org.eclipse.kapua.service.datastore.internal.model.query.IdsPredicateImpl;
import org.eclipse.kapua.service.datastore.internal.schema.ClientInfoSchema;
import org.eclipse.kapua.service.datastore.internal.schema.Metadata;
import org.eclipse.kapua.service.datastore.internal.schema.SchemaUtil;
import org.eclipse.kapua.service.datastore.model.ClientInfo;
import org.eclipse.kapua.service.datastore.model.ClientInfoListResult;
import org.eclipse.kapua.service.datastore.model.StorableId;
import org.eclipse.kapua.service.datastore.model.query.ClientInfoQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client information registry facade
 * 
 * @since 1.0
 *
 */
public class ClientInfoRegistryFacade {

    private static final Logger logger = LoggerFactory.getLogger(ClientInfoRegistryFacade.class);

    private final ClientInfoRegistryMediator mediator;
    private final ConfigurationProvider configProvider;
    private final Object metadataUpdateSync;
    private Client client = null;

    /**
     * Constructs the client info registry facade
     * 
     * @param configProvider
     * @param mediator
     * @throws ClientUnavailableException
     */
    public ClientInfoRegistryFacade(ConfigurationProvider configProvider, ClientInfoRegistryMediator mediator) throws ClientUnavailableException {
        this.configProvider = configProvider;
        this.mediator = mediator;
        this.metadataUpdateSync = new Object();
        client = ClientFactory.getInstance();
    }

    /**
     * Update the client information after a message store operation
     * 
     * @param scopeId
     * @param clientInfo
     * @return
     * @throws KapuaIllegalArgumentException
     * @throws ConfigurationException
     * @throws ClientException
     */
    public StorableId upstore(KapuaId scopeId, ClientInfo clientInfo)
            throws KapuaIllegalArgumentException,
            ConfigurationException, ClientException {
        //
        // Argument Validation
        ArgumentValidator.notNull(scopeId, "scopeId");
        ArgumentValidator.notNull(clientInfo, "clientInfo");
        ArgumentValidator.notNull(clientInfo.getFirstPublishedMessageId(), "clientInfo.firstPublishedMessageId");
        ArgumentValidator.notNull(clientInfo.getFirstPublishedMessageTimestamp(), "clientInfo.firstPublishedMessageTimestamp");

        // ClientInfoXContentBuilder docBuilder = new ClientInfoXContentBuilder();
        // docBuilder.build(clientInfo);
        // TO CHECK WITH STEFANO
        // changed the docBuilder.getClientId() with clientInfo.getClientId()

        // Save client
        if (!DatastoreCacheManager.getInstance().getClientsCache().get(clientInfo.getClientId())) {

            // The code is safe even without the synchronized block
            // Synchronize in order to let the first thread complete its update
            // then the others of the same type will find the cache updated and
            // skip the update.
            synchronized (this.metadataUpdateSync) {
                if (!DatastoreCacheManager.getInstance().getClientsCache().get(clientInfo.getClientId())) {
                    UpdateResponse response = null;
                    Metadata metadata = mediator.getMetadata(scopeId, clientInfo.getFirstPublishedMessageTimestamp().getTime());
                    String kapuaIndexName = metadata.getRegistryIndexName();

                    UpdateRequest request = new UpdateRequest(new TypeDescriptor(kapuaIndexName, ClientInfoSchema.CLIENT_TYPE_NAME), clientInfo.getId().toString(), clientInfo);
                    response = client.upsert(request);

                    ClientInfo clientInfoFromClient = find(scopeId, new StorableIdImpl(response.getId()));
                    logger.debug(String.format("Upsert on asset succesfully executed [%s.%s, %s]", kapuaIndexName,
                            ClientInfoSchema.CLIENT_TYPE_NAME, response.getId()));
                    // Update cache if asset update is completed successfully
                    DatastoreCacheManager.getInstance().getClientsCache().put(clientInfo.getClientId(), true);
                }
            }
        }

        return new StorableIdImpl(clientInfo.getClientId());
    }

    /**
     * Delete client information by identifier
     * 
     * @param scopeId
     * @param id
     * @throws KapuaIllegalArgumentException
     * @throws ConfigurationException
     * @throws ClientException
     */
    public void delete(KapuaId scopeId, StorableId id)
            throws KapuaIllegalArgumentException,
            ConfigurationException, ClientException {
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

        String indexName = SchemaUtil.getDataIndexName(scopeId);
        TypeDescriptor typeDescriptor = new TypeDescriptor(indexName, ClientInfoSchema.CLIENT_TYPE_NAME);
        client.delete(typeDescriptor, id.toString());
    }

    /**
     * Find client information by identifier
     * 
     * @param scopeId
     * @param id
     * @return
     * @throws KapuaIllegalArgumentException
     * @throws ConfigurationException
     * @throws QueryMappingException
     * @throws ClientException
     */
    public ClientInfo find(KapuaId scopeId, StorableId id)
            throws KapuaIllegalArgumentException,
            ConfigurationException,
            QueryMappingException,
            ClientException {
        //
        // Argument Validation
        ArgumentValidator.notNull(scopeId, "scopeId");
        ArgumentValidator.notNull(id, "id");

        ClientInfoQueryImpl idsQuery = new ClientInfoQueryImpl();
        idsQuery.setLimit(1);

        ArrayList<StorableId> ids = new ArrayList<StorableId>();
        ids.add(id);

        AndPredicateImpl allPredicates = new AndPredicateImpl();
        allPredicates.addPredicate(new IdsPredicateImpl(ClientInfoSchema.CLIENT_TYPE_NAME, ids));
        
        idsQuery.setPredicate(allPredicates);

        String indexName = SchemaUtil.getKapuaIndexName(scopeId);
        TypeDescriptor typeDescriptor = new TypeDescriptor(indexName, ClientInfoSchema.CLIENT_TYPE_NAME);
        return client.find(typeDescriptor, idsQuery, ClientInfo.class);
    }

    /**
     * Find clients informations matching the given query
     * 
     * @param scopeId
     * @param query
     * @return
     * @throws KapuaIllegalArgumentException
     * @throws ConfigurationException
     * @throws QueryMappingException
     * @throws ClientException
     */
    public ClientInfoListResult query(KapuaId scopeId, ClientInfoQuery query)
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
            return new ClientInfoListResultImpl();
        }

        String indexName = SchemaUtil.getKapuaIndexName(scopeId);
        ClientInfoListResult listResult = new ClientInfoListResultImpl();
        TypeDescriptor typeDescriptor = new TypeDescriptor(indexName, ClientInfoSchema.CLIENT_TYPE_NAME);
        List<ClientInfo> result = client.query(typeDescriptor, query, ClientInfo.class);
        listResult.addAll(result);
        return listResult;
    }

    /**
     * Get clients informations count matching the given query
     * 
     * @param scopeId
     * @param query
     * @return
     * @throws KapuaIllegalArgumentException
     * @throws ConfigurationException
     * @throws QueryMappingException
     * @throws ClientException
     */
    public long count(KapuaId scopeId, ClientInfoQuery query)
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

        String dataIndexName = SchemaUtil.getKapuaIndexName(scopeId);
        TypeDescriptor typeDescriptor = new TypeDescriptor(dataIndexName, ClientInfoSchema.CLIENT_TYPE_NAME);
        return client.count(typeDescriptor, query);
    }

    /**
     * Delete clients informations count matching the given query
     * 
     * @param scopeId
     * @param query
     * @throws KapuaIllegalArgumentException
     * @throws ConfigurationException
     * @throws QueryMappingException
     * @throws ClientException
     */
    public void delete(KapuaId scopeId, ClientInfoQuery query)
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
            logger.debug("Storage not enabled for account {}, skipping delete", scopeId);
            return;
        }

        String indexName = SchemaUtil.getKapuaIndexName(scopeId);
        TypeDescriptor typeDescriptor = new TypeDescriptor(indexName, ClientInfoSchema.CLIENT_TYPE_NAME);
        client.deleteByQuery(typeDescriptor, query);
    }

}
