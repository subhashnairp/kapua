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

import java.util.Map;

import org.eclipse.kapua.KapuaIllegalArgumentException;
import org.eclipse.kapua.locator.KapuaLocator;
import org.eclipse.kapua.message.KapuaPayload;
import org.eclipse.kapua.model.id.KapuaId;
import org.eclipse.kapua.service.datastore.ClientInfoRegistryService;
import org.eclipse.kapua.service.datastore.MetricInfoRegistryService;
import org.eclipse.kapua.service.datastore.client.ClientException;
import org.eclipse.kapua.service.datastore.client.QueryMappingException;
import org.eclipse.kapua.service.datastore.ChannelInfoRegistryService;
import org.eclipse.kapua.service.datastore.internal.ClientInfoRegistryFacade;
import org.eclipse.kapua.service.datastore.internal.MessageStoreFacade;
import org.eclipse.kapua.service.datastore.internal.MetricInfoRegistryFacade;
import org.eclipse.kapua.service.datastore.internal.ChannelInfoRegistryFacade;
import org.eclipse.kapua.service.datastore.internal.model.ClientInfoImpl;
import org.eclipse.kapua.service.datastore.internal.model.MetricInfoImpl;
import org.eclipse.kapua.service.datastore.internal.model.StorableIdImpl;
import org.eclipse.kapua.service.datastore.internal.model.ChannelInfoImpl;
import org.eclipse.kapua.service.datastore.internal.model.query.MessageQueryImpl;
import org.eclipse.kapua.service.datastore.internal.model.query.MetricInfoQueryImpl;
import org.eclipse.kapua.service.datastore.internal.schema.Metadata;
import org.eclipse.kapua.service.datastore.internal.schema.Schema;
import org.eclipse.kapua.service.datastore.internal.model.query.ChannelMatchPredicateImpl;
import org.eclipse.kapua.service.datastore.model.ClientInfo;
import org.eclipse.kapua.service.datastore.model.DatastoreMessage;
import org.eclipse.kapua.service.datastore.model.MetricInfo;
import org.eclipse.kapua.service.datastore.model.ChannelInfo;

/**
 * Datastore mediator definition
 * 
 * @since 1.0
 *
 */
public class DatastoreMediator implements MessageStoreMediator,
        ClientInfoRegistryMediator,
        ChannelInfoRegistryMediator,
        MetricInfoRegistryMediator {

    private static DatastoreMediator instance;

    private final Schema esSchema;

    private MessageStoreFacade messageStoreFacade;
    private ClientInfoRegistryFacade clientInfoStoreFacade;
    private ChannelInfoRegistryFacade channelInfoStoreFacade;
    private MetricInfoRegistryFacade metricInfoStoreFacade;

    static {
        instance = new DatastoreMediator();

        // Be sure the data registry services are instantiated
        KapuaLocator.getInstance().getService(ClientInfoRegistryService.class);
        KapuaLocator.getInstance().getService(ChannelInfoRegistryService.class);
        KapuaLocator.getInstance().getService(MetricInfoRegistryService.class);
    }

    private DatastoreMediator() {
        esSchema = new Schema();
    }

    /**
     * Get the {@link DatastoreMediator} instance (singleton)
     * 
     * @return
     */
    public static DatastoreMediator getInstance() {
        return instance;
    }

    /**
     * Set the message store facade
     * 
     * @param messageStoreFacade
     */
    public void setMessageStoreFacade(MessageStoreFacade messageStoreFacade) {
        this.messageStoreFacade = messageStoreFacade;
    }

    /**
     * Set the client info facade
     * 
     * @param clientInfoStoreFacade
     */
    public void setClientInfoStoreFacade(ClientInfoRegistryFacade clientInfoStoreFacade) {
        this.clientInfoStoreFacade = clientInfoStoreFacade;
    }

    /**
     * Set the channel info facade
     * 
     * @param channelInfoStoreFacade
     */
    public void setChannelInfoStoreFacade(ChannelInfoRegistryFacade channelInfoStoreFacade) {
        this.channelInfoStoreFacade = channelInfoStoreFacade;
    }

    /**
     * Set the metric info facade
     * 
     * @param metricInfoStoreFacade
     */
    public void setMetricInfoStoreFacade(MetricInfoRegistryFacade metricInfoStoreFacade) {
        this.metricInfoStoreFacade = metricInfoStoreFacade;
    }

    /*
     * Message Store Mediator methods
     */

    @Override
    public Metadata getMetadata(KapuaId scopeId, long indexedOn) throws ClientException {
        return esSchema.synch(scopeId, indexedOn);
    }

    @Override
    public void onUpdatedMappings(KapuaId scopeId, long indexedOn, Map<String, Metric> metrics) throws ClientException {
        esSchema.updateMessageMappings(scopeId, indexedOn, metrics);
    }

    @Override
    // public void onAfterMessageStore(KapuaId scopeId,
    // KapuaMessage<?, ?> message)
    public void onAfterMessageStore(KapuaId scopeId,
            MessageInfo messageInfo,
            DatastoreMessage message)
            throws KapuaIllegalArgumentException,
            ConfigurationException,
            ClientException {
        String accountName = messageInfo.getAccount().getName();
        // convert semantic channel to String
        String semanticChannel = message.getChannel() != null && message.getChannel().getSemanticParts() != null ? message.getChannel().getSemanticParts().toString() : "";

        ClientInfoImpl clientInfo = new ClientInfoImpl(accountName);
        clientInfo.setClientId(message.getClientId());
        clientInfo.setFirstPublishedMessageId(message.getDatastoreId());
        clientInfo.setFirstPublishedMessageTimestamp(message.getTimestamp());
        String clientInfoId = ClientInfoField.getOrDeriveId(null, accountName, message.getClientId());
        clientInfo.setId(new StorableIdImpl(clientInfoId));
        clientInfoStoreFacade.upstore(scopeId, clientInfo);

        ChannelInfoImpl channelInfo = new ChannelInfoImpl(accountName);
        channelInfo.setClientId(message.getClientId());
        channelInfo.setChannel(semanticChannel);
        channelInfo.setFirstPublishedMessageId(message.getDatastoreId());
        channelInfo.setFirstPublishedMessageTimestamp(message.getTimestamp());
        channelInfo.setId(new StorableIdImpl(ChannelInfoField.getOrDeriveId(null, channelInfo)));
        channelInfoStoreFacade.upstore(scopeId, channelInfo);

        KapuaPayload payload = message.getPayload();
        if (payload == null)
            return;

        Map<String, Object> metrics = payload.getProperties();
        if (metrics == null)
            return;

        int i = 0;
        MetricInfoImpl[] messageMetrics = new MetricInfoImpl[metrics.size()];
        for (Map.Entry<String, Object> entry : metrics.entrySet()) {
            MetricInfoImpl metricInfo = new MetricInfoImpl(accountName);
            metricInfo.setClientId(message.getClientId());
            metricInfo.setChannel(semanticChannel);
            metricInfo.setName(entry.getKey());
            metricInfo.setType(DatastoreUtils.getClientTypeFromValue(entry.getValue()));
            metricInfo.setFirstPublishedMessageId(message.getDatastoreId());
            metricInfo.setFirstPublishedMessageTimestamp(message.getTimestamp());
            metricInfo.setValue(entry.getValue());
            metricInfo.setId(new StorableIdImpl(MetricInfoField.getOrDeriveId(null, metricInfo)));
            messageMetrics[i++] = metricInfo;
        }

        metricInfoStoreFacade.upstore(scopeId, messageMetrics);
    }

    /*
     * ClientInfo Store Mediator methods
     */

    @Override
    public void onAfterClientInfoDelete(KapuaId scopeId, ClientInfo clientInfo)
            throws KapuaIllegalArgumentException,
            ConfigurationException,
            ClientException {
        messageStoreFacade.delete(scopeId, clientInfo.getFirstPublishedMessageId());
    }

    /*
     * ChannelInfo Store Mediator methods
     */

    @Override
    public void onBeforeChannelInfoDelete(KapuaId scopeId, ChannelInfo channelInfo)
            throws KapuaIllegalArgumentException,
            ConfigurationException,
            QueryMappingException,
            ClientException {
        MessageQueryImpl mqi = new MessageQueryImpl();
        ChannelMatchPredicateImpl predicate = new ChannelMatchPredicateImpl(MessageField.CHANNEL.field(), channelInfo.getChannel());
        mqi.setPredicate(predicate);
        messageStoreFacade.delete(scopeId, mqi);

        MetricInfoQueryImpl miqi = new MetricInfoQueryImpl();
        mqi.setPredicate(predicate);
        metricInfoStoreFacade.delete(scopeId, miqi);
    }

    @Override
    public void onAfterChannelInfoDelete(KapuaId scopeId, ChannelInfo channelInfo) {
        // TODO Auto-generated method stub

    }

    /*
     * MetricInfo Store Mediator methods
     */

    @Override
    public void onAfterMetricInfoDelete(KapuaId scopeId, MetricInfo metricInfo) {
        // TODO Auto-generated method stub

    }
}
