/*******************************************************************************
 * Copyright (c) 2011, 2016 Eurotech and/or its affiliates and others
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *     Eurotech - initial API and implementation
 *
 *******************************************************************************/
package org.eclipse.kapua.service.datastore.internal;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.kapua.KapuaException;
import org.eclipse.kapua.commons.util.KapuaDateUtils;
import org.eclipse.kapua.locator.KapuaLocator;
import org.eclipse.kapua.message.internal.KapuaPositionImpl;
import org.eclipse.kapua.message.internal.device.data.KapuaDataChannelImpl;
import org.eclipse.kapua.message.internal.device.data.KapuaDataMessageImpl;
import org.eclipse.kapua.message.internal.device.data.KapuaDataPayloadImpl;
import org.eclipse.kapua.model.id.KapuaId;
import org.eclipse.kapua.service.account.Account;
import org.eclipse.kapua.service.account.AccountService;
import org.eclipse.kapua.service.datastore.ChannelInfoRegistryService;
import org.eclipse.kapua.service.datastore.ClientInfoRegistryService;
import org.eclipse.kapua.service.datastore.DatastoreObjectFactory;
import org.eclipse.kapua.service.datastore.MessageStoreService;
import org.eclipse.kapua.service.datastore.MetricInfoRegistryService;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.ChannelInfoField;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.ClientInfoField;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.DatastoreChannel;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.MetricInfoField;
import org.eclipse.kapua.service.datastore.internal.setting.DatastoreSettingKey;
import org.eclipse.kapua.service.datastore.internal.setting.DatastoreSettings;
import org.eclipse.kapua.service.datastore.model.ChannelInfo;
import org.eclipse.kapua.service.datastore.model.ChannelInfoListResult;
import org.eclipse.kapua.service.datastore.model.ClientInfo;
import org.eclipse.kapua.service.datastore.model.ClientInfoListResult;
import org.eclipse.kapua.service.datastore.model.DatastoreMessage;
import org.eclipse.kapua.service.datastore.model.MetricInfo;
import org.eclipse.kapua.service.datastore.model.MetricInfoListResult;
import org.eclipse.kapua.service.datastore.model.StorableId;
import org.eclipse.kapua.service.datastore.model.query.ChannelInfoQuery;
import org.eclipse.kapua.service.datastore.model.query.ClientInfoQuery;
import org.eclipse.kapua.service.datastore.model.query.MetricInfoQuery;
import org.eclipse.kapua.service.datastore.model.query.StorableFetchStyle;
import org.eclipse.kapua.service.datastore.model.query.TermPredicate;
import org.eclipse.kapua.service.device.registry.Device;
import org.eclipse.kapua.service.device.registry.DeviceCreator;
import org.eclipse.kapua.service.device.registry.DeviceFactory;
import org.eclipse.kapua.service.device.registry.DeviceRegistryService;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageStoreServiceTest extends AbstractMessageStoreServiceTest
{
    @SuppressWarnings("unused")
    private static final Logger   s_logger = LoggerFactory.getLogger(MessageStoreServiceTest.class);
    
    @Test
    public void testStore()
        throws Exception
    {
        Account account = this.getTestAccountCreator(adminScopeId);   

        DeviceRegistryService devRegistryService = KapuaLocator.getInstance().getService(DeviceRegistryService.class);
        DeviceFactory deviceFactory = KapuaLocator.getInstance().getFactory(DeviceFactory.class);
 
        Date now = new Date();

        String clientId = String.format("device-%d", now.getTime());
        DeviceCreator deviceCreator = deviceFactory.newCreator(account.getId(), clientId);
        Device device = devRegistryService.create(deviceCreator);
        
        KapuaDataMessageImpl message = new KapuaDataMessageImpl();
        KapuaDataChannelImpl channel = new KapuaDataChannelImpl();
        KapuaDataPayloadImpl messagePayload = new KapuaDataPayloadImpl();
        KapuaPositionImpl messagePosition = new KapuaPositionImpl();
        Map<String, Object> metrics = new HashMap<String, Object>();

        message.setScopeId(account.getId());
        message.setDeviceId(device.getId());
        message.setCapturedOn(now);
        message.setReceivedOn(now);

        DatastoreChannel datastoreChannel = new DatastoreChannel(account.getName(), clientId, "APP01");
        
        channel.setClientId(datastoreChannel.getClientId());
        channel.setSemanticParts(Arrays.asList(datastoreChannel.getParts()));
        message.setChannel(channel);
        
        metrics.put("distance", 1L);
        metrics.put("label", "pippo");
        messagePayload.setProperties(metrics);

        messagePosition.setAltitude(1.0);
        messagePosition.setTimestamp(now);
        message.setPosition(messagePosition);

        messagePayload.setProperties(metrics);
        message.setPayload(messagePayload);

        MessageStoreService messageStoreService = KapuaLocator.getInstance().getService(MessageStoreService.class);
        StorableId messageId = messageStoreService.store(account.getId(), message);

        //
        // A non empty message id must be returned
        assertNotNull(messageId);
        assertTrue(!messageId.toString().isEmpty());
        
        // 
        // Wait ES indexes to be refreshed
        DatastoreSettings settings = DatastoreSettings.getInstance();
        Thread.sleep(settings.getLong(DatastoreSettingKey.ELASTICSEARCH_IDX_REFRESH_INTERVAL) * KapuaDateUtils.SEC_MILLIS);
        
        // 
        // Retrieve the message from its id
        DatastoreMessage retrievedMessage = messageStoreService.find(account.getId(), messageId, StorableFetchStyle.SOURCE_FULL);

        //
        // The returned message must be not null and values must coincide
        assertNotNull(retrievedMessage);
        assertTrue(messageId.equals(retrievedMessage.getDatastoreId()));
        assertTrue(account.getScopeId().equals(retrievedMessage.getScopeId()));
        assertTrue(device.getId().equals(retrievedMessage.getDeviceId()));
        assertTrue(device.getClientId().equals(retrievedMessage.getClientId()));

        
        // There must be a client info entry in the registry
        DatastoreObjectFactory objectFactory = KapuaLocator.getInstance().getFactory(DatastoreObjectFactory.class);
        
        TermPredicate equalsMessageId = objectFactory.newTermPredicate(ClientInfoField.MESSAGE_ID, messageId);

        ClientInfoQuery clientInfoQuery = objectFactory.newClientInfoQuery();
        clientInfoQuery.setOffset(0);
        clientInfoQuery.setLimit(1);
        clientInfoQuery.setFetchStyle(StorableFetchStyle.FIELDS);
        clientInfoQuery.setPredicate(equalsMessageId);
        
        ClientInfoRegistryService clientInfoRegistry = KapuaLocator.getInstance().getService(ClientInfoRegistryService.class);
        ClientInfoListResult clientInfos = clientInfoRegistry.query(account.getId(), clientInfoQuery);
        
        assertNotNull(clientInfos);
        assertTrue(clientInfos.size() == 1);
        
        ClientInfo clientInfo = clientInfos.get(0);
        
        assertNotNull(clientInfo);
        assertTrue(messageId.equals(clientInfo.getLastMessageId()));

        // There must be a channel info entry in the registry
        equalsMessageId = objectFactory.newTermPredicate(ChannelInfoField.MESSAGE_ID, messageId);
        
        ChannelInfoQuery channelInfoQuery = objectFactory.newChannelInfoQuery();
        channelInfoQuery.setOffset(0);
        channelInfoQuery.setLimit(1);
        channelInfoQuery.setFetchStyle(StorableFetchStyle.FIELDS);
        channelInfoQuery.setPredicate(equalsMessageId);
        
        ChannelInfoRegistryService channelInfoRegistry = KapuaLocator.getInstance().getService(ChannelInfoRegistryService.class);
        ChannelInfoListResult channelInfos = channelInfoRegistry.query(account.getId(), channelInfoQuery);
        
        assertNotNull(channelInfos);
        assertTrue(channelInfos.size() == 1);
        
        ChannelInfo channelInfo = channelInfos.get(0);
        
        assertNotNull(channelInfo);
        assertTrue(messageId.equals(channelInfo.getLastMessageId()));

        // There must be two metric info entries in the registry
        equalsMessageId = objectFactory.newTermPredicate(MetricInfoField.MESSAGE_ID_FULL, messageId);
        
        MetricInfoQuery metricInfoQuery = objectFactory.newMetricInfoQuery();
        metricInfoQuery.setOffset(0);
        metricInfoQuery.setLimit(2);
        metricInfoQuery.setFetchStyle(StorableFetchStyle.FIELDS);
        metricInfoQuery.setPredicate(equalsMessageId);
        
        MetricInfoRegistryService metricInfoRegistry = KapuaLocator.getInstance().getService(MetricInfoRegistryService.class);
        MetricInfoListResult metricInfos = metricInfoRegistry.query(account.getId(), metricInfoQuery);
        
        assertNotNull(metricInfos);
        assertTrue(metricInfos.size() == 2);
        
        MetricInfo metricInfo = metricInfos.get(0);
        
        assertNotNull(metricInfo);
        assertTrue(messageId.equals(metricInfo.getLastMessageId()));
        
        metricInfo = metricInfos.get(1);
        
        assertNotNull(metricInfo);
        assertTrue(messageId.equals(metricInfo.getLastMessageId()));
    }

    private Account getTestAccountCreator(KapuaId scopeId) throws KapuaException
    {
        KapuaLocator locator = KapuaLocator.getInstance();
        Account account = locator.getService(AccountService.class).findByName("kapua-sys");
        return account;
    }
}
