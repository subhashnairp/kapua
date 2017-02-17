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

import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang.ArrayUtils;
import org.eclipse.kapua.KapuaException;
import org.eclipse.kapua.commons.util.KapuaDateUtils;
import org.eclipse.kapua.locator.KapuaLocator;
import org.eclipse.kapua.message.KapuaChannel;
import org.eclipse.kapua.message.KapuaPosition;
import org.eclipse.kapua.message.device.data.KapuaDataMessage;
import org.eclipse.kapua.message.device.data.KapuaDataPayload;
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
import org.eclipse.kapua.service.datastore.internal.elasticsearch.EsSchema;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.MessageStoreConfiguration;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.MetricInfoField;
import org.eclipse.kapua.service.datastore.internal.model.DataIndexBy;
import org.eclipse.kapua.service.datastore.internal.model.MetricsIndexBy;
import org.eclipse.kapua.service.datastore.internal.model.query.AndPredicateImpl;
import org.eclipse.kapua.service.datastore.internal.model.query.ChannelInfoQueryImpl;
import org.eclipse.kapua.service.datastore.internal.model.query.MessageQueryImpl;
import org.eclipse.kapua.service.datastore.internal.model.query.RangePredicateImpl;
import org.eclipse.kapua.service.datastore.internal.model.query.SortFieldImpl;
import org.eclipse.kapua.service.datastore.internal.setting.DatastoreSettingKey;
import org.eclipse.kapua.service.datastore.internal.setting.DatastoreSettings;
import org.eclipse.kapua.service.datastore.model.ChannelInfo;
import org.eclipse.kapua.service.datastore.model.ChannelInfoListResult;
import org.eclipse.kapua.service.datastore.model.ClientInfo;
import org.eclipse.kapua.service.datastore.model.ClientInfoListResult;
import org.eclipse.kapua.service.datastore.model.DatastoreMessage;
import org.eclipse.kapua.service.datastore.model.MessageListResult;
import org.eclipse.kapua.service.datastore.model.MetricInfo;
import org.eclipse.kapua.service.datastore.model.MetricInfoListResult;
import org.eclipse.kapua.service.datastore.model.StorableId;
import org.eclipse.kapua.service.datastore.model.query.AndPredicate;
import org.eclipse.kapua.service.datastore.model.query.ChannelInfoQuery;
import org.eclipse.kapua.service.datastore.model.query.ClientInfoQuery;
import org.eclipse.kapua.service.datastore.model.query.MessageQuery;
import org.eclipse.kapua.service.datastore.model.query.MetricInfoQuery;
import org.eclipse.kapua.service.datastore.model.query.RangePredicate;
import org.eclipse.kapua.service.datastore.model.query.SortDirection;
import org.eclipse.kapua.service.datastore.model.query.SortField;
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

    private static final Logger s_logger = LoggerFactory.getLogger(MessageStoreServiceTest.class);

    @Test
    /**
     * Store few messages with few metrics, position and body (partially randomly generated) and check if the stored message (retrieved by id) has all the fields correctly set
     * 
     * @throws Exception
     */
    public void storeMessages()
        throws Exception
    {
        Account account = getTestAccountCreator(adminScopeId);

        String[] availableSematicParts = new String[] {
                                                        "/device1/bus/route/one",
                                                        "/device2/bus/route/one",
                                                        "/device3/bus/route/two/a",
                                                        "/device4/bus/route/two/b",
                                                        "/device5/tram/route/one",
                                                        "/device6/car/one"
        };

        Random random = new Random();
        DeviceRegistryService devRegistryService = KapuaLocator.getInstance().getService(DeviceRegistryService.class);
        DeviceFactory deviceFactory = KapuaLocator.getInstance().getFactory(DeviceFactory.class);

        KapuaDataMessage message = null;
        String clientId = String.format("device-%d", new Date().getTime());
        DeviceCreator deviceCreator = deviceFactory.newCreator(account.getScopeId(), clientId);
        Device device = devRegistryService.create(deviceCreator);
        for (int i = 0; i < 12; i++) {
            byte[] randomPayload = new byte[128];
            random.nextBytes(randomPayload);
            String stringPayload = "Hello World" + (i + 1) + " \n\n\t\n\tHelloWord2";
            byte[] payload = ArrayUtils.addAll(randomPayload, stringPayload.getBytes());

            KapuaDataPayloadImpl messagePayload = new KapuaDataPayloadImpl();

            Map<String, Object> metrics = new HashMap<String, Object>();
            metrics.put("float_int", new Float((float) (i + 1)));
            metrics.put("float_float", new Float((float) (i + 1) * 0.01));
            metrics.put("integer_value", new Integer((i + 1)));
            metrics.put("double_int", (double) (i + 1));
            metrics.put("double_float", (double) (i + 1) * 0.01);
            metrics.put("long_long", (long) (10000000000000l * (i + 1)));
            metrics.put("long_int_1", (long) (1000 * (i + 1)));
            metrics.put("long_int_2", (long) (i + 1));
            metrics.put("string_value", Integer.toString((i + 1) * 1000));
            messagePayload.setProperties(metrics);

            messagePayload.setBody(payload);
            Date receivedOn = new Date();
            Date sentOn = new Date(new SimpleDateFormat("dd/MM/yyyy").parse("01/01/2015").getTime());
            Date capturedOn = new Date(new SimpleDateFormat("dd/MM/yyyy").parse("01/01/2016").getTime());
            String semanticTopic = availableSematicParts[i % availableSematicParts.length];
            message = getMessage(clientId, account.getScopeId(), device.getId(), receivedOn, capturedOn, sentOn);
            updateChannel(message, semanticTopic);
            updatePayload(message, messagePayload);
            KapuaPosition messagePosition = getPosition(10.00d * (i + 1), 12d * (i + 1), 1.123d * (i + 1), 2d * (i + 1), 0001d * (i + 1), 1000 * (i + 1), 1d * (i + 1), 44 * (i + 1), new Date());
            message.setPosition(messagePosition);
            StorableId messageStoredId = null;
            try {
                // leave the message index by as default (DEVICE_TIMESTAMP)
                MessageStoreService messageStoreService = KapuaLocator.getInstance().getService(MessageStoreService.class);
                messageStoredId = messageStoreService.store(message);

                // Wait ES indexes to be refreshed
                DatastoreSettings settings = DatastoreSettings.getInstance();
                Thread.sleep(settings.getLong(DatastoreSettingKey.ELASTICSEARCH_IDX_REFRESH_INTERVAL) * KapuaDateUtils.SEC_MILLIS);

                DatastoreMessage messageQueried = messageStoreService.find(account.getScopeId(), messageStoredId, StorableFetchStyle.SOURCE_FULL);
                checkMessageId(messageQueried, messageStoredId);
                checkTopic(messageQueried, semanticTopic);
                checkMessageBody(messageQueried, message.getPayload().getBody());
                checkMetricsSize(messageQueried, metrics.size());
                checkMetrics(messageQueried, metrics);
                checkPosition(messageQueried, messagePosition);
                checkMessageDate(messageQueried, new Range<Date>("timestamp", capturedOn), new Range<Date>("sentOn", sentOn), new Range<Date>("capturedOn", capturedOn), new Range<Date>("receivedOn", receivedOn));
            }
            catch (KapuaException e) {
                s_logger.error("Exception: ", e.getMessage(), e);
            }
            Thread.sleep(1);
        }
    }

    /**
     * Test the correctness of the query filtering order (3 fields: date descending, date ascending, string descending)
     * 
     * @throws Exception
     */
    @Test
    public void testOrderingMixedTest()
        throws Exception
    {
        Account account = getTestAccountCreator(adminScopeId);

        String[] availableSematicParts = new String[] {
                                                        "/device1/bus/route/one",
                                                        "/device2/bus/route/one",
                                                        "/device3/bus/route/two/a",
                                                        "/device4/bus/route/two/b",
                                                        "/device5/tram/route/one",
                                                        "/device6/car/one"
        };

        DeviceRegistryService devRegistryService = KapuaLocator.getInstance().getService(DeviceRegistryService.class);
        DeviceFactory deviceFactory = KapuaLocator.getInstance().getFactory(DeviceFactory.class);

        KapuaDataMessage message = null;
        String clientId1 = String.format("device-%d", new Date().getTime());
        Thread.sleep(100);
        String clientId2 = String.format("device-%d", new Date().getTime());
        DeviceCreator deviceCreator = deviceFactory.newCreator(account.getScopeId(), clientId1);
        Device device1 = devRegistryService.create(deviceCreator);
        DeviceCreator deviceCreator2 = deviceFactory.newCreator(account.getScopeId(), clientId2);
        Device device2 = devRegistryService.create(deviceCreator2);
        int messagesCount = 100;
        MessageStoreService messageStoreService = KapuaLocator.getInstance().getService(MessageStoreService.class);
        Date sentOn1 = new Date();
        Date sentOn2 = new Date(sentOn1.getTime() + 5000);
        Date capturedOn1 = new Date(new Date().getTime() + 10000);
        Date capturedOn2 = new Date(capturedOn1.getTime() + 20000);
        String clientId = null;
        Device device = null;
        // leave the message index by as default (DEVICE_TIMESTAMP)
        for (int i = 0; i < messagesCount; i++) {
            clientId = clientId1;
            device = device1;
            Date receivedOn = new Date();
            Date sentOn = null;
            if (i < messagesCount / 2) {
                sentOn = sentOn1;
            }
            else {
                sentOn = sentOn2;
            }
            Date capturedOn = null;
            if (i < messagesCount / 4 - 1 || (i > messagesCount / 2 - 1 && i < messagesCount * 3 / 4 - 1)) {
                capturedOn = capturedOn1;
                if (i % 2 == 0) {
                    clientId = clientId2;
                    device = device2;
                }
            }
            else {
                capturedOn = capturedOn2;
                if (i % 2 == 0) {
                    clientId = clientId2;
                    device = device2;
                }
            }
            String semanticTopic = availableSematicParts[i % availableSematicParts.length];
            message = getMessage(clientId, account.getScopeId(), device.getId(), receivedOn, capturedOn, sentOn);
            updateChannel(message, semanticTopic);
            List<StorableId> messageStoredId = new ArrayList<StorableId>();
            try {
                messageStoredId.add(messageStoreService.store(message));
            }
            catch (KapuaException e) {
                s_logger.error("Exception: ", e.getMessage(), e);
            }
            Thread.sleep(1);
        }
        // Wait ES indexes to be refreshed
        DatastoreSettings settings = DatastoreSettings.getInstance();
        Thread.sleep(settings.getLong(DatastoreSettingKey.ELASTICSEARCH_IDX_REFRESH_INTERVAL) * KapuaDateUtils.SEC_MILLIS);
        List<SortField> sort = new ArrayList<SortField>();
        SortField sortSentOn = new SortFieldImpl();
        sortSentOn.setField(EsSchema.MESSAGE_SENT_ON);
        sortSentOn.setSortDirection(SortDirection.DESC);
        sort.add(sortSentOn);
        SortField sortTimestamp = new SortFieldImpl();
        sortTimestamp.setField(EsSchema.MESSAGE_TIMESTAMP);
        sortTimestamp.setSortDirection(SortDirection.ASC);
        sort.add(sortTimestamp);
        SortField sortClientId = new SortFieldImpl();
        sortClientId.setField(EsSchema.MESSAGE_CLIENT_ID);
        sortClientId.setSortDirection(SortDirection.DESC);
        sort.add(sortClientId);
        MessageQuery messageQuery = getOrderedQuery(messagesCount + 1, sort);
        DatastoreObjectFactory objectFactory = KapuaLocator.getInstance().getFactory(DatastoreObjectFactory.class);
        AndPredicate andPredicate = new AndPredicateImpl();
        TermPredicate accountName = objectFactory.newTermPredicate(ClientInfoField.ACCOUNT, account.getName());
        andPredicate.getPredicates().add(accountName);
        RangePredicate timestampPredicate = new RangePredicateImpl(ClientInfoField.TIMESTAMP, new Date(capturedOn1.getTime()), new Date(capturedOn2.getTime()));
        andPredicate.getPredicates().add(timestampPredicate);
        messageQuery.setPredicate(andPredicate);
        
        MessageListResult result = messageStoreService.query(account.getScopeId(), messageQuery);
        checkMessagesCount(result, messagesCount);
        for (DatastoreMessage messageStored : result) {
            s_logger.info("message sentOn: '" + messageStored.getSentOn() + "' - capturedOn: '" + messageStored.getCapturedOn() + "' clientId: '" + messageStored.getClientId() + "'");
        }
        checkListOrder(result, sort);
    }

    @Test
    /**
     * Test the correctness of the storage process with a basic message (no metrics, payload and position) indexing message date by device timestamp (as default)
     * 
     * @throws Exception
     */
    public void testStoreWithDeviceTimestampIndexing()
        throws Exception
    {
        DeviceRegistryService devRegistryService = KapuaLocator.getInstance().getService(DeviceRegistryService.class);
        DeviceFactory deviceFactory = KapuaLocator.getInstance().getFactory(DeviceFactory.class);

        Account account = createAccount(null, null);
        Date messageTime = new Date();
        String clientId = String.format("device-%d", messageTime.getTime());
        DeviceCreator deviceCreator = deviceFactory.newCreator(account.getScopeId(), clientId);
        Device device = devRegistryService.create(deviceCreator);
        
        String topicSemanticPart = "testStoreWithNullPayload/testStoreWithNullPayload/" + Calendar.getInstance().getTimeInMillis();
        Date sentOn = new Date(new SimpleDateFormat("dd/MM/yyyy").parse("01/01/2015").getTime());
        Date capturedOn = new Date();
        Date receivedOn = new Date();
        KapuaDataMessage message = getMessage(clientId, account.getScopeId(), device.getId(), receivedOn, capturedOn, sentOn);
        updateChannel(message, topicSemanticPart);
        message.setReceivedOn(messageTime);

        MessageStoreService messageStoreService = KapuaLocator.getInstance().getService(MessageStoreService.class);
        // leave the message index by as default (DEVICE_TIMESTAMP)
        StorableId messageStoredId = null;
        try {
            messageStoredId = messageStoreService.store(message);
        }
        catch (Exception e) {
            fail("Store message with null payload should have succeded");
        }

        // Wait ES indexes to be refreshed
        DatastoreSettings settings = DatastoreSettings.getInstance();
        Thread.sleep(settings.getLong(DatastoreSettingKey.ELASTICSEARCH_IDX_REFRESH_INTERVAL) * KapuaDateUtils.SEC_MILLIS);

        MessageQuery messageQuery = getBaseMessageQuery();
        DatastoreObjectFactory objectFactory = KapuaLocator.getInstance().getFactory(DatastoreObjectFactory.class);
        AndPredicate andPredicate = new AndPredicateImpl();
        TermPredicate accountName = objectFactory.newTermPredicate(ClientInfoField.ACCOUNT, account.getName());
        andPredicate.getPredicates().add(accountName);
        RangePredicate timestampPredicate = new RangePredicateImpl(ClientInfoField.TIMESTAMP, new Date(capturedOn.getTime()), new Date(capturedOn.getTime()));
        andPredicate.getPredicates().add(timestampPredicate);

        // TermPredicate timestampPredicate = objectFactory.newTermPredicate(ClientInfoField.TIMESTAMP, new Date(messageTime));
        // andPredicate.getPredicates().add(timestampPredicate);

        // TermPredicate clientIdPredicate = objectFactory.newTermPredicate(ClientInfoField.CLIENT_ID, clientId);
        // andPredicate.getPredicates().add(clientIdPredicate);
        messageQuery.setPredicate(andPredicate);

        MessageListResult result = messageStoreService.query(account.getScopeId(), messageQuery);
        DatastoreMessage messageQueried = checkMessagesCount(result, 1);
        checkMessageId(messageQueried, messageStoredId);
        checkMessageBody(messageQueried, null);
        checkMetricsSize(messageQueried, 0);
        checkPosition(messageQueried, null);
        checkMessageDate(messageQueried, new Range<Date>("timestamp", capturedOn), new Range<Date>("sentOn", sentOn), new Range<Date>("capturedOn", capturedOn), new Range<Date>("receivedOn", messageTime));
    }

    @Test
    /**
     * Test the correctness of the storage process with a basic message (no metrics, payload and position) indexing message date by server timestamp
     * 
     * @throws Exception
     */
    public void testStoreWithServerTimestampIndexing()
        throws Exception
    {
        DeviceRegistryService devRegistryService = KapuaLocator.getInstance().getService(DeviceRegistryService.class);
        DeviceFactory deviceFactory = KapuaLocator.getInstance().getFactory(DeviceFactory.class);

        Account account = createAccount(null, null);
        Date messageTime = new Date();
        String clientId = String.format("device-%d", messageTime.getTime());
        DeviceCreator deviceCreator = deviceFactory.newCreator(account.getScopeId(), clientId);
        Device device = devRegistryService.create(deviceCreator);

        String topicSemanticPart = "testStoreWithNullPayload/testStoreWithNullPayload/" + Calendar.getInstance().getTimeInMillis();
        Date sentOn = new Date(new SimpleDateFormat("dd/MM/yyyy").parse("01/01/2015").getTime());
        Date capturedOn = new Date();
        Date receivedOn = new Date();
        KapuaDataMessage message = getMessage(clientId, account.getScopeId(), device.getId(), receivedOn, capturedOn, sentOn);
        updateChannel(message, topicSemanticPart);
        message.setReceivedOn(messageTime);

        MessageStoreService messageStoreService = KapuaLocator.getInstance().getService(MessageStoreService.class);
        StorableId messageStoredId = null;
        updateConfiguration(messageStoreService, account.getScopeId(), DataIndexBy.SERVER_TIMESTAMP, MetricsIndexBy.TIMESTAMP, 30, true);
        messageTime = new Date();
        message.setReceivedOn(messageTime);
        try {
            messageStoredId = messageStoreService.store(message);
        }
        catch (Exception e) {
            fail("Store message with null payload should have succeded");
        }

        // Wait ES indexes to be refreshed
        DatastoreSettings settings = DatastoreSettings.getInstance();
        Thread.sleep(settings.getLong(DatastoreSettingKey.ELASTICSEARCH_IDX_REFRESH_INTERVAL) * KapuaDateUtils.SEC_MILLIS);

        MessageQuery messageQuery = getBaseMessageQuery();

        DatastoreObjectFactory objectFactory = KapuaLocator.getInstance().getFactory(DatastoreObjectFactory.class);
        AndPredicate andPredicate = new AndPredicateImpl();
        TermPredicate accountName = objectFactory.newTermPredicate(ClientInfoField.ACCOUNT, account.getName());
        andPredicate.getPredicates().add(accountName);
        Date timestampUpperBound = new Date(messageTime.getTime() + 2000);
        Date timestampLowerBound = new Date(messageTime.getTime() - 2000);
        RangePredicate timestampPredicate = new RangePredicateImpl(ClientInfoField.TIMESTAMP, timestampLowerBound, timestampUpperBound);
        andPredicate.getPredicates().add(timestampPredicate);
        messageQuery.setPredicate(andPredicate);
        
        MessageListResult result = messageStoreService.query(account.getScopeId(), messageQuery);
        DatastoreMessage messageQueried = checkMessagesCount(result, 1);
        checkMessageId(messageQueried, messageStoredId);
        checkTopic(messageQueried, topicSemanticPart);
        checkMessageBody(messageQueried, null);
        checkMetricsSize(messageQueried, 0);
        checkPosition(messageQueried, null);
        checkMessageDate(messageQueried, new Range<Date>("timestamp", timestampLowerBound, timestampUpperBound), new Range<Date>("sentOn", sentOn), new Range<Date>("capturedOn", capturedOn), new Range<Date>("receivedOn", messageTime));
    }

    @Test
    /**
     * Check the correctness of the channel info data stored by retrieving the client id information.
     * This test stores few messages with 4 different client id so it checks if the channel info stored for that account contains 4 records and if the 4 clients id are the same used by the store
     * messages process.
     * 
     * @throws Exception
     */
    public void testClientIdByAccount()
        throws Exception
    {
        DeviceRegistryService devRegistryService = KapuaLocator.getInstance().getService(DeviceRegistryService.class);
        DeviceFactory deviceFactory = KapuaLocator.getInstance().getFactory(DeviceFactory.class);

        Account account = createAccount(null, null);
        Date messageTime = new Date();
        String clientId = String.format("device-%d", messageTime.getTime());
        DeviceCreator deviceCreator = deviceFactory.newCreator(account.getScopeId(), clientId);
        Device device = devRegistryService.create(deviceCreator);

        String accountName = account.getName();
        String[] assets = new String[] { "asset1", "asset2", "asset3", "asset4" };
        // Create two sample "metric" messages
        Date sentOn = new Date(new SimpleDateFormat("dd/MM/yyyy").parse("01/01/2015").getTime());
        Date capturedOn = new Date();
        Date receivedOn = new Date();
        KapuaDataMessage message1 = getMessage(assets[0], account.getScopeId(), device.getId(), receivedOn, capturedOn, sentOn);
        updateChannel(message1, "1/2/3");
        message1.setReceivedOn(messageTime);
        KapuaDataMessage message2 = getMessage(assets[1], account.getScopeId(), device.getId(), receivedOn, capturedOn, sentOn);
        updateChannel(message2, "1/2/3");
        message2.setReceivedOn(messageTime);
        KapuaDataMessage message3 = getMessage(assets[2], account.getScopeId(), device.getId(), receivedOn, capturedOn, sentOn);
        updateChannel(message3, "1/2/3");
        message3.setReceivedOn(messageTime);
        KapuaDataMessage message4 = getMessage(assets[3], account.getScopeId(), device.getId(), receivedOn, capturedOn, sentOn);
        updateChannel(message4, "1/2/3");
        message4.setReceivedOn(messageTime);
        KapuaDataMessage message5 = getMessage(assets[3], account.getScopeId(), device.getId(), receivedOn, capturedOn, sentOn);
        updateChannel(message5, "1/2/3");
        message5.setReceivedOn(messageTime);
        KapuaDataMessage message6 = getMessage(assets[3], account.getScopeId(), device.getId(), receivedOn, capturedOn, sentOn);
        updateChannel(message6, "1/2/3");
        message6.setReceivedOn(messageTime);
        MessageStoreService messageStoreService = KapuaLocator.getInstance().getService(MessageStoreService.class);
        StorableId messageStoredId1 = null;
        StorableId messageStoredId2 = null;
        StorableId messageStoredId3 = null;
        StorableId messageStoredId4 = null;
        StorableId messageStoredId5 = null;
        StorableId messageStoredId6 = null;
        updateConfiguration(messageStoreService, account.getScopeId(), DataIndexBy.DEVICE_TIMESTAMP, MetricsIndexBy.TIMESTAMP, 30, true);
        // Store messages
        try {
            messageStoredId1 = messageStoreService.store(message1);
            messageStoredId2 = messageStoreService.store(message2);
            messageStoredId3 = messageStoreService.store(message3);
            messageStoredId4 = messageStoreService.store(message4);
            messageStoredId5 = messageStoreService.store(message5);
            messageStoredId6 = messageStoreService.store(message6);
        }
        catch (Exception e) {
            fail("Store messages should have succeded");
        }

        // Wait ES indexes to be refreshed
        DatastoreSettings settings = DatastoreSettings.getInstance();
        Thread.sleep(settings.getLong(DatastoreSettingKey.ELASTICSEARCH_IDX_REFRESH_INTERVAL) * KapuaDateUtils.SEC_MILLIS);

        Date timestampUpperBound = new Date(messageTime.getTime() + 2000);
        Date timestampLowerBound = new Date(messageTime.getTime() - 2000);
        ChannelInfoRegistryService registryService = KapuaLocator.getInstance().getService(ChannelInfoRegistryService.class);

        ChannelInfoQuery channelInfoQuery = getBaseChannelInfoQuery();

        DatastoreObjectFactory objectFactory = KapuaLocator.getInstance().getFactory(DatastoreObjectFactory.class);
        AndPredicate andPredicateChannel = new AndPredicateImpl();
        TermPredicate accountNamePredicateChannel = objectFactory.newTermPredicate(ChannelInfoField.ACCOUNT, account.getName());
        andPredicateChannel.getPredicates().add(accountNamePredicateChannel);
        RangePredicate timestampPredicateChannel = new RangePredicateImpl(ClientInfoField.TIMESTAMP, timestampLowerBound, timestampUpperBound);
        andPredicateChannel.getPredicates().add(timestampPredicateChannel);
        channelInfoQuery.setPredicate(andPredicateChannel);

        ArrayList<String> allAssets = new ArrayList<String>();

        ChannelInfoListResult channelList = registryService.query(account.getScopeId(), channelInfoQuery);
        for (ChannelInfo channelInfo : channelList) {
            allAssets.add(channelInfo.getClientId());
        }
        for (String asset : assets) {
            assertTrue(allAssets.contains(asset));
        }
    }

    @Test
    /**
     * Check the correctness of the channel info last publish date stored by retrieving the client id information.
     * messages process.
     * This test is failing because Elastichsearch caching code should be improved.
     * 
     * @throws Exception
     */
    public void testClientIdPublishDateByAccount()
        throws Exception
    {
        DeviceRegistryService devRegistryService = KapuaLocator.getInstance().getService(DeviceRegistryService.class);
        DeviceFactory deviceFactory = KapuaLocator.getInstance().getFactory(DeviceFactory.class);

        Account account = createAccount(null, null);
        Date messageTime = new Date();
        String clientId = String.format("device-%d", messageTime.getTime());
        DeviceCreator deviceCreator = deviceFactory.newCreator(account.getScopeId(), clientId);
        Device device = devRegistryService.create(deviceCreator);

        String accountName = account.getName();
        String[] assets = new String[] { "asset1", "asset2" };
        // Create two sample "metric" messages
        Date sentOn = new Date(new SimpleDateFormat("dd/MM/yyyy").parse("01/01/2015").getTime());
        Date capturedOn = new Date();
        Date capturedOnSecondMessage = new Date(capturedOn.getTime() + 1000);
        Date capturedOnThirdMessage = new Date(capturedOn.getTime() + 2000);
        Date receivedOn = new Date();
        KapuaDataMessage message1 = getMessage(assets[0], account.getScopeId(), device.getId(), receivedOn, capturedOn, sentOn);
        updateChannel(message1, "1/2/3");
        message1.setReceivedOn(messageTime);
        KapuaDataMessage message2 = getMessage(assets[1], account.getScopeId(), device.getId(), receivedOn, capturedOn, sentOn);
        updateChannel(message2, "1/2/3");
        message2.setReceivedOn(messageTime);
        KapuaDataMessage message3 = getMessage(assets[1], account.getScopeId(), device.getId(), receivedOn, capturedOnSecondMessage, sentOn);
        updateChannel(message3, "1/2/3");
        message3.setReceivedOn(messageTime);
        KapuaDataMessage message4 = getMessage(assets[1], account.getScopeId(), device.getId(), receivedOn, capturedOnThirdMessage, sentOn);
        updateChannel(message4, "1/2/3");
        message4.setReceivedOn(messageTime);
        MessageStoreService messageStoreService = KapuaLocator.getInstance().getService(MessageStoreService.class);
        StorableId messageStoredId1 = null;
        StorableId messageStoredId2 = null;
        StorableId messageStoredId3 = null;
        StorableId messageStoredId4 = null;
        updateConfiguration(messageStoreService, account.getScopeId(), DataIndexBy.DEVICE_TIMESTAMP, MetricsIndexBy.TIMESTAMP, 30, true);
        // Store messages
        try {
            messageStoredId1 = messageStoreService.store(message1);
            messageStoredId2 = messageStoreService.store(message2);
            messageStoredId3 = messageStoreService.store(message3);
            messageStoredId4 = messageStoreService.store(message4);
        }
        catch (Exception e) {
            fail("Store messages should have succeded");
        }

        // Wait ES indexes to be refreshed
        DatastoreSettings settings = DatastoreSettings.getInstance();
        Thread.sleep(settings.getLong(DatastoreSettingKey.ELASTICSEARCH_IDX_REFRESH_INTERVAL) * KapuaDateUtils.SEC_MILLIS);

        Date timestampUpperBound = new Date(capturedOnThirdMessage.getTime() + 2000);
        Date timestampLowerBound = new Date(messageTime.getTime() - 2000);
        ChannelInfoRegistryService registryService = KapuaLocator.getInstance().getService(ChannelInfoRegistryService.class);

        ChannelInfoQuery channelInfoQuery = getBaseChannelInfoQuery();

        DatastoreObjectFactory objectFactory = KapuaLocator.getInstance().getFactory(DatastoreObjectFactory.class);
        AndPredicate andPredicateChannel = new AndPredicateImpl();
        TermPredicate accountNamePredicateChannel = objectFactory.newTermPredicate(ChannelInfoField.ACCOUNT, account.getName());
        andPredicateChannel.getPredicates().add(accountNamePredicateChannel);
        RangePredicate timestampPredicateChannel = new RangePredicateImpl(ClientInfoField.TIMESTAMP, timestampLowerBound, timestampUpperBound);
        andPredicateChannel.getPredicates().add(timestampPredicateChannel);
        channelInfoQuery.setPredicate(andPredicateChannel);

        ArrayList<String> allAssets = new ArrayList<String>();

        ChannelInfoListResult channelList = registryService.query(account.getScopeId(), channelInfoQuery);
        for (ChannelInfo channelInfo : channelList) {
            allAssets.add(channelInfo.getClientId());
        }
        for (String asset : assets) {
            assertTrue(allAssets.contains(asset));
        }

        // MessageQuery messageQuery = getBaseMessageQuery();
        //
        // AndPredicate andPredicate = new AndPredicateImpl();
        // TermPredicate accountNamePredicate = objectFactory.newTermPredicate(ClientInfoField.ACCOUNT, account.getName());
        // andPredicate.getPredicates().add(accountNamePredicate);
        // RangePredicate timestampPredicate = new RangePredicateImpl(ClientInfoField.TIMESTAMP, timestampLowerBound, timestampUpperBound);
        // andPredicate.getPredicates().add(timestampPredicate);
        // messageQuery.setPredicate(andPredicate);
        //
        // allAssets = new ArrayList<String>();
        //
        // MessageListResult result = messageStoreService.query(account.getScopeId(), messageQuery);
        // for (DatastoreMessage datastoreMessage : result) {
        // allAssets.add(datastoreMessage.getClientId());
        // }
        //
        // for (String asset : assets) {
        // assertTrue(allAssets.contains(asset));
        // }

        // check the message date
        for (ChannelInfo channelInfo : channelList) {
            if (assets[0].equals(channelInfo.getClientId())) {
                assertEquals(String.format("Wrong last publish date for the client id [%s]", assets[0]), capturedOn, channelInfo.getLastMessageTimestamp());
            }
            else if (assets[1].equals(channelInfo.getClientId())) {
                assertEquals(String.format("Wrong last publish date for the client id [%s]", assets[1]), capturedOnThirdMessage, channelInfo.getLastMessageTimestamp());
            }
        }
    }

    // ===========================================================
    // ===========================================================
    // utility methods
    // ===========================================================
    // ===========================================================

    /**
     * This method should create a new account for the test (temp implementation that return always the default kapua-sys account)
     * 
     * @param accountName
     * @param password
     * @return
     * @throws KapuaException
     */
    private Account createAccount(String accountName, String password) throws KapuaException
    {
        return getTestAccountCreator(adminScopeId);
    }

    /**
     * Creates a new KapuaMessage setting the provided parameters
     * 
     * @param clientId
     * @param scopeId
     * @param deviceId
     * @param receiedOn
     * @param capturedOn
     * @param sentOn
     * @return
     */
    private KapuaDataMessage getMessage(String clientId, KapuaId scopeId, KapuaId deviceId,
                                        Date receiedOn, Date capturedOn, Date sentOn)
    {
        KapuaDataMessage message = new KapuaDataMessageImpl();
        message.setReceivedOn(receiedOn);
        message.setCapturedOn(capturedOn);
        message.setSentOn(sentOn);
        message.setChannel(new KapuaDataChannelImpl());
        message.setClientId(clientId);
        message.setDeviceId(deviceId);
        message.setScopeId(scopeId);
        return message;
    }

    /**
     * Update the KapuaMessage channel with the provided semantic part
     * 
     * @param message
     * @param semanticPart
     */
    private void updateChannel(KapuaDataMessage message, String semanticPart)
    {
        message.setChannel(new KapuaDataChannelImpl());
        message.getChannel().setSemanticParts(new ArrayList<String>(Arrays.asList(semanticPart.split("/"))));
    }

    /**
     * Update the KapuaMessage payload with the provided payload
     * 
     * @param message
     * @param messagePayload
     */
    private void updatePayload(KapuaDataMessage message, KapuaDataPayload messagePayload)
    {
        message.setPayload(messagePayload);
    }

    /**
     * Return a new KapuaPosition instance and set all the provided position informations
     * 
     * @param altitude
     * @param heading
     * @param latitude
     * @param longitude
     * @param precision
     * @param satellites
     * @param speed
     * @param status
     * @param timestamp
     * @return
     */
    private KapuaPosition getPosition(Double altitude, Double heading, Double latitude, Double longitude, Double precision, Integer satellites, Double speed, Integer status, Date timestamp)
    {
        KapuaPosition messagePosition = new KapuaPositionImpl();
        messagePosition.setAltitude(altitude);
        messagePosition.setHeading(heading);
        messagePosition.setLatitude(latitude);
        messagePosition.setLongitude(longitude);
        messagePosition.setPrecision(precision);
        messagePosition.setSatellites(satellites);
        messagePosition.setSpeed(speed);
        messagePosition.setStatus(status);
        messagePosition.setTimestamp(timestamp);
        return messagePosition;
    }

    //
    // Utility methods to help to to create message queries
    //

    /**
     * Creates a new query setting the default base parameters (fetch style, sort, limit, offset, ...)
     * 
     * @return
     */
    private MessageQuery getBaseMessageQuery()
    {
        MessageQuery query = new MessageQueryImpl();
        query.setAskTotalCount(true);
        query.setFetchStyle(StorableFetchStyle.SOURCE_FULL);
        query.setLimit(10);
        query.setOffset(0);
        List<SortField> order = new ArrayList<SortField>();
        SortField sf = new SortFieldImpl();
        sf.setField(EsSchema.MESSAGE_TIMESTAMP);
        sf.setSortDirection(SortDirection.DESC);
        order.add(sf);
        query.setSortFields(order);
        return query;
    }
    
    /**
     * Creates a new query setting the default base parameters (fetch style, sort, limit, offset, ...)
     * 
     * @return
     */
    private ChannelInfoQuery getBaseChannelInfoQuery()
    {
        ChannelInfoQuery query = new ChannelInfoQueryImpl();
        query.setAskTotalCount(true);
        query.setFetchStyle(StorableFetchStyle.SOURCE_FULL);
        query.setLimit(10);
        query.setOffset(0);
        List<SortField> order = new ArrayList<SortField>();
        SortField sf = new SortFieldImpl();
        sf.setField(EsSchema.MESSAGE_TIMESTAMP);
        sf.setSortDirection(SortDirection.DESC);
        order.add(sf);
        query.setSortFields(order);
        return query;
    }

    /**
     * Get the ordered query (adding the sort fields list provided and the result limit count)
     * 
     * @param limit
     * @param order
     * @return
     */
    private MessageQuery getOrderedQuery(int limit, List<SortField> order)
    {
        MessageQuery query = new MessageQueryImpl();
        query.setAskTotalCount(true);
        query.setFetchStyle(StorableFetchStyle.SOURCE_FULL);
        query.setLimit(limit);
        query.setOffset(0);
        query.setSortFields(order);
        return query;
    }
    
    //
    // Utility methods to help to check the message result
    //
    /**
     * Check if in the result set has the expected messages count and return the first (if any)
     * 
     * @param result
     * @return
     */
    private DatastoreMessage checkMessagesCount(MessageListResult result, int messagesCount)
    {
        DatastoreMessage messageQueried = null;
        if (messagesCount > 0) {
            assertNotNull("No result found!", result);
            assertNotNull("No result found!", result.getTotalCount());
            assertEquals("Result message has a wrong size!", messagesCount, result.getTotalCount().intValue());
            messageQueried = result.get(0);
            assertNotNull("Result message is null!", messageQueried);
        }
        else {
            assertTrue("No result should be found!", result == null || result.getTotalCount() == null || result.getTotalCount() <= 0);

        }
        return messageQueried;
    }

    /**
     * Check if the queried message has the correct message id
     * 
     * @param message
     * @param storableId
     */
    private void checkMessageId(DatastoreMessage message, StorableId storableId)
    {
        if (storableId != null) {
            assertNotNull("Message id doesn't match", message.getId());
            assertEquals("Message id doesn't match", storableId.toString(), message.getId().toString());
        }
    }

    /**
     * Check if the queried message has the correct semantic part (the same topic part and the same length)
     * 
     * @param message
     * @param topicSemanticPart
     */
    private void checkTopic(DatastoreMessage message, String topicSemanticPart)
    {
        KapuaChannel channel = message.getChannel();
        assertNotNull("Null message channel!", channel);
        List<String> semanticParts = channel.getSemanticParts();
        assertNotNull("Null topic semantic part!", semanticParts);
        String[] topicSemanticPartTokenized = topicSemanticPart.split("/");
        assertEquals("Wrong semantic topic stored!", topicSemanticPartTokenized.length, semanticParts.size());
        int i = 0;
        for (String tmp : topicSemanticPartTokenized) {
            assertEquals(String.format("Wrong [%s] sematic part!", i), tmp, semanticParts.get(i++));
        }
    }

    /**
     * Check if the queried message has the correct dates (indexOn, sentOn, receivedOn, capturedOn). The dates can be checked also for a range.
     * 
     * @param message
     * @param index
     * @param sentOn
     * @param capturedOn
     * @param receivedOn
     */
    private void checkMessageDate(DatastoreMessage message, Range<Date> index, Range<Date> sentOn, Range<Date> capturedOn, Range<Date> receivedOn)
    {
        assertNotNull("Message timestamp is null!", message.getTimestamp());
        index.checkValue(message.getTimestamp());
        sentOn.checkValue(message.getSentOn());
        capturedOn.checkValue(message.getCapturedOn());
        receivedOn.checkValue(message.getReceivedOn());
        assertNotNull("Message payload is null!", message.getPayload());
        assertNotNull("Message prorperties are null!", message.getPayload().getProperties());
    }

    /**
     * Check if the queried message has the correct metrics size
     * 
     * @param message
     * @param metricsSize
     */
    private void checkMetricsSize(DatastoreMessage message, int metricsSize)
    {
        if (metricsSize < 0) {
            assertNull("Message metrics is not null!", message.getPayload().getProperties());
        }
        else {
            assertNotNull("Message metrics shouldn't be null!", message.getPayload().getProperties());
            assertEquals("Message metrics size doesn't match!", metricsSize, message.getPayload().getProperties().size());
        }
    }

    /**
     * Check if the queried message has the correct body (length and also content should be the same)
     * 
     * @param message
     * @param body
     */
    private void checkMessageBody(DatastoreMessage message, byte[] body)
    {
        if (body == null) {
            assertNull("Message body is not null!", message.getPayload().getBody());
        }
        else {
            assertNotNull("Message body shouldn't be null!", message.getPayload().getBody());
            assertEquals("Message body size doesn't match!", body.length, message.getPayload().getBody().length);
            assertArrayEquals("Message body differs from the original!", body, message.getPayload().getBody());
        }
    }

    /**
     * Check if the queried message has the correct metrics (metrics count and same keys/values)
     * 
     * @param message
     * @param metrics
     */
    private void checkMetrics(DatastoreMessage message, Map<String, Object> metrics)
    {
        // assuming metrics size is already checked by the checkMetricsSize
        Map<String, Object> messageProperties = message.getPayload().getProperties();
        Iterator<String> metricsKeys = metrics.keySet().iterator();
        while (metricsKeys.hasNext()) {
            String key = metricsKeys.next();
            assertEquals(String.format("Metric [%s] differs!", key), metrics.get(key), messageProperties.get(key));
        }
    }

    /**
     * Check if the queried message has the correct position
     * 
     * @param message
     * @param position
     */
    private void checkPosition(DatastoreMessage message, KapuaPosition position)
    {
        if (position == null) {
            assertNull("Message position is not null!", message.getPosition());
        }
        else {
            assertNotNull("Message position shouldn't be null!", message.getPosition());
            KapuaPosition messagePosition = message.getPosition();
            assertEquals("Altitude position differs from the original!", messagePosition.getAltitude(), position.getAltitude());
            assertEquals("Heading position differs from the original!", messagePosition.getHeading(), position.getHeading());
            assertEquals("Latitude position differs from the original!", messagePosition.getLatitude(), position.getLatitude());
            assertEquals("Longitude position differs from the original!", messagePosition.getLongitude(), position.getLongitude());
            assertEquals("Precision position differs from the original!", messagePosition.getPrecision(), position.getPrecision());
            assertEquals("Satellites position differs from the original!", messagePosition.getSatellites(), position.getSatellites());
            assertEquals("Speed position differs from the original!", messagePosition.getSpeed(), position.getSpeed());
            assertEquals("Status position differs from the original!", messagePosition.getStatus(), position.getStatus());
            assertEquals("Timestamp position differs from the original!", messagePosition.getTimestamp(), position.getTimestamp());
        }
    }

    /**
     * Check if in the result set has the expected messages count and return the first (if any)
     * 
     * @param result
     * @return
     */
    private ChannelInfo checkChannelInfoCount(ChannelInfoListResult result, int messagesCount)
    {
        ChannelInfo channelInfoQueried = null;
        if (messagesCount > 0) {
            assertNotNull("No result found!", result);
            assertNotNull("No result found!", result.getTotalCount());
            assertEquals("Result channel has a wrong size!", messagesCount, result.getTotalCount().intValue());
            channelInfoQueried = result.get(0);
            assertNotNull("Result channel is null!", channelInfoQueried);
        }
        else {
            assertTrue("No result should be found!", result == null || result.getTotalCount() == null || result.getTotalCount() <= 0);

        }
        return channelInfoQueried;
    }

    /**
     * Check if the message result list is correctly ordered by the provided criteria (list of fields and ordering)
     * 
     * @param result
     * @param sortFieldList
     */
    @SuppressWarnings("rawtypes")
    private void checkListOrder(MessageListResult result, List<SortField> sortFieldList)
    {
        DatastoreMessage previousMessage = null;
        for (DatastoreMessage message : result) {
            for (SortField field : sortFieldList) {
                String normalizedFieldName = getNormalizedFieldName(field);
                if (previousMessage != null) {
                    Comparable currentValue = getValue(message, normalizedFieldName);
                    Comparable previousValue = getValue(previousMessage, normalizedFieldName);
                    if (!currentValue.equals(previousValue)) {
                        checkNextValueCoherence(field, currentValue, previousValue);
                        // proceed with next message
                        break;
                    }
                }
                else {
                    break;
                }
            }
            previousMessage = message;
        }
    }

    /**
     * Normalize the field name to compose (in a different method) the getter name.
     * It removes the _ and append the remaining part capitalizing the first letter
     * 
     * @param field
     * @return
     */
    private String getNormalizedFieldName(SortField field) {
        String str[] = field.getField().split("_");
        if (str==null || str.length<=0) {
            throw new IllegalArgumentException(String.format("Invalid field name [%s]", field.getField()));
        }
        String fieldName = str[0];
        for (int i=1; i<str.length; i++) {
            fieldName += str[i].substring(0, 1).toUpperCase() + str[i].substring(1);
        }
        return fieldName;
    }

    /**
     * Check if the next value (it must be not equals, so the equals condition must be checked before calling this method) is coherent with its ordering criteria
     * 
     * @param field
     * @param currentValue
     * @param previousValue
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void checkNextValueCoherence(SortField field, Comparable currentValue, Comparable previousValue)
    {
        if (SortDirection.ASC.equals(field.getSortDirection())) {
            assertTrue(String.format("The field [%s] is not correctly ordered as [%s]!", field.getField(), field.getSortDirection()), currentValue.compareTo(previousValue) > 0);
        }
        else {
            assertTrue(String.format("The field [%s] is not correctly ordered as [%s]!", field.getField(), field.getSortDirection()), currentValue.compareTo(previousValue) < 0);
        }
    }

    /**
     * Return the value of the field name provided (assuming that this value is a Comparable)
     * 
     * @param message
     * @param field
     * @return
     */
    @SuppressWarnings("rawtypes")
    private Comparable getValue(DatastoreMessage message, String field)
    {
        try {
            Class dataStoreClass = DatastoreMessage.class;
            Method getMethod = getMethod(dataStoreClass, field, "get");
            if (getMethod != null) {
                return (Comparable) getMethod.invoke(message, new Object[0]);
            }
            getMethod = getMethod(dataStoreClass, field, "is");
            if (getMethod != null) {
                return (Comparable) getMethod.invoke(message, new Object[0]);
            }
            else {
                throw new IllegalArgumentException(String.format("Cannot find getter for field [%s] or the field value is not a Comparable value!", field));
            }
        }
        catch (Exception e) {
            throw new IllegalArgumentException(String.format("Cannot find getter for field [%s] or the field value is not a Comparable value!", field));
        }
    }

    /**
     * Return the method combining the prefix and the field name provided
     * 
     * @param dataStoreClass
     * @param field
     * @param prefix
     * @return
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private Method getMethod(Class dataStoreClass, String field, String prefix)
    {
        String fieldName = prefix + field.substring(0, 1).toUpperCase() + field.substring(1);
        try {
            return dataStoreClass.getMethod(fieldName, new Class[0]);
        }
        catch (NoSuchMethodException e) {
            return null;
        }
    }

    //
    // Configuration utility
    //
    /**
     * Update the store service configuration with the provided values
     * 
     * @param messageStoreService
     * @param scopeId
     * @param dataIndexBy
     * @param metricsIndexBy
     * @param dataTTL
     * @param storageEnabled
     * @throws KapuaException
     */
    private void updateConfiguration(MessageStoreService messageStoreService, KapuaId scopeId, DataIndexBy dataIndexBy, MetricsIndexBy metricsIndexBy, int dataTTL, boolean storageEnabled) throws KapuaException
    {
        Map<String, Object> config = messageStoreService.getConfigValues(scopeId);
        if (config == null) {
            config = new HashMap<String, Object>();
        }
        if (dataIndexBy != null) {
            config.put(MessageStoreConfiguration.CONFIGURATION_DATA_INDEX_BY_KEY, dataIndexBy.name());
        }
        if (metricsIndexBy != null) {
            config.put(MessageStoreConfiguration.CONFIGURATION_METRICS_INDEX_BY_KEY, metricsIndexBy.name());
        }
        config.put(MessageStoreConfiguration.CONFIGURATION_DATA_TTL_KEY, dataTTL);
        config.put(MessageStoreConfiguration.CONFIGURATION_DATA_STORAGE_ENABLED_KEY, storageEnabled);
        messageStoreService.setConfigValues(scopeId, config);
    }

    /**
     * Utility class to check the correctness value (exact value or range depending on the constructor used)
     *
     * @param <O>
     */
    private class Range<O extends Comparable<O>>
    {

        private String field;
        private O      min;
        private O      max;

        private Range(String field, O min, O max)
        {
            this(field, min);
            this.max = max;
        }

        private Range(String field, O exactValue)
        {
            assertNotNull("The lower bound or the exact value to compare cannot be null!", exactValue);
            this.field = field;
            this.min = exactValue;
        }

        public void checkValue(O value)
        {
            if (max == null) {
                assertEquals("Expected value for " + field + " doesn't match!", min, value);
            }
            else {
                assertTrue("Expected value for " + field + " doesn't match the lower bound", min.compareTo(value) <= 0);
                assertTrue("Expected value for " + field + " doesn't match the upper bound", max.compareTo(value) >= 0);
            }
        }

    }

    @Test
    /**
     * Base test, may be removed since we should already have tests that covers also this one (to be check!)
     * 
     * @throws Exception
     */
    public void testStore()
        throws Exception
    {
        Account account = getTestAccountCreator(adminScopeId);

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
        message.setClientId(clientId);

        MessageStoreService messageStoreService = KapuaLocator.getInstance().getService(MessageStoreService.class);
        StorableId messageId = messageStoreService.store(message);

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

    /**
     * Return a new account created just for the test.<br>
     * <b>WARNING!!!!!!! Current implementation is not compliance with that since it is a temporary implementation that returns the default kapua-sys account</b>
     * 
     * @param scopeId
     * @return
     * @throws KapuaException
     */
    private Account getTestAccountCreator(KapuaId scopeId) throws KapuaException
    {
        KapuaLocator locator = KapuaLocator.getInstance();
        Account account = locator.getService(AccountService.class).findByName("kapua-sys");
        return account;
    }

}
