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
package org.eclipse.kapua.service.datastore.internal.schema;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

import org.eclipse.kapua.commons.util.KapuaDateUtils;
import org.eclipse.kapua.model.id.KapuaId;
import org.eclipse.kapua.service.datastore.client.ClientException;
import org.eclipse.kapua.service.datastore.client.ClientUnavailableException;
import org.eclipse.kapua.service.datastore.client.DatamodelMappingException;
import org.eclipse.kapua.service.datastore.client.Client;
import org.eclipse.kapua.service.datastore.client.IndexExistsRequest;
import org.eclipse.kapua.service.datastore.client.IndexExistsResponse;
import org.eclipse.kapua.service.datastore.client.TypeDescriptor;
import org.eclipse.kapua.service.datastore.internal.client.ClientFactory;
import org.eclipse.kapua.service.datastore.internal.mediator.DatastoreUtils;
import org.eclipse.kapua.service.datastore.internal.mediator.Metric;
import org.eclipse.kapua.service.datastore.internal.setting.DatastoreSettingKey;
import org.eclipse.kapua.service.datastore.internal.setting.DatastoreSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Datastore schema utilities
 * 
 * @since 1.0
 *
 */
public class Schema {

    private static final Logger logger = LoggerFactory.getLogger(Schema.class);

    private Map<String, Metadata> schemaCache;
    private Object schemaCacheSync;
    private Object mappingsSync;

    private Client client;

    /**
     * Construct the Elasticsearch schema
     * 
     */
    public Schema() {
        schemaCache = new HashMap<String, Metadata>();
        schemaCacheSync = new Object();
        mappingsSync = new Object();
        try {
            client = ClientFactory.getInstance();// TODO check how to handle the exception
        } catch (ClientUnavailableException e) {
            throw new RuntimeException("Cannot get client instance", e);
        }
    }

    /**
     * Synchronize metadata
     * 
     * @param scopeId
     * @param time
     * @return
     * @throws IOException
     * @throws ClientException
     */
    public Metadata synch(KapuaId scopeId, long time)
            throws ClientException {
        String scopeIdShort = scopeId.toCompactId();
        String dataIndexName = DatastoreUtils.getDataIndexName(scopeIdShort, time);

        synchronized (schemaCacheSync) {
            if (schemaCache.containsKey(dataIndexName)) {
                Metadata currentMetadata = schemaCache.get(dataIndexName);
                return currentMetadata;
            }
        }

        logger.info("Before entering updating metadata");

        Metadata currentMetadata = null;
        synchronized (mappingsSync) {
            logger.info("Entered updating metadata");

            // Check existence of the data index
            IndexExistsResponse dataIndexExistsResponse = client.isIndexExists(new IndexExistsRequest(dataIndexName));
            if (!dataIndexExistsResponse.isIndexExists()) {
                client.createIndex(dataIndexName, getMappingSchema());
                logger.info("Data index created: " + dataIndexName);
            }

            boolean enableAllField = false;
            boolean enableSourceField = true;

            client.putMapping(new TypeDescriptor(dataIndexName, MessageSchema.MESSAGE_TYPE_NAME), MessageSchema.getMesageTypeBuilder(enableAllField, enableSourceField));
            // Check existence of the kapua internal index
            String registryIndexName = DatastoreUtils.getRegistryIndexName(scopeIdShort);
            IndexExistsResponse registryIndexExistsResponse = client.isIndexExists(new IndexExistsRequest(registryIndexName));
            if (!registryIndexExistsResponse.isIndexExists()) {
                client.createIndex(registryIndexName, getMappingSchema());
                logger.info("Metadata index created: " + registryIndexExistsResponse);

                client.putMapping(new TypeDescriptor(registryIndexName, ChannelInfoSchema.CHANNEL_TYPE_NAME), ChannelInfoSchema.getChannelTypeBuilder(enableAllField, enableSourceField));
                client.putMapping(new TypeDescriptor(registryIndexName, MetricInfoSchema.METRIC_TYPE_NAME), MetricInfoSchema.getMetricTypeBuilder(enableAllField, enableSourceField));
                client.putMapping(new TypeDescriptor(registryIndexName, ClientInfoSchema.CLIENT_TYPE_NAME), ClientInfoSchema.getClientTypeBuilder(enableAllField, enableSourceField));
            }

            currentMetadata = new Metadata(dataIndexName, registryIndexName);
            logger.info("Leaving updating metadata");
        }

        synchronized (schemaCacheSync) {
            // Current metadata can only increase the custom mappings
            // other fields does not change within the same account id
            // and custom mappings are not and must not be exposed to
            // outside this class to preserve thread safetyness
            schemaCache.put(dataIndexName, currentMetadata);
        }

        return currentMetadata;
    }

    /**
     * Update metric mappings
     * 
     * @param scopeId
     * @param time
     * @param esMetrics
     * @throws ClientException
     */
    public void updateMessageMappings(KapuaId scopeId, long time, Map<String, Metric> metrics)
            throws ClientException {
        if (metrics == null || metrics.size() == 0) {
            return;
        }
        Metadata currentMetadata = null;
        synchronized (schemaCacheSync) {
            String scopeIdShort = scopeId.toCompactId();
            String newIndex = DatastoreUtils.getDataIndexName(scopeIdShort, time);
            currentMetadata = schemaCache.get(newIndex);
        }

        ObjectNode metricsMapping = null;
        Map<String, Metric> diffs = null;

        synchronized (mappingsSync) {
            // Update mappings only if a metric is new (not in cache)
            diffs = getMessageMappingDiffs(currentMetadata, metrics);
            if (diffs == null || diffs.size() == 0) {
                return;
            }
            metricsMapping = getNewMessageMappingsBuilder(diffs);
        }

        logger.trace("Sending dynamic message mappings: " + metricsMapping);
        client.putMapping(new TypeDescriptor(currentMetadata.getDataIndexName(), MessageSchema.MESSAGE_TYPE_NAME), metricsMapping);
    }

    // REVIEW WITH STEFANO!!!!! TODO
    private ObjectNode getNewMessageMappingsBuilder(Map<String, Metric> esMetrics) throws DatamodelMappingException {
        final int METRIC_TERM = 0;
        if (esMetrics == null) {
            return null;
        }
        // It is assumed the mappings (key values) are all of the type
        // metrics.metric_name.type
        ObjectNode rootNode = SchemaUtil.getObjectNode();
        ObjectNode messageTypeNode = SchemaUtil.getObjectNode();// MESSAGE_TYPE_NAME
        ObjectNode propertiesRootNode = SchemaUtil.getObjectNode();// properties
        ObjectNode metricsNode = SchemaUtil.getObjectNode();// Schema.MESSAGE_METRICS
        ObjectNode propertiesNode = SchemaUtil.getObjectNode();// propertiesNode

        // XContentBuilder builder = XContentFactory.jsonBuilder()
        // .startObject() // rootNode
        // .startObject(MessageSchema.MESSAGE_TYPE_NAME) // messageTypeNode
        // .startObject("properties") // propertiesRootNode
        // .startObject(MessageSchema.MESSAGE_METRICS) // metricsNode
        // .startObject("properties"); // propertiesNode

            // TODO precondition for the loop: there are no two consecutive mappings for the same field with
            // two different types (field are all different)

        String[] prevKeySplit = new String[] { "", "" };
        Set<String> keys = esMetrics.keySet();
        String metricName = null;
        for (String key : keys) {

            Metric metric = esMetrics.get(key);
            String[] keySplit = key.split(Pattern.quote("."));

            if (!keySplit[METRIC_TERM].equals(prevKeySplit[METRIC_TERM])) {
                if (!prevKeySplit[METRIC_TERM].isEmpty()) {
                    metricsNode.set("properties", propertiesNode);
                    propertiesRootNode.set(MessageSchema.MESSAGE_METRICS, metricsNode);
                    // builder.endObject(); // Previously open properties section
                    // builder.endObject(); // Previously open metric-object section
                }
                metricsNode = SchemaUtil.getObjectNode();
                propertiesRootNode = SchemaUtil.getObjectNode();
                metricName = metric.getName();
                // builder.startObject(metric.getName()); // Start new metric object
                // builder.startObject("properties"); // Start new object properties section
            }

            if (!keySplit[METRIC_TERM].equals(prevKeySplit[METRIC_TERM])) {

                ObjectNode metricMapping = SchemaUtil.getObjectNode();
                ObjectNode metricMappingContent = null;
                if (metric.getType().equals("string")) {
                    metricMappingContent = SchemaUtil.getField(
                            new String[] { "type", "index" },
                            new String[] { metric.getType(), "not_analyzed" });
                } else {
                    metricMappingContent = SchemaUtil.getField(
                            new String[] { "type" },
                            new String[] { metric.getType() });
                }
                metricMapping.set(DatastoreUtils.getClientTypeAcronym(metric.getType()), metricMappingContent);
                propertiesNode.set("properties", metricMapping);
                // builder.startObject(DatastoreUtils.getClientTypeAcronym(metric.getType()));
                // builder.field("type", metric.getType());
                // if (metric.getType().equals("string"))
                // builder.field("index", "not_analyzed");
                // builder.endObject();
            }

            prevKeySplit = keySplit;
        }

        if (keys.size() > 0) {
            if (!prevKeySplit[METRIC_TERM].isEmpty()) {
                metricsNode.set("properties", propertiesNode);
                propertiesRootNode.set(metricName, metricsNode);
                // builder.endObject(); // Previously open properties section
                // builder.endObject(); // Previously open metrics-object section
            }
        }

        metricsNode.set("properties", propertiesNode); // Properties
        propertiesRootNode.set(MessageSchema.MESSAGE_METRICS, metricsNode); // Metrics
        messageTypeNode.set("properties", propertiesRootNode);// Properties
        rootNode.set(MessageSchema.MESSAGE_TYPE_NAME, messageTypeNode);// Type
        // builder.endObject() // Properties
        // .endObject() // Metrics
        // .endObject() // Properties
        // .endObject() // Type
        // .endObject(); // Root

        return rootNode;
    }

    // private XContentBuilder ciccio(Map<String, Metric> esMetrics) {
    // final int METRIC_TERM = 0;
    // // final int TYPE_TERM = 1;
    //
    // if (esMetrics == null)
    // return null;
    //
    // // It is assumed the mappings (key values) are all of the type
    // // metrics.metric_name.type
    // XContentBuilder builder = XContentFactory.jsonBuilder()
    // .startObject()
    // .startObject(MESSAGE_TYPE_NAME)
    // .startObject("properties")
    // .startObject(Schema.MESSAGE_METRICS)
    // .startObject("properties");
    //
    // // TODO precondition for the loop: there are no two consecutive mappings for the same field with
    // // two different types (field are all different)
    //
    // String[] prevKeySplit = new String[] { "", "" };
    // Set<String> keys = esMetrics.keySet();
    // for (String key : keys) {
    //
    // Metric metric = esMetrics.get(key);
    // String[] keySplit = key.split(Pattern.quote("."));
    //
    // if (!keySplit[METRIC_TERM].equals(prevKeySplit[METRIC_TERM])) {
    // if (!prevKeySplit[METRIC_TERM].isEmpty()) {
    // builder.endObject(); // Previously open properties section
    // builder.endObject(); // Previously open metric-object section
    // }
    // builder.startObject(metric.getName()); // Start new metric object
    // builder.startObject("properties"); // Start new object properties section
    // }
    //
    // if (!keySplit[METRIC_TERM].equals(prevKeySplit[METRIC_TERM])) {
    //
    // builder.startObject(DatastoreUtils.getClientTypeAcronym(metric.getType()));
    // builder.field("type", metric.getType());
    // if (metric.getType().equals("string"))
    // builder.field("index", "not_analyzed");
    // builder.endObject();
    // }
    //
    // prevKeySplit = keySplit;
    // }
    //
    // if (keys.size() > 0) {
    // if (!prevKeySplit[METRIC_TERM].isEmpty()) {
    // builder.endObject(); // Previously open properties section
    // builder.endObject(); // Previously open metrics-object section
    // }
    // }
    //
    // builder.endObject() // Properties
    // .endObject() // Metrics
    // .endObject() // Properties
    // .endObject() // Type
    // .endObject(); // Root
    //
    // return builder;
    // }

    private Map<String, Metric> getMessageMappingDiffs(Metadata currentMetadata, Map<String, Metric> esMetrics) {

        if (esMetrics == null || esMetrics.size() == 0)
            return null;

        Entry<String, Metric> el;
        Map<String, Metric> diffs = null;
        Iterator<Entry<String, Metric>> iter = esMetrics.entrySet().iterator();
        while (iter.hasNext()) {

            el = iter.next();
            if (!currentMetadata.getMessageMappingsCache().containsKey(el.getKey())) {

                if (diffs == null)
                    diffs = new HashMap<String, Metric>(100);

                currentMetadata.getMessageMappingsCache().put(el.getKey(), el.getValue());
                diffs.put(el.getKey(), el.getValue());
            }
        }

        return diffs;
    }

    private ObjectNode getMappingSchema() throws DatamodelMappingException {
        String idxRefreshInterval = String.format("%ss", DatastoreSettings.getInstance().getLong(DatastoreSettingKey.INDEX_REFRESH_INTERVAL) * KapuaDateUtils.SEC_MILLIS);
        ObjectNode rootNode = SchemaUtil.getObjectNode();
        ObjectNode refreshIntervaleNode = SchemaUtil.getField(new String[] { "refresh_interval" }, new String[] { idxRefreshInterval });
        rootNode.set("index", refreshIntervaleNode);
        // XContentBuilder builder = XContentFactory.jsonBuilder()
        // .startObject()
        // .startObject("index")
        // .field("refresh_interval", idxRefreshInterval)
        // .endObject()
        // .endObject();

        return rootNode;
    }

}
