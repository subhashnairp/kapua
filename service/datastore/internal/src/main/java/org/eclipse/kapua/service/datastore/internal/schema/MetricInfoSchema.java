package org.eclipse.kapua.service.datastore.internal.schema;

import org.eclipse.kapua.service.datastore.client.DatamodelMappingException;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class MetricInfoSchema {

    /**
     * Metric information schema name
     */
    public final static String METRIC_TYPE_NAME = "metric";
    /**
     * Metric information - channel
     */
    public final static String METRIC_CHANNEL = "channel";
    /**
     * Metric information - client identifier
     */
    public final static String METRIC_CLIENT_ID = "client_id";
    /**
     * Metric information - account name
     */
    public final static String METRIC_ACCOUNT = "account";
    /**
     * Metric information - metric map prefix
     */
    public final static String METRIC_MTR = "metric";
    /**
     * Metric information - name
     */
    public final static String METRIC_MTR_NAME = "name";
    /**
     * Metric information - full name (so with the metric type suffix)
     */
    public final static String METRIC_MTR_NAME_FULL = "metric.name";
    /**
     * Metric information - type
     */
    public final static String METRIC_MTR_TYPE = "type";
    /**
     * Metric information - full type (so with the metric type suffix)
     */
    public final static String METRIC_MTR_TYPE_FULL = "metric.type";
    /**
     * Metric information - value
     */
    public final static String METRIC_MTR_VALUE = "value";
    /**
     * Metric information - full value (so with the metric type suffix)
     */
    public final static String METRIC_MTR_VALUE_FULL = "metric.value";
    /**
     * Metric information - message timestamp (of the first message published in this channel)
     */
    public final static String METRIC_MTR_TIMESTAMP = "timestamp";
    /**
     * Metric information - message timestamp (of the first message published in this channel, with the metric type suffix)
     */
    public final static String METRIC_MTR_TIMESTAMP_FULL = "metric.timestamp";
    /**
     * Metric information - message identifier (of the first message published in this channel)
     */
    public final static String METRIC_MTR_MSG_ID = "message_id";
    /**
     * Metric information - full message identifier (of the first message published in this channel, with the metric type suffix)
     */
    public final static String METRIC_MTR_MSG_ID_FULL = "metric.message_id";

    public static ObjectNode getMetricTypeBuilder(boolean allEnable, boolean sourceEnable) throws DatamodelMappingException {
        ObjectNode rootNode = SchemaUtil.getObjectNode();

        ObjectNode metricName = SchemaUtil.getObjectNode();
        ObjectNode sourceMetric = SchemaUtil.getField(new String[] { "enabled" }, new Object[] { sourceEnable });
        metricName.set("_source", sourceMetric);

        ObjectNode allMetric = SchemaUtil.getField(new String[] { "enabled" }, new Object[] { allEnable });
        metricName.set("_all", allMetric);

        ObjectNode propertiesNode = SchemaUtil.getObjectNode();
        ObjectNode metricAccount = SchemaUtil.getField(new String[] { "type", "index" }, new String[] { "string", "not_analyzed" });
        propertiesNode.set(METRIC_ACCOUNT, metricAccount);
        ObjectNode metricClientId = SchemaUtil.getField(new String[] { "type", "index" }, new String[] { "string", "not_analyzed" });
        propertiesNode.set(METRIC_CLIENT_ID, metricClientId);
        ObjectNode metricChannel = SchemaUtil.getField(new String[] { "type", "index" }, new String[] { "string", "not_analyzed" });
        propertiesNode.set(METRIC_CHANNEL, metricChannel);

        ObjectNode metricMtrNode = SchemaUtil.getField(new String[] { "type", "enabled", "dynamic", "include_in_all" },
                new Object[] { "object", true, false, false });
        ObjectNode metricMtrPropertiesNode = SchemaUtil.getObjectNode();
        ObjectNode metricMtrNameNode = SchemaUtil.getField(new String[] { "type", "index" }, new String[] { "string", "not_analyzed" });
        metricMtrPropertiesNode.set(METRIC_MTR_NAME, metricMtrNameNode);
        ObjectNode metricMtrTypeNode = SchemaUtil.getField(new String[] { "type", "index" }, new String[] { "string", "not_analyzed" });
        metricMtrPropertiesNode.set(METRIC_MTR_TYPE, metricMtrTypeNode);
        ObjectNode metricMtrValueNode = SchemaUtil.getField(new String[] { "type", "index" }, new String[] { "string", "not_analyzed" });
        metricMtrPropertiesNode.set(METRIC_MTR_VALUE, metricMtrValueNode);
        ObjectNode metricMtrTimestampNode = SchemaUtil.getField(new String[] { "type" }, new String[] { "date" });
        metricMtrPropertiesNode.set(METRIC_MTR_TIMESTAMP, metricMtrTimestampNode);
        ObjectNode metricMtrMsgIdNode = SchemaUtil.getField(new String[] { "type", "index" }, new String[] { "string", "not_analyzed" });
        metricMtrPropertiesNode.set(METRIC_MTR_MSG_ID, metricMtrMsgIdNode);
        metricMtrNode.set("properties", metricMtrPropertiesNode);
        propertiesNode.set(METRIC_MTR, metricMtrNode);

        metricName.set("properties", propertiesNode);

        rootNode.set(METRIC_TYPE_NAME, metricName);
        return rootNode;
    }

}
