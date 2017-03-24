package org.eclipse.kapua.service.datastore.internal.schema;

import org.eclipse.kapua.service.datastore.client.DatamodelMappingException;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class MessageSchema {

    /**
     * Message schema name
     */
    public final static String MESSAGE_TYPE_NAME = "message";
    /**
     * Message timestamp
     */
    public final static String MESSAGE_TIMESTAMP = "timestamp";
    /**
     * Message received on timestamp
     */
    public final static String MESSAGE_RECEIVED_ON = "received_on";
    /**
     * Message received by address
     */
    public final static String MESSAGE_IP_ADDRESS = "ip_address";
    /**
     * Message account identifier
     */
    public final static String MESSAGE_ACCOUNT_ID = "account_id";
    /**
     * Message account name
     */
    public final static String MESSAGE_ACCOUNT = "account";// TODO TO BE REMOVED
    /**
     * Message device identifier
     */
    public final static String MESSAGE_DEVICE_ID = "device_id";
    /**
     * Message client identifier
     */
    public final static String MESSAGE_CLIENT_ID = "client_id";
    /**
     * Message channel
     */
    public final static String MESSAGE_CHANNEL = "channel";
    /**
     * Message channel parts
     */
    public final static String MESSAGE_CHANNEL_PARTS = "channel_parts";
    /**
     * Message captured on timestamp
     */
    public final static String MESSAGE_CAPTURED_ON = "captured_on";
    /**
     * Message sent on timestamp
     */
    public final static String MESSAGE_SENT_ON = "sent_on";
    /**
     * Message position - (composed object)
     */
    public final static String MESSAGE_POSITION = "position";
    /**
     * Message position - location (field name relative to the position object)
     */
    public final static String MESSAGE_POS_LOCATION = "location";
    /**
     * Message position - location (full field name)
     */
    public final static String MESSAGE_POS_LOCATION_FULL = "position.location";
    /**
     * Message position - altitude (field name relative to the position object)
     */
    public final static String MESSAGE_POS_ALT = "alt";
    /**
     * Message position - altitude (full field name)
     */
    public final static String MESSAGE_POS_ALT_FULL = "position.alt";
    /**
     * Message position - precision (field name relative to the position object)
     */
    public final static String MESSAGE_POS_PRECISION = "precision";
    /**
     * Message position - precision (full field name)
     */
    public final static String MESSAGE_POS_PRECISION_FULL = "position.precision";
    /**
     * Message position - heading (field name relative to the position object)
     */
    public final static String MESSAGE_POS_HEADING = "heading";
    /**
     * Message position - heading (full field name)
     */
    public final static String MESSAGE_POS_HEADING_FULL = "position.heading";
    /**
     * Message position - speed (field name relative to the position object)
     */
    public final static String MESSAGE_POS_SPEED = "speed";
    /**
     * Message position - speed (full field name)
     */
    public final static String MESSAGE_POS_SPEED_FULL = "position.speed";
    /**
     * Message position - timestamp (field name relative to the position object)
     */
    public final static String MESSAGE_POS_TIMESTAMP = "timestamp";
    /**
     * Message position - timestamp (full field name)
     */
    public final static String MESSAGE_POS_TIMESTAMP_FULL = "position.timestamp";
    /**
     * Message position - satellites (field name relative to the position object)
     */
    public final static String MESSAGE_POS_SATELLITES = "satellites";
    /**
     * Message position - satellites (full field name)
     */
    public final static String MESSAGE_POS_SATELLITES_FULL = "position.satellites";
    /**
     * Message position - status (field name relative to the position object)
     */
    public final static String MESSAGE_POS_STATUS = "status";
    /**
     * Message position - status (full field name)
     */
    public final static String MESSAGE_POS_STATUS_FULL = "position.status";
    /**
     * Message metrics
     */
    public final static String MESSAGE_METRICS = "metrics";
    /**
     * Message body
     */
    public final static String MESSAGE_BODY = "body";

    public static ObjectNode getMesageTypeBuilder(boolean allEnable, boolean sourceEnable) throws DatamodelMappingException {
        ObjectNode rootNode = SchemaUtil.getObjectNode();

        ObjectNode messageNode = SchemaUtil.getObjectNode();
        ObjectNode sourceMessage = SchemaUtil.getField(new String[] { "enabled" }, new Object[] { sourceEnable });
        messageNode.set("_source", sourceMessage);

        ObjectNode allMessage = SchemaUtil.getField(new String[] { "enabled" }, new Object[] { allEnable });
        messageNode.set("_all", allMessage);

        ObjectNode propertiesNode = SchemaUtil.getObjectNode();
        ObjectNode messageTimestamp = SchemaUtil.getField(new String[] { "type" }, new String[] { "date" });
        propertiesNode.set(MESSAGE_TIMESTAMP, messageTimestamp);
        ObjectNode messageReceivedOn = SchemaUtil.getField(new String[] { "type" }, new String[] { "date" });
        propertiesNode.set(MESSAGE_RECEIVED_ON, messageReceivedOn);
        ObjectNode messageIp = SchemaUtil.getField(new String[] { "type" }, new String[] { "ip" });
        propertiesNode.set(MESSAGE_IP_ADDRESS, messageIp);
        ObjectNode messageAccountId = SchemaUtil.getField(new String[] { "type", "index" }, new String[] { "string", "not_analyzed" });
        propertiesNode.set(MESSAGE_ACCOUNT_ID, messageAccountId);
        ObjectNode messageAccount = SchemaUtil.getField(new String[] { "type", "index" }, new String[] { "string", "not_analyzed" });
        propertiesNode.set(MESSAGE_ACCOUNT, messageAccount);
        ObjectNode messageDeviceId = SchemaUtil.getField(new String[] { "type", "index" }, new String[] { "string", "not_analyzed" });
        propertiesNode.set(MESSAGE_DEVICE_ID, messageDeviceId);
        ObjectNode messageClientId = SchemaUtil.getField(new String[] { "type", "index" }, new String[] { "string", "not_analyzed" });
        propertiesNode.set(MESSAGE_CLIENT_ID, messageClientId);
        ObjectNode messageChannel = SchemaUtil.getField(new String[] { "type", "index" }, new String[] { "string", "not_analyzed" });
        propertiesNode.set(MESSAGE_CHANNEL, messageChannel);
        ObjectNode messageCapturedOn = SchemaUtil.getField(new String[] { "type" }, new String[] { "date" });
        propertiesNode.set(MESSAGE_CAPTURED_ON, messageCapturedOn);
        ObjectNode messageSentOn = SchemaUtil.getField(new String[] { "type" }, new String[] { "date" });
        propertiesNode.set(MESSAGE_SENT_ON, messageSentOn);

        ObjectNode positionNode = SchemaUtil.getField(new String[] { "type", "enabled", "dynamic", "include_in_all" }, new Object[] { "object", true, false, false });

        ObjectNode positionPropertiesNode = SchemaUtil.getObjectNode();
        ObjectNode messagePositionPropLocation = SchemaUtil.getField(new String[] { "type" }, new String[] { "geo_point" });
        positionPropertiesNode.set(MESSAGE_POS_LOCATION, messagePositionPropLocation);
        ObjectNode messagePositionPropAlt = SchemaUtil.getField(new String[] { "type" }, new String[] { "double" });
        positionPropertiesNode.set(MESSAGE_POS_ALT, messagePositionPropAlt);
        ObjectNode messagePositionPropPrec = SchemaUtil.getField(new String[] { "type" }, new String[] { "double" });
        positionPropertiesNode.set(MESSAGE_POS_PRECISION, messagePositionPropPrec);
        ObjectNode messagePositionPropHead = SchemaUtil.getField(new String[] { "type" }, new String[] { "double" });
        positionPropertiesNode.set(MESSAGE_POS_HEADING, messagePositionPropHead);
        ObjectNode messagePositionPropSpeed = SchemaUtil.getField(new String[] { "type" }, new String[] { "double" });
        positionPropertiesNode.set(MESSAGE_POS_SPEED, messagePositionPropSpeed);
        ObjectNode messagePositionPropTime = SchemaUtil.getField(new String[] { "type" }, new String[] { "date" });
        positionPropertiesNode.set(MESSAGE_POS_TIMESTAMP, messagePositionPropTime);
        ObjectNode messagePositionPropSat = SchemaUtil.getField(new String[] { "type" }, new String[] { "integer" });
        positionPropertiesNode.set(MESSAGE_POS_SATELLITES, messagePositionPropSat);
        ObjectNode messagePositionPropStat = SchemaUtil.getField(new String[] { "type" }, new String[] { "integer" });
        positionPropertiesNode.set(MESSAGE_POS_STATUS, messagePositionPropStat);
        positionNode.set("properties", positionPropertiesNode);
        propertiesNode.set("position", positionNode);
        messageNode.set("properties", propertiesNode);

        ObjectNode messageMetrics = SchemaUtil.getField(new String[] { "type", "enabled", "dynamic", "include_in_all" },
                new Object[] { "object", true, true, false });
        propertiesNode.set(MESSAGE_METRICS, messageMetrics);

        ObjectNode messageBody = SchemaUtil.getField(new String[] { "type", "index" }, new String[] { "binary", "no" });
        propertiesNode.set(MESSAGE_BODY, messageBody);

        rootNode.set(MESSAGE_TYPE_NAME, messageNode);
        return rootNode;
    }

}
