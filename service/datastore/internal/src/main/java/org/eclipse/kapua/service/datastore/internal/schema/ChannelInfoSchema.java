package org.eclipse.kapua.service.datastore.internal.schema;

import org.eclipse.kapua.service.datastore.client.DatamodelMappingException;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class ChannelInfoSchema {

    /**
     * Channel information schema name
     */
    public final static String CHANNEL_TYPE_NAME = "channel";
    /**
     * Channel information - channel
     */
    public final static String CHANNEL_NAME = "channel";
    /**
     * Channel information - client identifier
     */
    public final static String CHANNEL_CLIENT_ID = "client_id";
    /**
     * Channel information - account
     */
    public final static String CHANNEL_ACCOUNT = "account";
    /**
     * Channel information - message timestamp (of the first message published in this channel)
     */
    public final static String CHANNEL_TIMESTAMP = "timestamp";
    /**
     * Channel information - message identifier (of the first message published in this channel)
     */
    public final static String CHANNEL_MESSAGE_ID = "message_id";

    public static ObjectNode getChannelTypeBuilder(boolean allEnable, boolean sourceEnable) throws DatamodelMappingException {
        ObjectNode rootNode = SchemaUtil.getObjectNode();

        ObjectNode channelNode = SchemaUtil.getObjectNode();
        ObjectNode sourceChannel = SchemaUtil.getField(new String[] { "enabled" }, new Object[] { sourceEnable });
        channelNode.set("_source", sourceChannel);

        ObjectNode allChannel = SchemaUtil.getField(new String[] { "enabled" }, new Object[] { allEnable });
        channelNode.set("_all", allChannel);

        ObjectNode propertiesNode = SchemaUtil.getObjectNode();
        ObjectNode channelChannel = SchemaUtil.getField(new String[] { "type", "index" }, new String[] { "string", "not_analyzed" });
        propertiesNode.set(CHANNEL_ACCOUNT, channelChannel);
        ObjectNode channelClientId = SchemaUtil.getField(new String[] { "type", "index" }, new String[] { "string", "not_analyzed" });
        propertiesNode.set(CHANNEL_CLIENT_ID, channelClientId);
        ObjectNode channelName = SchemaUtil.getField(new String[] { "type", "index" }, new String[] { "string", "not_analyzed" });
        propertiesNode.set(CHANNEL_NAME, channelName);
        ObjectNode channelTimestamp = SchemaUtil.getField(new String[] { "type" }, new String[] { "date" });
        propertiesNode.set(CHANNEL_TIMESTAMP, channelTimestamp);
        ObjectNode channelMessageId = SchemaUtil.getField(new String[] { "type", "index" }, new String[] { "string", "not_analyzed" });
        propertiesNode.set(CHANNEL_MESSAGE_ID, channelMessageId);
        channelNode.set("properties", propertiesNode);
        rootNode.set(CHANNEL_TYPE_NAME, channelNode);
        return rootNode;
    }

}
