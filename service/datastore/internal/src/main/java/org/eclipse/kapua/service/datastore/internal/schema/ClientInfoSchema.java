package org.eclipse.kapua.service.datastore.internal.schema;

import org.eclipse.kapua.service.datastore.client.DatamodelMappingException;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class ClientInfoSchema {

    /**
     * Client information schema name
     */
    public final static String CLIENT_TYPE_NAME = "client";
    /**
     * Client information - client identifier
     */
    public final static String CLIENT_ID = "client_id";
    /**
     * Client information - account name
     */
    public final static String CLIENT_ACCOUNT = "account";
    /**
     * Client information - message timestamp (of the first message published in this channel)
     */
    public final static String CLIENT_TIMESTAMP = "timestamp";
    /**
     * Client information - message identifier (of the first message published in this channel)
     */
    public final static String CLIENT_MESSAGE_ID = "message_id";

    public static ObjectNode getClientTypeBuilder(boolean allEnable, boolean sourceEnable) throws DatamodelMappingException {
        ObjectNode rootNode = SchemaUtil.getObjectNode();

        ObjectNode clientNodeName = SchemaUtil.getObjectNode();
        ObjectNode sourceClient = SchemaUtil.getField(new String[] { "enabled" }, new Object[] { sourceEnable });
        clientNodeName.set("_source", sourceClient);

        ObjectNode allClient = SchemaUtil.getField(new String[] { "enabled" }, new Object[] { allEnable });
        clientNodeName.set("_all", allClient);

        ObjectNode propertiesNode = SchemaUtil.getObjectNode();
        ObjectNode clientId = SchemaUtil.getField(new String[] { "type", "index" }, new String[] { "string", "not_analyzed" });
        propertiesNode.set(CLIENT_ID, clientId);
        ObjectNode clientTimestamp = SchemaUtil.getField(new String[] { "type" }, new String[] { "date" });
        propertiesNode.set(CLIENT_TIMESTAMP, clientTimestamp);
        ObjectNode clientAccount = SchemaUtil.getField(new String[] { "type", "index" }, new String[] { "string", "not_analyzed" });
        propertiesNode.set(CLIENT_ACCOUNT, clientAccount);
        ObjectNode clientMessageId = SchemaUtil.getField(new String[] { "type", "index" }, new String[] { "string", "not_analyzed" });
        propertiesNode.set(CLIENT_MESSAGE_ID, clientMessageId);
        clientNodeName.set("properties", propertiesNode);
        rootNode.set(CLIENT_TYPE_NAME, clientNodeName);
        return rootNode;
    }

}
