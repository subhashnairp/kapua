package org.eclipse.kapua.service.datastore.internal.converter;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.eclipse.kapua.commons.model.id.KapuaEid;
import org.eclipse.kapua.message.KapuaPayload;
import org.eclipse.kapua.message.KapuaPosition;
import org.eclipse.kapua.message.internal.KapuaPositionImpl;
import org.eclipse.kapua.message.internal.device.data.KapuaDataChannelImpl;
import org.eclipse.kapua.message.internal.device.data.KapuaDataPayloadImpl;
import org.eclipse.kapua.service.datastore.client.DatamodelMappingException;
import org.eclipse.kapua.service.datastore.client.ModelContext;
import org.eclipse.kapua.service.datastore.internal.mediator.DatastoreUtils;
import org.eclipse.kapua.service.datastore.internal.mediator.Metric;
import org.eclipse.kapua.service.datastore.internal.mediator.MetricInfoField;
import org.eclipse.kapua.service.datastore.internal.model.DatastoreMessageImpl;
import org.eclipse.kapua.service.datastore.internal.model.MetricInfoImpl;
import org.eclipse.kapua.service.datastore.internal.model.StorableIdImpl;
import org.eclipse.kapua.service.datastore.internal.schema.ChannelInfoSchema;
import org.eclipse.kapua.service.datastore.internal.schema.ClientInfoSchema;
import org.eclipse.kapua.service.datastore.internal.schema.MessageSchema;
import org.eclipse.kapua.service.datastore.internal.schema.MetricInfoSchema;
import org.eclipse.kapua.service.datastore.model.ChannelInfo;
import org.eclipse.kapua.service.datastore.model.ClientInfo;
import org.eclipse.kapua.service.datastore.model.DatastoreMessage;
import org.eclipse.kapua.service.datastore.model.MetricInfo;
import org.eclipse.kapua.service.datastore.model.query.StorableFetchStyle;

import com.fasterxml.jackson.core.Base64Variants;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ModelContextImpl implements ModelContext {

    private static final String CONVERSION_ERROR_MSG = "Data conversion error";
    private static final String UNSUPPORTED_OBJECT_TYPE_ERROR_MSG = "The conversion of object [%s] is not supported!";
    private static final String UNMARSHAL_INVALID_PARAMETERS_ERROR_MSG = "Object and/or object type cannot be null!";
    private static final String MARSHAL_INVALID_PARAMETERS_ERROR_MSG = "Object and/or object type cannot be null!";

    @SuppressWarnings("unchecked")
    @Override
    public <T> T unmarshal(Class<T> clazz, String serializedObject) throws DatamodelMappingException {
        if (clazz == null || serializedObject == null) {
            throw new DatamodelMappingException(UNMARSHAL_INVALID_PARAMETERS_ERROR_MSG);
        }
        if (ChannelInfo.class.isAssignableFrom(clazz) || ClientInfo.class.isAssignableFrom(clazz)) {
            try {
                return (new ObjectMapper()).readValue(serializedObject, clazz);
            } catch (IOException e) {
                throw new DatamodelMappingException(CONVERSION_ERROR_MSG, e);
            }
        } else if (MetricInfo.class.isAssignableFrom(clazz)) {
            try {
                return (T) unmarshalDatastoreMessage(serializedObject);
            } catch (IOException e) {
                throw new DatamodelMappingException(CONVERSION_ERROR_MSG, e);
            }
        } else if (DatastoreMessage.class.isAssignableFrom(clazz)) {
            try {
                return (T) unmarshalMetricInfo(serializedObject);
            } catch (IOException e) {
                throw new DatamodelMappingException(CONVERSION_ERROR_MSG, e);
            }
        }
        throw new DatamodelMappingException(String.format(UNSUPPORTED_OBJECT_TYPE_ERROR_MSG, clazz.getName()));
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, Object> marshal(Object object) throws DatamodelMappingException {
        if (object==null) {
            throw new DatamodelMappingException(MARSHAL_INVALID_PARAMETERS_ERROR_MSG);
        }
        if (object instanceof DatastoreMessage) {
            return marshalDatastoreMessage((DatastoreMessage) object);
        }
        if (object instanceof ClientInfo) {
            return marshalClientInfo((ClientInfo) object);
        }
        if (object instanceof ChannelInfo) {
            return marshalChannelInfo((ChannelInfo) object);
        }
        if (object instanceof MetricInfo) {
            return marshalMetricInfo((MetricInfo) object);
        }
        throw new DatamodelMappingException(String.format(UNSUPPORTED_OBJECT_TYPE_ERROR_MSG, object.getClass().getName()));
    }
    
    private DatastoreMessage unmarshalDatastoreMessage(String serializedObject) throws DatamodelMappingException, JsonParseException, JsonMappingException, IOException {
        ObjectMapper mapper = new ObjectMapper();
        DatastoreMessageImpl message = new DatastoreMessageImpl();
        Map<String, Object> objectMap = mapper.readValue(serializedObject.getBytes(), new TypeReference<Map<String, String>>() {
        });
        String accountId = (String) objectMap.get(MessageSchema.MESSAGE_ACCOUNT_ID);
        String deviceId = (String) objectMap.get(MessageSchema.MESSAGE_DEVICE_ID);
        String clientId = (String) objectMap.get(MessageSchema.MESSAGE_CLIENT_ID);

        String id = (String) objectMap.get("id");// TODO check!!!! searchHit.getId()
        message.setId(UUID.fromString(id));
        KapuaDataChannelImpl dataChannel = new KapuaDataChannelImpl();
        message.setChannel(dataChannel);

        String timestamp = (String) objectMap.get(MessageSchema.MESSAGE_TIMESTAMP);
        message.setTimestamp((Date) (timestamp == null ? null : DatastoreUtils.convertToKapuaObject("date", (String) timestamp)));

        message.setScopeId((accountId == null ? null : KapuaEid.parseCompactId(accountId)));
        message.setDeviceId(deviceId == null ? null : KapuaEid.parseCompactId(deviceId));
        message.setClientId(clientId);
        message.setDatastoreId(new StorableIdImpl(id));

        // if (fetchStyle.equals(StorableFetchStyle.FIELDS)) {
        // return message;
        // }

        Map<String, Object> source = (Map<String, Object>) objectMap.get("source"); // TODO CHECK!!! searchHit.getSource();

        @SuppressWarnings("unchecked")
        List<String> channelParts = (List<String>) source.get(MessageSchema.MESSAGE_CHANNEL_PARTS);
        dataChannel.setSemanticParts(channelParts);

        KapuaDataPayloadImpl payload = new KapuaDataPayloadImpl();
        KapuaPositionImpl position = null;
        if (source.get(MessageSchema.MESSAGE_POSITION) != null) {

            @SuppressWarnings("unchecked")
            Map<String, Object> positionMap = (Map<String, Object>) source.get(MessageSchema.MESSAGE_POSITION);

            @SuppressWarnings("unchecked")
            Map<String, Object> locationMap = (Map<String, Object>) positionMap.get(MessageSchema.MESSAGE_POS_LOCATION);

            position = new KapuaPositionImpl();
            if (locationMap != null && locationMap.get("lat") != null) {
                position.setLatitude((double) locationMap.get("lat"));
            }
            if (locationMap != null && locationMap.get("lon") != null) {
                position.setLongitude((double) locationMap.get("lon"));
            }
            Object obj = positionMap.get(MessageSchema.MESSAGE_POS_ALT);
            if (obj != null) {
                position.setAltitude((double) obj);
            }
            obj = positionMap.get(MessageSchema.MESSAGE_POS_HEADING);
            if (obj != null) {
                position.setHeading((double) obj);
            }
            obj = positionMap.get(MessageSchema.MESSAGE_POS_PRECISION);
            if (obj != null) {
                position.setPrecision((double) obj);
            }
            obj = positionMap.get(MessageSchema.MESSAGE_POS_SATELLITES);
            if (obj != null) {
                position.setSatellites((int) obj);
            }
            obj = positionMap.get(MessageSchema.MESSAGE_POS_SPEED);
            if (obj != null) {
                position.setSpeed((double) obj);
            }
            obj = positionMap.get(MessageSchema.MESSAGE_POS_STATUS);
            if (obj != null) {
                position.setStatus((int) obj);
            }
            obj = positionMap.get(MessageSchema.MESSAGE_POS_TIMESTAMP);
            if (obj != null) {
                position.setTimestamp((Date) DatastoreUtils.convertToKapuaObject("date", (String) obj));
            }
            message.setPosition(position);
        }
        Object capturedOnFld = source.get(MessageSchema.MESSAGE_CAPTURED_ON);
        if (capturedOnFld != null) {
            message.setCapturedOn((Date) (capturedOnFld == null ? null : DatastoreUtils.convertToKapuaObject("date", (String) capturedOnFld)));
        }
        Object sentOnFld = source.get(MessageSchema.MESSAGE_SENT_ON);
        if (sentOnFld != null) {
            message.setSentOn((Date) (sentOnFld == null ? null : DatastoreUtils.convertToKapuaObject("date", (String) sentOnFld)));
        }
        Object receivedOnFld = source.get(MessageSchema.MESSAGE_RECEIVED_ON);
        if (receivedOnFld != null) {
            message.setReceivedOn((Date) (receivedOnFld == null ? null : DatastoreUtils.convertToKapuaObject("date", (String) receivedOnFld)));
        }
        if (source.get(MessageSchema.MESSAGE_METRICS) != null) {
            @SuppressWarnings("unchecked")
            Map<String, Object> metrics = (Map<String, Object>) source.get(MessageSchema.MESSAGE_METRICS);
            Map<String, Object> payloadMetrics = new HashMap<String, Object>();
            String[] metricNames = metrics.keySet().toArray(new String[] {});
            for (String metricsName : metricNames) {
                @SuppressWarnings("unchecked")
                Map<String, Object> metricValue = (Map<String, Object>) metrics.get(metricsName);
                if (metricValue.size() > 0) {
                    String[] valueTypes = metricValue.keySet().toArray(new String[] {});
                    Object value = metricValue.get(valueTypes[0]);
                    // since elasticsearch doesn't return always the same type of the saved field
                    // (usually due to some promotion of the field type)
                    // we need to check the metric type returned by elasticsearch and, if needed, convert to the proper type
                    payloadMetrics.put(DatastoreUtils.restoreMetricName(metricsName), DatastoreUtils.convertToCorrectType(valueTypes[0], value));
                }
            }
            payload.setProperties(payloadMetrics);
        }
        // if (fetchStyle.equals(StorableFetchStyle.SOURCE_SELECT)) {
        // this.message = tmpMessage;
        // }
        if (source.get(MessageSchema.MESSAGE_BODY) != null) {
            byte[] body = Base64Variants.getDefaultVariant().decode((String) source.get(MessageSchema.MESSAGE_BODY));
            payload.setBody(body);
        }
        if (payload != null) {
            message.setPayload(payload);
        }
        return message;
    }

    private MetricInfo unmarshalMetricInfo(String serializedObject) throws DatamodelMappingException, JsonParseException, JsonMappingException, IOException {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> objectMap = mapper.readValue(serializedObject.getBytes(), new TypeReference<Map<String, String>>() {
        });
        String account = (String) objectMap.get(MetricInfoField.ACCOUNT.field());
        String name = (String) objectMap.get(MetricInfoField.NAME_FULL.field());
        String type = (String) objectMap.get(MetricInfoField.TYPE_FULL.field());
        String lastMsgTimestamp = (String) objectMap.get(MetricInfoField.TIMESTAMP_FULL.field());
        String lastMsgId = (String) objectMap.get(MetricInfoField.MESSAGE_ID_FULL.field());
        Object value = objectMap.get(MetricInfoField.VALUE_FULL.field());
        String clientId = (String) objectMap.get(MetricInfoField.CLIENT_ID.field());
        String channel = (String) objectMap.get(MetricInfoField.CHANNEL.field());
        MetricInfo metricInfo = new MetricInfoImpl(account);// TODO get scope
        metricInfo.setClientId(clientId);
        metricInfo.setChannel(channel);
        metricInfo.setFirstPublishedMessageId(new StorableIdImpl(lastMsgId));
        String metricName = DatastoreUtils.restoreMetricName(name);
        metricInfo.setName(metricName);
        Date timestamp = (Date) DatastoreUtils.convertToKapuaObject("date", lastMsgTimestamp);
        metricInfo.setFirstPublishedMessageTimestamp(timestamp);
        if (DatastoreUtils.CLIENT_TYPE_STRING.equals(type)) {
            metricInfo.setType(DatastoreUtils.convertToKapuaType(type));
            metricInfo.setValue((String) value);
        }
        if (DatastoreUtils.CLIENT_TYPE_INTEGER.equals(type)) {
            metricInfo.setType(DatastoreUtils.convertToKapuaType(type));
            metricInfo.setValue((Integer) value);
        }
        if (DatastoreUtils.CLIENT_TYPE_LONG.equals(type)) {
            Object obj = value;
            if (value != null && value instanceof Integer) {
                obj = ((Integer) value).longValue();
            }
            metricInfo.setType(DatastoreUtils.convertToKapuaType(type));
            metricInfo.setValue((Long) obj);
        }
        if (DatastoreUtils.CLIENT_TYPE_FLOAT.equals(type)) {
            Object obj = value;
            if (value != null && value instanceof Double) {
                obj = ((Double) value).floatValue();
            }
            metricInfo.setType(DatastoreUtils.convertToKapuaType(type));
            metricInfo.setValue((Float) obj);
        }
        if (DatastoreUtils.CLIENT_TYPE_DOUBLE.equals(type)) {
            metricInfo.setType(DatastoreUtils.convertToKapuaType(type));
            metricInfo.setValue((Double) value);
        }
        if (DatastoreUtils.CLIENT_TYPE_BOOL.equals(type)) {
            metricInfo.setType(DatastoreUtils.convertToKapuaType(type));
            metricInfo.setValue((Boolean) value);
        }
        if (DatastoreUtils.CLIENT_TYPE_BINARY.equals(type)) {
            metricInfo.setType(DatastoreUtils.convertToKapuaType(type));
            metricInfo.setValue((byte[]) value);
        }
        if (DatastoreUtils.CLIENT_TYPE_DATE.equals(type)) {
            metricInfo.setType(DatastoreUtils.convertToKapuaType(type));
            metricInfo.setValue((Date) DatastoreUtils.convertToKapuaObject(type, (String) value));
        }
        if (metricInfo.getType() == null) {
            throw new DatamodelMappingException(String.format("Unknown metric type [%s]", type));
        }
        return metricInfo;
    }

    // private Map<String, Object> unmarshalMessage(KapuaMessage<?, ?> message, String messageId,
    // Date timestamp, Date indexedOn, Date receivedOn) {
    private Map<String, Object> marshalDatastoreMessage(DatastoreMessage message) {
        Map<String, Object> unmarshalledMessage = new HashMap<>();
        String accountIdStr = message.getScopeId() == null ? null : message.getScopeId().toCompactId();
        String deviceIdStr = message.getDeviceId() == null ? null : message.getDeviceId().toCompactId();
        unmarshalledMessage.put(MessageSchema.MESSAGE_TIMESTAMP, message.getTimestamp());
        unmarshalledMessage.put(MessageSchema.MESSAGE_RECEIVED_ON, message.getReceivedOn()); // TODO Which field ??
        unmarshalledMessage.put(MessageSchema.MESSAGE_IP_ADDRESS, "127.0.0.1");
        unmarshalledMessage.put(MessageSchema.MESSAGE_ACCOUNT_ID, accountIdStr);
        unmarshalledMessage.put(MessageSchema.MESSAGE_DEVICE_ID, deviceIdStr);
        unmarshalledMessage.put(MessageSchema.MESSAGE_CLIENT_ID, message.getClientId());
        unmarshalledMessage.put(MessageSchema.MESSAGE_CHANNEL, message.getChannel());
        unmarshalledMessage.put(MessageSchema.MESSAGE_CHANNEL_PARTS, message.getChannel().getSemanticParts());
        unmarshalledMessage.put(MessageSchema.MESSAGE_CAPTURED_ON, message.getCapturedOn());
        unmarshalledMessage.put(MessageSchema.MESSAGE_SENT_ON, message.getSentOn());

        KapuaPosition kapuaPosition = message.getPosition();
        if (kapuaPosition != null) {

            Map<String, Object> location = null;
            if (kapuaPosition.getLongitude() != null && kapuaPosition.getLatitude() != null) {
                location = new HashMap<String, Object>();
                location.put("lon", kapuaPosition.getLongitude());
                location.put("lat", kapuaPosition.getLatitude());
            }

            Map<String, Object> position = new HashMap<String, Object>();
            position.put(MessageSchema.MESSAGE_POS_LOCATION, location);
            position.put(MessageSchema.MESSAGE_POS_ALT, kapuaPosition.getAltitude());
            position.put(MessageSchema.MESSAGE_POS_PRECISION, kapuaPosition.getPrecision());
            position.put(MessageSchema.MESSAGE_POS_HEADING, kapuaPosition.getHeading());
            position.put(MessageSchema.MESSAGE_POS_SPEED, kapuaPosition.getSpeed());
            position.put(MessageSchema.MESSAGE_POS_TIMESTAMP, kapuaPosition.getTimestamp());
            position.put(MessageSchema.MESSAGE_POS_SATELLITES, kapuaPosition.getSatellites());
            position.put(MessageSchema.MESSAGE_POS_STATUS, kapuaPosition.getStatus());
            position.put(MessageSchema.MESSAGE_POSITION, position);
        }

        KapuaPayload payload = message.getPayload();
        if (payload == null) {
            return unmarshalledMessage;
        }

        unmarshalledMessage.put(MessageSchema.MESSAGE_BODY, payload.getBody());

        Map<String, Metric> metricMappings = new HashMap<String, Metric>();
        Map<String, Object> kapuaMetrics = payload.getProperties();
        if (kapuaMetrics != null) {
            Map<String, Object> metrics = new HashMap<String, Object>();
            String[] metricNames = kapuaMetrics.keySet().toArray(new String[] {});
            for (String kapuaMetricName : metricNames) {
                Object metricValue = kapuaMetrics.get(kapuaMetricName);
                // Sanitize field names: '.' is not allowed
                String esMetricName = DatastoreUtils.normalizeMetricName(kapuaMetricName);
                String esType = DatastoreUtils.getClientTypeFromValue(metricValue);
                String esTypeAcronim = DatastoreUtils.getClientTypeAcronym(esType);
                Metric esMetric = new Metric();
                esMetric.setName(esMetricName);
                esMetric.setType(esType);

                Map<String, Object> field = new HashMap<String, Object>();
                field.put(esTypeAcronim, metricValue);
                metrics.put(esMetricName, field);

                // each metric is potentially a dynamic field so report it a new mapping
                String mappedName = DatastoreUtils.getMetricValueQualifier(esMetricName, esType);
                metricMappings.put(mappedName, esMetric);
            }
            unmarshalledMessage.put(MessageSchema.MESSAGE_METRICS, metrics);
        }
        return unmarshalledMessage;
    }

    private Map<String, Object> marshalClientInfo(ClientInfo clientInfo) {
        Map<String, Object> unmarshalledClientInfo = new HashMap<>();
        unmarshalledClientInfo.put(ClientInfoSchema.CLIENT_ID, clientInfo.getClientId());
        unmarshalledClientInfo.put(ClientInfoSchema.CLIENT_MESSAGE_ID, clientInfo.getFirstPublishedMessageId());
        unmarshalledClientInfo.put(ClientInfoSchema.CLIENT_TIMESTAMP, clientInfo.getFirstPublishedMessageTimestamp());
        unmarshalledClientInfo.put(ClientInfoSchema.CLIENT_ACCOUNT, clientInfo.getAccount());
        return unmarshalledClientInfo;
    }

    private Map<String, Object> marshalChannelInfo(ChannelInfo channelInfo) {
        Map<String, Object> unmarshalledChannelInfo = new HashMap<>();
        unmarshalledChannelInfo.put(ChannelInfoSchema.CHANNEL_NAME, channelInfo.getChannel());
        unmarshalledChannelInfo.put(ChannelInfoSchema.CHANNEL_TIMESTAMP, channelInfo.getFirstPublishedMessageTimestamp());
        unmarshalledChannelInfo.put(ChannelInfoSchema.CHANNEL_CLIENT_ID, channelInfo.getClientId());
        unmarshalledChannelInfo.put(ChannelInfoSchema.CHANNEL_ACCOUNT, channelInfo.getAccount());
        unmarshalledChannelInfo.put(ChannelInfoSchema.CHANNEL_MESSAGE_ID, channelInfo.getId());
        return unmarshalledChannelInfo;
    }

    private Map<String, Object> marshalMetricInfo(MetricInfo metricInfo) {
        Map<String, Object> unmarshalledMetricInfo = new HashMap<>();
        unmarshalledMetricInfo.put(MetricInfoSchema.METRIC_ACCOUNT, metricInfo.getAccount());
        unmarshalledMetricInfo.put(MetricInfoSchema.METRIC_CLIENT_ID, metricInfo.getClientId());
        unmarshalledMetricInfo.put(MetricInfoSchema.METRIC_CHANNEL, metricInfo.getChannel());

        Map<String, Object> unmarshalledMetricValue = new HashMap<>();
        unmarshalledMetricValue.put(MetricInfoSchema.METRIC_MTR_NAME, metricInfo.getName());
        // TODO to be fixed after refactoring since Alberto's code introduced a get value without parameters
        // unmarshalledMetricValue.put(MetricInfoSchema.METRIC_MTR_TYPE, DatastoreUtils.getClientTypeFromValue(metricInfo.getValue(clazz)));
        // unmarshalledMetricValue.put(MetricInfoSchema.METRIC_MTR_VALUE, value);
        unmarshalledMetricValue.put(MetricInfoSchema.METRIC_MTR_TIMESTAMP, metricInfo.getFirstPublishedMessageTimestamp());
        unmarshalledMetricValue.put(MetricInfoSchema.METRIC_MTR_MSG_ID, metricInfo.getFirstPublishedMessageId());
        unmarshalledMetricInfo.put(MetricInfoSchema.METRIC_MTR, unmarshalledMetricValue);
        return unmarshalledMetricInfo;
    }

    // //CHANNEL
    // Map<String, SearchHitField> fields = searchHit.getFields();
    // String channel = fields.get(ClientSchema.CHANNEL_NAME).getValue();
    // String lastMsgId = fields.get(ClientSchema.CHANNEL_MESSAGE_ID).getValue();
    // String lastMsgTimestampStr = fields.get(ClientSchema.CHANNEL_TIMESTAMP).getValue();
    // String clientId = fields.get(ClientSchema.CHANNEL_CLIENT_ID).getValue();
    // String account = fields.get(ClientSchema.CHANNEL_ACCOUNT).getValue();
    //
    // //CLIENT
    // String idStr = searchHit.getId();
    // Map<String, SearchHitField> fields = searchHit.getFields();
    // String clientId = fields.get(ClientInfoField.CLIENT_ID.field()).getValue();
    // String timestampStr = fields.get(ClientInfoField.TIMESTAMP.field()).getValue();
    // String account = fields.get(ClientInfoField.ACCOUNT.field()).getValue();
    // String messageId = fields.get(ClientInfoField.MESSAGE_ID.field()).getValue();
    // this.clientInfo = new ClientInfoImpl(account, new StorableIdImpl(idStr));
    // this.clientInfo.setClientId(clientId);
    // this.clientInfo.setFirstPublishedMessageId(new StorableIdImpl(messageId));
    // Date timestamp = (Date) DatastoreUtils.convertToKapuaObject("date", timestampStr);
    // this.clientInfo.setFirstPublishedMessageTimestamp(timestamp);
    //
    // //METRIC
    //
    // MESSAGE

    // /**
    // * Get the {@link MessageXContentBuilder} initialized with the provided parameters
    // *
    // * @param account
    // * @param message
    // * @param indexedOn
    // * @param receivedOn
    // * @return
    // * @throws ClientDocumentBuilderException
    // */
    // public MessageXContentBuilder build(String account, KapuaMessage<?, ?> message, Date indexedOn, Date receivedOn)
    // throws ClientDocumentBuilderException {
    // StorableId messageId;
    // UUID uuid = UUID.randomUUID();
    // messageId = new StorableIdImpl(uuid.toString());
    //
    // this.setAccountName(account);
    // this.setClientId(message.getClientId());
    //
    // List<String> parts = message.getChannel().getSemanticParts();
    // this.setChannel(DatastoreChannel.getChannel(parts));
    // this.setChannelParts(parts.toArray(new String[] {}));
    //
    // XContentBuilder messageBuilder;
    // messageBuilder = this.build(message, messageId.toString(),
    // indexedOn, indexedOn, receivedOn);
    //
    // this.setTimestamp(indexedOn);
    // this.setIndexedOn(indexedOn);
    // this.setReceivedOn(receivedOn);
    // this.setSentOn(message.getSentOn());
    // this.setCapturedOn(message.getCapturedOn());
    //
    // this.setMessageId(messageId);
    // this.setBuilder(messageBuilder);
    // return this;
    // }

    // METRIC

    // /**
    // * Get the {@link MetricInfoXContentBuilder} initialized with the provided parameters
    // *
    // * @param metricInfoCreator
    // * @return
    // * @throws ClientDocumentBuilderException
    // */
    // public MetricInfoXContentBuilder build(MetricInfoCreator metricInfoCreator)
    // throws ClientDocumentBuilderException {
    // String idStr = getOrDeriveId(null, metricInfoCreator);
    // StorableId id = new StorableIdImpl(idStr);
    // MetricInfoImpl metricInfo = new MetricInfoImpl(metricInfoCreator.getAccount(), id);
    // metricInfo.setClientId(metricInfoCreator.getClientId());
    // metricInfo.setChannel(metricInfoCreator.getChannel());
    // metricInfo.setFirstPublishedMessageId(metricInfoCreator.getMessageId());
    // metricInfo.setFirstPublishedMessageTimestamp(metricInfoCreator.getMessageTimestamp());
    // metricInfo.setName(metricInfoCreator.getName());
    // metricInfo.setType(metricInfoCreator.getType());
    // metricInfo.setValue(metricInfoCreator.getValue(Object.class));
    //
    // return this.build(metricInfo);
    // }
    //
    // /**
    // * Get the {@link MetricInfoXContentBuilder} initialized with the provided parameters
    // *
    // * @param metricInfo
    // * @return
    // * @throws ClientDocumentBuilderException
    // */
    // public MetricInfoXContentBuilder build(MetricInfo metricInfo)
    // throws ClientDocumentBuilderException {
    // StorableId msgId = metricInfo.getFirstPublishedMessageId();
    // Date msgTimestamp = metricInfo.getFirstPublishedMessageTimestamp();
    // String metricName = metricInfo.getName();
    // Object value = metricInfo.getValue(Object.class);
    //
    // String metricMappedName = DatastoreUtils.getMetricValueQualifier(metricName, DatastoreUtils.convertToClientType(metricInfo.getType()));
    //
    // XContentBuilder metricContentBuilder;
    // metricContentBuilder = this.build(metricInfo.getAccount(),
    // metricInfo.getClientId(),
    // metricInfo.getChannel(),
    // metricMappedName,
    // value,
    // msgTimestamp,
    // msgId.toString());
    //
    // MetricXContentBuilder metricBuilder = new MetricXContentBuilder();
    // metricBuilder.setId(getOrDeriveId(metricInfo.getId(), metricInfo));
    // metricBuilder.setContent(metricContentBuilder);
    // List<MetricXContentBuilder> metricBuilders = new ArrayList<MetricXContentBuilder>();
    // metricBuilders.add(metricBuilder);
    // this.setBuilders(metricBuilders);
    // return this;
    // }
    //
    // /**
    // * Get the {@link XContentBuilder} initialized with the provided parameters
    // *
    // * @param account
    // * @param clientId
    // * @param channel
    // * @param metricMappedName
    // * @param value
    // * @param msgTimestamp
    // * @param msgId
    // * @return
    // * @throws ClientDocumentBuilderException
    // */
    // private XContentBuilder build(String account, String clientId, String channel, String metricMappedName, Object value, Date msgTimestamp, String msgId)
    // throws ClientDocumentBuilderException {
    // try {
    // XContentBuilder builder = XContentFactory.jsonBuilder()
    //
    // return builder;
    // } catch (IOException e) {
    // throw new ClientDocumentBuilderException(String.format("Unable to build metric info document"), e);
    // }
    // }
    //
    // private void getMessageBuilder(String account, String clientId,
    // KapuaMessage<?, ?> message, String messageId,
    // Date indexedOn, Date receivedOn)
    // throws ClientDocumentBuilderException {
    // KapuaPayload payload = message.getPayload();
    // if (payload == null)
    // return;
    //
    // List<MetricXContentBuilder> metricBuilders = new ArrayList<MetricXContentBuilder>();
    //
    // Map<String, Object> kapuaMetrics = payload.getProperties();
    // if (kapuaMetrics != null) {
    //
    // Map<String, Object> metrics = new HashMap<String, Object>();
    // String[] metricNames = kapuaMetrics.keySet().toArray(new String[] {});
    // for (String kapuaMetricName : metricNames) {
    //
    // Object metricValue = kapuaMetrics.get(kapuaMetricName);
    //
    // // Sanitize field names: '.' is not allowed
    // String esMetricName = DatastoreUtils.normalizeMetricName(kapuaMetricName);
    // String esType = DatastoreUtils.getClientTypeFromValue(metricValue);
    //
    // String esTypeAcronim = DatastoreUtils.getClientTypeAcronym(esType);
    // ClientMetric esMetric = new ClientMetric();
    // esMetric.setName(esMetricName);
    // esMetric.setType(esType);
    //
    // Map<String, Object> field = new HashMap<String, Object>();
    // field.put(esTypeAcronim, metricValue);
    // metrics.put(esMetricName, field);
    //
    // // each metric is potentially a dynamic field so report it a new mapping
    // String mappedName = DatastoreUtils.getMetricValueQualifier(esMetricName, esType);
    // String channel = DatastoreChannel.getChannel(message.getChannel().getSemanticParts());
    //
    // MetricXContentBuilder metricBuilder = new MetricXContentBuilder();
    // String metricId = getOrDeriveId(null, account,
    // clientId,
    // channel,
    // mappedName);
    // metricBuilder.setId(metricId);
    //
    // // TODO retrieve the uuid field
    // metricBuilder.setContent(this.build(account,
    // clientId,
    // channel,
    // mappedName,
    // metricValue,
    // indexedOn,
    // messageId));
    // metricBuilders.add(metricBuilder);
    // }
    // }
    //
    // this.setBuilders(metricBuilders);
    // }
}
