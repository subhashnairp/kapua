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
package org.eclipse.kapua.service.datastore.internal.elasticsearch;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.kapua.commons.model.id.KapuaEid;
import org.eclipse.kapua.message.internal.KapuaPositionImpl;
import org.eclipse.kapua.message.internal.device.data.KapuaDataChannelImpl;
import org.eclipse.kapua.message.internal.device.data.KapuaDataPayloadImpl;
import org.eclipse.kapua.service.datastore.internal.model.DatastoreMessageImpl;
import org.eclipse.kapua.service.datastore.internal.model.StorableIdImpl;
import org.eclipse.kapua.service.datastore.model.DatastoreMessage;
import org.eclipse.kapua.service.datastore.model.query.StorableFetchStyle;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;

public class MessageObjectBuilder
{

    private DatastoreMessage message;

    public MessageObjectBuilder build(SearchHit searchHit, StorableFetchStyle fetchStyle) 
    		throws EsObjectBuilderException
    {
    	Map<String, SearchHitField> searchHitFields = searchHit.getFields();
    	//String account = searchHitFields.get(EsSchema.MESSAGE_ACCOUNT).getValue();
    	String accountId = searchHitFields.get(EsSchema.MESSAGE_ACCOUNT_ID).getValue();
    	String deviceId = searchHitFields.get(EsSchema.MESSAGE_DEVICE_ID).getValue();
    	String clientId = searchHitFields.get(EsSchema.MESSAGE_CLIENT_ID).getValue();
        //String channel = searchHitFields.get(EsSchema.MESSAGE_CHANNEL).getValue();

        DatastoreMessageImpl tmpMessage = new DatastoreMessageImpl();
        KapuaDataChannelImpl dataChannel = new KapuaDataChannelImpl();
        tmpMessage.setChannel(dataChannel);

    	SearchHitField timestampObj = searchHitFields.get(EsSchema.MESSAGE_TIMESTAMP);
		tmpMessage.setTimestamp((Date) (timestampObj == null ? 
				null : EsUtils.convertToKapuaObject("date", (String) timestampObj.getValue())));

 		tmpMessage.setScopeId((accountId == null ? null : KapuaEid.parseCompactId(accountId)));
 		tmpMessage.setDeviceId(deviceId == null ? null : KapuaEid.parseCompactId(deviceId));
 		tmpMessage.setClientId(clientId);
        tmpMessage.setDatastoreId(new StorableIdImpl(searchHit.getId()));			

        if (fetchStyle.equals(StorableFetchStyle.FIELDS)) {
            this.message = tmpMessage;
            return this;
        }

        Map<String, Object> source = searchHit.getSource();

        @SuppressWarnings("unchecked")
		List<String> channelParts = (List<String>) source.get(EsSchema.MESSAGE_CHANNEL_PARTS);
        dataChannel.setSemanticParts(channelParts);

        KapuaDataPayloadImpl payload = new KapuaDataPayloadImpl();
        KapuaPositionImpl position = null;
        if (source.get(EsSchema.MESSAGE_POSITION) != null) {

            @SuppressWarnings("unchecked")
			Map<String, Object> positionMap = (Map<String, Object>) source.get(EsSchema.MESSAGE_POSITION);

            @SuppressWarnings("unchecked")
			Map<String, Object> locationMap = (Map<String, Object>) positionMap.get(EsSchema.MESSAGE_POS_LOCATION);

            position = new KapuaPositionImpl();
            if (locationMap != null && locationMap.get("lat") != null)
                position.setLatitude((double) locationMap.get("lat"));

            if (locationMap != null && locationMap.get("lon") != null)
                position.setLatitude((double) locationMap.get("lon"));

            Object obj = positionMap.get(EsSchema.MESSAGE_POS_ALT);
            if (obj != null)
                position.setAltitude((double) obj);

            obj = positionMap.get(EsSchema.MESSAGE_POS_HEADING);
            if (obj != null)
                position.setHeading((double) obj);

            obj = positionMap.get(EsSchema.MESSAGE_POS_PRECISION);
            if (obj != null)
                position.setPrecision((double) obj);

            obj = positionMap.get(EsSchema.MESSAGE_POS_SATELLITES);
            if (obj != null)
                position.setSatellites((int) obj);

            obj = positionMap.get(EsSchema.MESSAGE_POS_SPEED);
            if (obj != null)
                position.setSpeed((double) obj);

            obj = positionMap.get(EsSchema.MESSAGE_POS_STATUS);
            if (obj != null)
                position.setStatus((int) obj);

            obj = positionMap.get(EsSchema.MESSAGE_POS_TIMESTAMP);
            if (obj != null)
				position.setTimestamp((Date) EsUtils.convertToKapuaObject("date", (String) obj));
        }

        Object collectedOnFld = source.get(EsSchema.MESSAGE_COLLECTED_ON);
        if (collectedOnFld != null)
			tmpMessage.setCapturedOn((Date) (collectedOnFld == null ? null : EsUtils.convertToKapuaObject("date", (String) collectedOnFld)));

        if (source.get(EsSchema.MESSAGE_METRICS) != null) {

            @SuppressWarnings("unchecked")
			Map<String, Object> metrics = (Map<String, Object>) source.get(EsSchema.MESSAGE_METRICS);
            
            Map<String, Object> payloadMetrics = new HashMap<String, Object>();
            
            String[] metricNames = metrics.keySet().toArray(new String[] {});
            for (String metricsName : metricNames) {
                @SuppressWarnings("unchecked")
				Map<String, Object> metricValue = (Map<String, Object>) metrics.get(metricsName);
                if (metricValue.size() > 0) {
                    String[] valueTypes = metricValue.keySet().toArray(new String[] {});
                    Object value = metricValue.get(valueTypes[0]);
                    if (value != null && value instanceof Integer)
                        payloadMetrics.put(EsUtils.restoreMetricName(metricsName), value);
                }
            }
            
            payload.setProperties(payloadMetrics);
        }

        if (fetchStyle.equals(StorableFetchStyle.SOURCE_SELECT)) {
            this.message = tmpMessage;
        }

        if (source.get(EsSchema.MESSAGE_BODY) != null) {
            byte[] body = ((String) source.get(EsSchema.MESSAGE_BODY)).getBytes();
            payload.setBody(body);
        }

        if (payload != null)
            tmpMessage.setPayload(payload);

        this.message = tmpMessage;
        return this;
    }

    public DatastoreMessage getMessage()
    {
        return message;
    }
}
