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

import java.text.ParseException;
import java.util.Date;
import java.util.Map;

import org.eclipse.kapua.commons.util.KapuaDateUtils;
import org.eclipse.kapua.service.datastore.internal.model.DataIndexBy;
import org.eclipse.kapua.service.datastore.internal.model.MetricsIndexBy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageStoreConfiguration
{

    private static final Logger logger = LoggerFactory.getLogger(MessageStoreConfiguration.class);

    /**
     * Expiration date key.<br>
     * <b>The key must be aligned with the key used in org.eclipse.kapua.service.datastore.MessageStoreService.xml meta data configuration file).</b>
     * 
     */
    public final static String CONFIGURATION_EXPIRATION_DATE_KEY = "messageStore.expirationDate";

    /**
     * Data storage enabled key.<br>
     * <b>The key must be aligned with the key used in org.eclipse.kapua.service.datastore.MessageStoreService.xml meta data configuration file).</b>
     * 
     */
    public final static String CONFIGURATION_DATA_STORAGE_ENABLED_KEY = "messageStore.enabled";

    /**
     * Data time to live key.<br>
     * <b>The key must be aligned with the key used in org.eclipse.kapua.service.datastore.MessageStoreService.xml meta data configuration file).</b>
     * 
     */
    public final static String CONFIGURATION_DATA_TTL_KEY = "dataTTL";

    /**
     * Data received per month limit key.<br>
     * <b>The key must be aligned with the key used in org.eclipse.kapua.service.datastore.MessageStoreService.xml meta data configuration file).</b>
     * 
     */
    public final static String CONFIGURATION_RX_BYTE_LIMIT_KEY = "rxByteLimit";

    /**
     * Data index by key (available options are in DataIndexBy enumeration).<br>
     * <b>The key must be aligned with the key used in org.eclipse.kapua.service.datastore.MessageStoreService.xml meta data configuration file).</b>
     * 
     */
    public final static String CONFIGURATION_DATA_INDEX_BY_KEY = "dataIndexBy";

    /**
     * Metrics index by key (available options are in MetricsIndexBy enumeration).<br>
     * <b>The key must be aligned with the key used in org.eclipse.kapua.service.datastore.MessageStoreService.xml meta data configuration file).</b>
     * 
     */
    public final static String CONFIGURATION_METRICS_INDEX_BY_KEY = "metricsIndexBy";

    /**
     * Defines a value in service plan as unlimited resource
     */
    public static final int UNLIMITED = -1;
    
    /**
     * Defines a value in service plan as disabled resource
     */
    public static final int DISABLED = 0;

    private static final long TTL_DEFAULT_DAYS = 30;                                         // TODO define as a default configuration

    private Date expirationDate = null;
    private boolean dataStorageEnabled = true;
    private long           dataTimeToLive     = 90;
    private long           rxByteLimit        = 1000000;
    private DataIndexBy dataIndexBy = DataIndexBy.SERVER_TIMESTAMP;
    private MetricsIndexBy metricsIndexBy = MetricsIndexBy.TIMESTAMP;
    
    private Map<String, Object> values;

    public MessageStoreConfiguration(Map<String, Object> values)
    {
        this.values = values;
        if (this.values != null) {
            if (this.values.get(CONFIGURATION_EXPIRATION_DATE_KEY) != null) {
                try {
                    setExpirationDate(KapuaDateUtils.parseDate((String) this.values.get(CONFIGURATION_EXPIRATION_DATE_KEY)));
                }
                catch (ParseException e) {
                    logger.error("Cannot parse the expiration date parameter: {}", e.getMessage(), e);
                }
            }
            if (this.values.get(CONFIGURATION_DATA_STORAGE_ENABLED_KEY) != null) {
                setDataStorageEnabled(Boolean.parseBoolean((String) this.values.get(CONFIGURATION_DATA_STORAGE_ENABLED_KEY)));
            }
            if (this.values.get(CONFIGURATION_DATA_TTL_KEY) != null) {
                setDataTimeToLive((Integer) this.values.get(CONFIGURATION_DATA_TTL_KEY));
            }
            if (this.values.get(CONFIGURATION_RX_BYTE_LIMIT_KEY) != null) {
                setRxByteLimit((Long) this.values.get(CONFIGURATION_RX_BYTE_LIMIT_KEY));
            }
            if (this.values.get(CONFIGURATION_DATA_INDEX_BY_KEY) != null) {
                setDataIndexBy(DataIndexBy.valueOf((String) this.values.get(CONFIGURATION_DATA_INDEX_BY_KEY)));
            }
            if (this.values.get(CONFIGURATION_METRICS_INDEX_BY_KEY) != null) {
                setMetricsIndexBy(MetricsIndexBy.valueOf((String) this.values.get(CONFIGURATION_METRICS_INDEX_BY_KEY)));
            }
        }
    }

    public Date getExpirationDate()
    {
        return expirationDate;
    }

    public void setExpirationDate(Date expirationDate)
    {
        this.expirationDate = expirationDate;
    }

    public boolean getDataStorageEnabled()
    {
        return dataStorageEnabled;
    }

    public void setDataStorageEnabled(boolean dataStorageEnabled)
    {
        this.dataStorageEnabled = dataStorageEnabled;
    }

    public long getDataTimeToLiveMilliseconds()
    {
        return dataTimeToLive * KapuaDateUtils.DAY_MILLIS;// * KapuaDateUtils.DAY_SECS????
    }

    public long getDataTimeToLive()
    {
        return dataTimeToLive;
    }

    public void setDataTimeToLive(int dataTimeToLive)
    {
        if (dataTimeToLive < 0) {
            this.dataTimeToLive = TTL_DEFAULT_DAYS;
        }
        else {
            this.dataTimeToLive = dataTimeToLive;
        }
    }

    public long getRxByteLimit()
    {
        return rxByteLimit;
    }

    public void setRxByteLimit(long rxByteLimit)
    {
        this.rxByteLimit = rxByteLimit;
    }

    public DataIndexBy getDataIndexBy()
    {
        return dataIndexBy;
    }

    public void setDataIndexBy(DataIndexBy dataIndexBy)
    {
        this.dataIndexBy = dataIndexBy;
    }

    public MetricsIndexBy getMetricsIndexBy()
    {
        return metricsIndexBy;
    }

    public void setMetricsIndexBy(MetricsIndexBy metricsIndexBy)
    {
        this.metricsIndexBy = metricsIndexBy;
    }
}
