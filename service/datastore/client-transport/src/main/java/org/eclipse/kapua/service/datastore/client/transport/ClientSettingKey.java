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
package org.eclipse.kapua.service.datastore.client.transport;

import org.eclipse.kapua.commons.setting.SettingKey;

/**
 * Datastore setting keys.
 * 
 * @since 1.0
 *
 */
public enum ClientSettingKey implements SettingKey {

    /**
     * Elasticsearch client provider
     */
    ELASTICSEARCH_CLIENT_PROVIDER("datastore.elasticsearch.client.provider"),
    /**
     * Elasticsearch nodes count
     */
    ELASTICSEARCH_NODES("datastore.elasticsearch.node"),
    /**
     * Elasticsearch cluster name
     */
    ELASTICSEARCH_CLUSTER("datastore.elasticsearch.cluster");

    private String key;

    private ClientSettingKey(String key) {
        this.key = key;
    }

    @Override
    public String key() {
        return key;
    }
}
