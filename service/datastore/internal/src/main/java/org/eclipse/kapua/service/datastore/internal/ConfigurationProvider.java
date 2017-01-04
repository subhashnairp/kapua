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
package org.eclipse.kapua.service.datastore.internal;

import org.eclipse.kapua.model.id.KapuaId;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.EsConfigurationException;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.LocalServicePlan;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.MessageInfo;

public interface ConfigurationProvider {

	public LocalServicePlan getConfiguration(KapuaId scopeId) 
			throws EsConfigurationException;
	
	public MessageInfo getInfo(KapuaId scopeId, KapuaId deviceId)
			throws EsConfigurationException;
}
