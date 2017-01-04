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

import java.util.concurrent.Callable;

import org.eclipse.kapua.KapuaException;
import org.eclipse.kapua.commons.security.KapuaSecurityUtils;
import org.eclipse.kapua.model.id.KapuaId;
import org.eclipse.kapua.service.account.Account;
import org.eclipse.kapua.service.account.AccountService;
import org.eclipse.kapua.service.config.KapuaConfigurableService;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.EsConfigurationException;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.LocalServicePlan;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.MessageInfo;
import org.eclipse.kapua.service.device.registry.Device;
import org.eclipse.kapua.service.device.registry.DeviceRegistryService;

public class ConfigurationProviderImpl implements ConfigurationProvider {

	private AccountService accountService;
	private DeviceRegistryService deviceRegistryService;
	private KapuaConfigurableService configurableService;
	
	public ConfigurationProviderImpl(KapuaConfigurableService configurableService, 
									 AccountService accountService,
									 DeviceRegistryService deviceRegistryService) {
		this.accountService  = accountService;
		this.deviceRegistryService = deviceRegistryService;
		this.configurableService = configurableService;
	}
	
	@Override
	public LocalServicePlan getConfiguration(KapuaId scopeId) 
			throws EsConfigurationException {
		
		LocalServicePlan localServicePlan = null;	
        try {
        	localServicePlan = new LocalServicePlan(configurableService.getConfigValues(scopeId));
		} catch (KapuaException e) {
			throw new EsConfigurationException(e);
		}
        
        return localServicePlan;
	}
	
	@Override
	public MessageInfo getInfo(KapuaId scopeId, KapuaId deviceId) 
			throws EsConfigurationException {

        Account account = null;        
		if (scopeId != null) {
			try {
				account = KapuaSecurityUtils.doPriviledge(new Callable<Account>() {
	
					@Override
					public Account call() throws Exception {
						return accountService.find(scopeId);
					}
					
				});
			} catch (KapuaException exc) {
				throw new EsConfigurationException(exc);
			} catch (Exception exc) {
				throw new EsConfigurationException(exc);
			}
		}

        Device device = null;
        if (deviceId != null) {
			try {
				device = KapuaSecurityUtils.doPriviledge(new Callable<Device>() {
	
					@Override
					public Device call() throws Exception {
						return deviceRegistryService.find(scopeId, deviceId);
					}
					
				});
			} catch (KapuaException exc) {
				throw new EsConfigurationException(exc);
			} catch (Exception exc) {
				throw new EsConfigurationException(exc);
			}
        }
        
        return new MessageInfo(account, device);
	}

}
