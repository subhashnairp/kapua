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

import org.eclipse.kapua.KapuaException;
import org.eclipse.kapua.commons.configuration.AbstractKapuaConfigurableService;
import org.eclipse.kapua.commons.util.ArgumentValidator;
import org.eclipse.kapua.locator.KapuaLocator;
import org.eclipse.kapua.locator.KapuaProvider;
import org.eclipse.kapua.model.id.KapuaId;
import org.eclipse.kapua.service.account.AccountService;
import org.eclipse.kapua.service.authorization.AuthorizationService;
import org.eclipse.kapua.service.authorization.domain.Domain;
import org.eclipse.kapua.service.authorization.permission.Actions;
import org.eclipse.kapua.service.authorization.permission.Permission;
import org.eclipse.kapua.service.authorization.permission.PermissionFactory;
import org.eclipse.kapua.service.datastore.MessageStoreService;
import org.eclipse.kapua.service.datastore.MetricInfoRegistryService;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.DatastoreMediator;
import org.eclipse.kapua.service.datastore.model.MetricInfo;
import org.eclipse.kapua.service.datastore.model.MetricInfoListResult;
import org.eclipse.kapua.service.datastore.model.StorableId;
import org.eclipse.kapua.service.datastore.model.query.MetricInfoQuery;
import org.eclipse.kapua.service.device.registry.DeviceRegistryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@KapuaProvider
public class MetricInfoRegistryServiceImpl extends AbstractKapuaConfigurableService implements MetricInfoRegistryService
{
    private static final long    serialVersionUID = 7490084233555473342L;
    
    private static final Domain datastoreDomain = new DatastoreDomain();

    @SuppressWarnings("unused")
    private static final Logger  logger           = LoggerFactory.getLogger(MetricInfoRegistryServiceImpl.class);

    private final AccountService       accountService;
    private final DeviceRegistryService deviceRegistryService;
    private final AuthorizationService authorizationService;
    private final PermissionFactory    permissionFactory;
    private final MetricInfoRegistryFacade metricInfoStoreFacade;

    public MetricInfoRegistryServiceImpl()
    {
        super(MetricInfoRegistryService.class.getName(), datastoreDomain, DatastoreEntityManagerFactory.getInstance());

        KapuaLocator locator = KapuaLocator.getInstance();
        accountService = locator.getService(AccountService.class);
        deviceRegistryService = locator.getService(DeviceRegistryService.class);
        authorizationService = locator.getService(AuthorizationService.class);
        permissionFactory = locator.getFactory(PermissionFactory.class);
        
        MessageStoreService messageStoreService = KapuaLocator.getInstance().getService(MessageStoreService.class);
        ConfigurationProviderImpl configurationProvider = new ConfigurationProviderImpl(messageStoreService, accountService, deviceRegistryService);
        this.metricInfoStoreFacade = new MetricInfoRegistryFacade(configurationProvider, DatastoreMediator.getInstance());
        DatastoreMediator.getInstance().setMetricInfoStoreFacade(metricInfoStoreFacade);
    }

    @Override
    public void delete(KapuaId scopeId, StorableId id)
        throws KapuaException
    {
    	try 
    	{
	        //
	        // Argument Validation
	        ArgumentValidator.notNull(scopeId, "scopeId");
	        //
	        // Check Access
	        this.checkDataAccess(scopeId, Actions.delete);
	
	        this.metricInfoStoreFacade.delete(scopeId, id);
    	}
    	catch (Exception e) {
    		throw KapuaException.internalError(e);
    	}
    }

    @Override
    public MetricInfo find(KapuaId scopeId, StorableId id)
        throws KapuaException
    {
    	try
    	{
	        //
	        // Argument Validation
	        ArgumentValidator.notNull(scopeId, "scopeId");
	        //
	        // Check Access
	        this.checkDataAccess(scopeId, Actions.read);
	
	        return this.metricInfoStoreFacade.find(scopeId, id);
    	} 
    	catch (Exception e) {
    		throw KapuaException.internalError(e);
    	}
    }

    @Override
    public MetricInfoListResult query(KapuaId scopeId, MetricInfoQuery query)
        throws KapuaException
    {
    	try
    	{
	        //
	        // Argument Validation
	        ArgumentValidator.notNull(scopeId, "scopeId");
	        //
	        // Check Access
	        this.checkDataAccess(scopeId, Actions.read);
	
	        return this.metricInfoStoreFacade.query(scopeId, query);
    	} 
    	catch (Exception e) {
    		throw KapuaException.internalError(e);
    	}
    }

    @Override
    public long count(KapuaId scopeId, MetricInfoQuery query)
        throws KapuaException
    {
    	try
    	{
	        //
	        // Argument Validation
	        ArgumentValidator.notNull(scopeId, "scopeId");
	        //
	        // Check Access
	        this.checkDataAccess(scopeId, Actions.read);
	
	        return this.metricInfoStoreFacade.count(scopeId, query);
    	} 
    	catch (Exception e) {
    		throw KapuaException.internalError(e);
    	}
    }

    @Override
    public void delete(KapuaId scopeId, MetricInfoQuery query)
        throws KapuaException
    {
    	try
    	{
	        //
	        // Argument Validation
	        ArgumentValidator.notNull(scopeId, "scopeId");
	
	        //
	        // Check Access
	        this.checkDataAccess(scopeId, Actions.read);
	
	        this.metricInfoStoreFacade.delete(scopeId, query);
		} 
		catch (Exception e) {
			throw KapuaException.internalError(e);
		}
    }

    private void checkDataAccess(KapuaId scopeId, Actions action)
        throws KapuaException
    {
        //
        // Check Access
        Permission permission = permissionFactory.newPermission(datastoreDomain, action, scopeId);
        authorizationService.checkPermission(permission);
    }
}
