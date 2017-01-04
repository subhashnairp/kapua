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

import org.eclipse.kapua.service.account.Account;
import org.eclipse.kapua.service.device.registry.Device;

public class MessageInfo 
{
    private Account account;
    private Device device;
    
    public MessageInfo(Account account, Device device) {
        this.account = account;
        this.device = device;
    }
    
    public Account getAccount() {
        return this.account;
    }
    
    public Device getDevice() {
    	return this.device;
    }
}
