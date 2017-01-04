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

public class EsDocumentBuilderException extends EsDatastoreException {

	private static final long serialVersionUID = -978823971375607146L;

	public EsDocumentBuilderException(String message) {
		super(message);
	}
    
    public EsDocumentBuilderException(Throwable t) {
    	super(t);
    }
    
    public EsDocumentBuilderException(String reason, Throwable t) {
    	super(reason, t);
    }
}
