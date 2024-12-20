package com.edgestream.worker.metrics.icmp4j;

import java.util.TreeMap;

import com.edgestream.worker.metrics.icmp4j.util.StringUtil;

/**
 * ShortPasta Foundation
 * http://www.shortpasta.org
 * Copyright 2009 and beyond, Sal Ingrilli at the ShortPasta Software Foundation
 * <p/>
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License version 3
 * as published by the Free Software Foundation as long as:
 * 1. You credit the original author somewhere within your product or website
 * 2. The credit is easily reachable and not burried deep
 * 3. Your end-user can easily see it
 * 4. You register your name (optional) and company/group/org name (required)
 * at http://www.shortpasta.org
 * 5. You do all of the above within 4 weeks of integrating this software
 * 6. You contribute feedback, fixes, and requests for features
 * <p/>
 * If/when you derive a commercial gain from using this software
 * please donate at http://www.shortpasta.org
 * <p/>
 * If prefer or require, contact the author specified above to:
 * 1. Release you from the above requirements
 * 2. Acquire a commercial license
 * 3. Purchase a support contract
 * 4. Request a different license
 * 5. Anything else
 * <p/>
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE, similarly
 * to how this is described in the GNU Lesser General Public License.
 * <p/>
 * User: Sal Ingrilli
 * Date: Oct 10, 2014
 * Time: 12:41:46 AM
 */
public class IcmpTraceRouteResponse {

  // my attributes
  private TreeMap<Integer, IcmpPingResponse> ttlToResponseMap;

  // my attributes
  public void setTtlToResponseMap (final TreeMap<Integer, IcmpPingResponse> ttlToResponseMap) { this.ttlToResponseMap = ttlToResponseMap; }
  public TreeMap<Integer, IcmpPingResponse> getTtlToResponseMap () { return ttlToResponseMap; }

  /**
   * The Object interface
   * @return String
   */
  @Override
  public String toString () {
    
    String string = "[";
    for (final int ttl : ttlToResponseMap.keySet ()) {

      final IcmpPingResponse response = ttlToResponseMap.get (ttl);
      if (string.length () > 0) {
        string += StringUtil.getNewLine ();
      }
      string += "ttl: " + ttl + ", response: " + response;
    }
    
    string += StringUtil.getNewLine ();
    string += "]";

    // done
    return string;
  }
}