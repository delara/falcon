package com.edgestream.worker.metrics.icmp4j.platform.windows.jna;

import java.util.Arrays;
import java.util.List;

import com.sun.jna.Library;
import com.sun.jna.Structure;
import com.sun.jna.Structure.ByValue;
import com.sun.jna.Structure.ByReference;
import com.sun.jna.Pointer;

/**
 * Internet Control Message Protocol for Java (ICMP4J)
 * http://www.icmp4j.org
 * Copyright 2009 and beyond, icmp4j
 * <p/>
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License version 3
 * as published by the Free Software Foundation as long as:
 * 1. You credit the original author somewhere within your product or website
 * 2. The credit is easily reachable and not burried deep
 * 3. Your end-user can easily see it
 * 4. You register your name (optional) and company/group/org name (required)
 * at http://www.icmp4j.org
 * 5. You do all of the above within 4 weeks of integrating this software
 * 6. You contribute feedback, fixes, and requests for features
 * <p/>
 * If/when you derive a commercial gain from using this software
 * please donate at http://www.icmp4j.org
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
 * Date: May 23, 2014
 * Time: 6:02:09 PM
 */
public interface IcmpLibrary extends Library {

  /**
   * 
   */
  class IpAddr extends Structure {

    public byte[] bytes = new byte[4];
    
    @Override
    protected List<String> getFieldOrder() {
      return Arrays.asList ("bytes");
    }
  }

  /**
   * 
   */
  class IpAddrByVal extends IpAddr implements ByValue {

    @Override
    protected List<String> getFieldOrder() {
      return Arrays.asList ("bytes");
    }
  }

  /**
   * 
   */
  class IpOptionInformation extends Structure {
    public byte ttl;
    public byte tos;
    public byte flags;
    public byte optionsSize;
    public Pointer optionsData;
    
    @Override
    protected List<String> getFieldOrder() {
      return Arrays.asList ("ttl", "tos", "flags", "optionsSize", "optionsData");
    }
  }

  /**
   * 
   */
  class IpOptionInformationByVal
      extends IpOptionInformation implements ByValue {
  }

  /**
   * 
   */
  class IpOptionInformationByRef
      extends IpOptionInformation implements ByReference {
  }

  /**
   * 
   */
  class IcmpEchoReply extends Structure {
    public IpAddrByVal address;
    public int status;
    public int roundTripTime;
    public short dataSize;
    public short reserved;
    public Pointer data;
    public IpOptionInformationByVal options;

    public IcmpEchoReply(){
    }

    public IcmpEchoReply(Pointer p){
      useMemory(p);
      read();
    }
    
    @Override
    protected List<String> getFieldOrder() {
      return Arrays.asList ("address", "status", "roundTripTime", "dataSize", "reserved", "data", "options");
    }
  }

  Pointer IcmpCreateFile();

  boolean IcmpCloseHandle(Pointer hIcmp);

  int IcmpSendEcho(
          Pointer hIcmp,
          IpAddrByVal destinationAddress,
          Pointer requestData,
          short requestSize,
          IpOptionInformationByRef requestOptions,
          Pointer replyBuffer,
          int replySize,
          int timeout
  );
}