package com.edgestream.worker.metrics.icmp4j.platform.linux.jna;


import com.edgestream.worker.metrics.icmp4j.util.JnaUtil;

public class LibraryUtil {
	// my attributes
	  private static IcmpLibrary icmpLibrary;

	  public static IcmpLibrary getIcmpLibrary () { return icmpLibrary; }
	  
	  /**
	   * Uniformly initializes this object
	   * This is in an explicit method and NOT a static initializer so that the caller gets the full stack trace
	   */
	  public static void initialize () {

	    // delegate
	    if (icmpLibrary == null) {
	      icmpLibrary = (IcmpLibrary) JnaUtil.loadLibraryBestEffort(
	        "icmp4jJNA",
	        IcmpLibrary.class);
	    }
	    if (icmpLibrary == null) {
	    	throw new UnsatisfiedLinkError("unnable to find icmp4jJNA");
	    }
	  }
}