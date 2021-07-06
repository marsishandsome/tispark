package com.pingcap.tikv.meta;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class StoresInfo {
  private final List<StoreInfo> storeAddrs;

  public StoresInfo(@JsonProperty("stores") List<StoreInfo> storeAddrs) {
    this.storeAddrs = storeAddrs;
  }

  public List<StoreInfo> getStoreAddrs() {
    return storeAddrs;
  }
}
