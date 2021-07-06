package com.pingcap.tikv.meta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class StoreInfo {
  private final Store store;

  @JsonCreator
  public StoreInfo(@JsonProperty("store") Store store) {
    this.store = store;
  }

  public Store getStore() {
    return store;
  }
}
