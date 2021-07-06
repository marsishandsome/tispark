package com.pingcap.tikv.meta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Store {
  private final String addr;

  @JsonCreator
  public Store(@JsonProperty("address") String addr) {
    this.addr = addr;
  }

  public String getAddr() {
    return addr;
  }
}
