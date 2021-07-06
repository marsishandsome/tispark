package com.pingcap.tikv.region;

import io.grpc.stub.StreamObserver;
import org.tikv.kvproto.ImportSstpb;

public class RawWriteOutputStreamObserver implements StreamObserver<ImportSstpb.RawWriteResponse> {
  RegionStoreClient client;

  public RawWriteOutputStreamObserver(RegionStoreClient client) {
    this.client = client;
  }

  @Override
  public void onNext(ImportSstpb.RawWriteResponse rawWriteResponse) {
    client.setRawWriteResponse(rawWriteResponse);
  }

  @Override
  public void onError(Throwable throwable) {}

  @Override
  public void onCompleted() {}
}
