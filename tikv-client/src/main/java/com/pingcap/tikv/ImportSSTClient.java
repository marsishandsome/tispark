package com.pingcap.tikv;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pingcap.tikv.exception.TiKVException;
import com.pingcap.tikv.meta.Store;
import com.pingcap.tikv.meta.StoreInfo;
import com.pingcap.tikv.meta.StoresInfo;
import com.pingcap.tikv.operation.NoopHandler;
import com.pingcap.tikv.util.ChannelFactory;
import com.pingcap.tikv.util.ConcreteBackOffer;
import io.grpc.ManagedChannel;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.tikv.kvproto.ImportSSTGrpc;
import org.tikv.kvproto.ImportSSTGrpc.ImportSSTBlockingStub;
import org.tikv.kvproto.ImportSSTGrpc.ImportSSTStub;
import org.tikv.kvproto.ImportSstpb;
import org.tikv.kvproto.ImportSstpb.SwitchMode;
import org.tikv.kvproto.ImportSstpb.SwitchModeRequest;
import org.tikv.kvproto.ImportSstpb.SwitchModeResponse;

public class ImportSSTClient extends AbstractGRPCClient<ImportSSTBlockingStub, ImportSSTStub> {

  public static ImportSSTClient createImportSSTClient(
      TiConfiguration conf, ChannelFactory channelFactory) {
    return new ImportSSTClient(conf, channelFactory);
  }

  private ImportSSTClient(TiConfiguration conf, ChannelFactory channelFactory) {
    super(conf, channelFactory);
  }

  public void switchTiKVToImportMode() {
    switchTiKVMode(SwitchMode.Import);
  }

  public void switchTiKVToNormalMode() {
    switchTiKVMode(SwitchMode.Normal);
  }

  private void switchTiKVMode(ImportSstpb.SwitchMode mode) {
    URI pdAddr = conf.getPdAddrs().get(0);
    List<String> tikvAddrs =
        getAllTiKVAddrs(String.format("%s:%s", pdAddr.getHost(), pdAddr.getPort()));
    tikvAddrs.forEach(
        addr -> {
          // update channel for different tikv stores;
          ManagedChannel channel = channelFactory.getChannel(addr);
          this.blockingStub = ImportSSTGrpc.newBlockingStub(channel);
          this.asyncStub = ImportSSTGrpc.newStub(channel);

          Supplier<SwitchModeRequest> request =
              () -> ImportSstpb.SwitchModeRequest.newBuilder().setMode(mode).build();
          NoopHandler<SwitchModeResponse> noopHandler = new NoopHandler<>();

          // backoff in 1 second.
          callWithRetry(
              ConcreteBackOffer.newCustomBackOff(1),
              ImportSSTGrpc.getSwitchModeMethod(),
              request,
              noopHandler);
        });
  }

  private List<String> getAllTiKVAddrs(String pdAddr) {
    String url = String.format("http://%s/pd/api/v1/stores", pdAddr);
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper
          .readValue(new URL(url), StoresInfo.class)
          .getStoreAddrs()
          .stream()
          .map(StoreInfo::getStore)
          .map(Store::getAddr)
          .collect(Collectors.toList());
    } catch (IOException e) {
      throw new TiKVException("failed to get store's addr from pd");
    }
  }

  @Override
  protected ImportSSTBlockingStub getBlockingStub() {
    return blockingStub;
  }

  @Override
  protected ImportSSTStub getAsyncStub() {
    return asyncStub;
  }

  @Override
  public void close() throws Exception {
    if (channelFactory != null) {
      channelFactory.close();
    }
  }
}
