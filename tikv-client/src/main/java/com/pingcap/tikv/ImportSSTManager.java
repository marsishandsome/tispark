package com.pingcap.tikv;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.key.Key;
import com.pingcap.tikv.region.RawWriteOutputStreamObserver;
import com.pingcap.tikv.region.RegionStoreClient;
import com.pingcap.tikv.region.TiRegion;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.tikv.kvproto.ImportSstpb;
import org.tikv.kvproto.Metapb;

public class ImportSSTManager {
  private boolean openStream = false;
  private ByteString uuid;
  private TiSession tiSession;
  private Key minKey;
  private Key maxKey;
  private TiRegion region;

  private ImportSstpb.SSTMeta sstMeta;
  private List<RegionStoreClient> clientList;
  private RegionStoreClient clientLeader;

  public ImportSSTManager(
      byte[] uuid, TiSession tiSession, Key minKey, Key maxKey, TiRegion region) {
    this.uuid = ByteString.copyFrom(uuid);
    this.tiSession = tiSession;
    this.minKey = minKey;
    this.maxKey = maxKey;
    this.region = region;
  }

  public void write(Iterator<BytePairWrapper> iterator) {
    int batchSize = 10240;
    while (iterator.hasNext()) {
      ArrayList<ImportSstpb.Pair> pairs = new ArrayList<>(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (iterator.hasNext()) {
          BytePairWrapper pair = iterator.next();
          pairs.add(
              ImportSstpb.Pair.newBuilder()
                  .setKey(ByteString.copyFrom(pair.getKey()))
                  .setValue(ByteString.copyFrom(pair.getValue()))
                  .build());
        }
      }
      if (!openStream) {
        init();
        openStream();
        writeMeta();
        openStream = true;
      }
      writeBatch(pairs);
    }

    if (openStream) {
      finishWriteBatch();
      ingest();
    }
  }

  private void init() {
    long regionId = region.getId();
    Metapb.RegionEpoch regionEpoch = region.getRegionEpoch();
    ImportSstpb.Range range =
        ImportSstpb.Range.newBuilder()
            .setStart(minKey.toByteString())
            .setEnd(maxKey.toByteString())
            .build();

    sstMeta =
        ImportSstpb.SSTMeta.newBuilder()
            .setUuid(uuid)
            .setRegionId(regionId)
            .setRegionEpoch(regionEpoch)
            .setRange(range)
            .build();

    clientLeader = tiSession.getRegionStoreClientBuilder().build(region);
    clientList = new ArrayList<>();
    for (Metapb.Peer peer : region.getPeersList()) {
      long storeId = peer.getStoreId();
      Metapb.Store store = tiSession.getRegionManager().getStoreById(storeId);
      clientList.add(tiSession.getRegionStoreClientBuilder().build(region, store));
    }
  }

  private void openStream() {
    for (RegionStoreClient client : clientList) {
      RawWriteOutputStreamObserver streamObserverResponse =
          new RawWriteOutputStreamObserver(client);
      StreamObserver<ImportSstpb.RawWriteRequest> streamObserverRequest =
          client.rawWrite(streamObserverResponse);

      client.streamObserverResponse = streamObserverResponse;
      client.streamObserverRequest = streamObserverRequest;
    }
  }

  private void writeMeta() {
    ImportSstpb.RawWriteRequest request =
        ImportSstpb.RawWriteRequest.newBuilder().setMeta(sstMeta).build();
    for (RegionStoreClient client : clientList) {
      client.streamObserverRequest.onNext(request);
    }
  }

  private void writeBatch(List<ImportSstpb.Pair> pairs) {
    ImportSstpb.RawWriteBatch batch =
        ImportSstpb.RawWriteBatch.newBuilder().addAllPairs(pairs).build();
    ImportSstpb.RawWriteRequest request =
        ImportSstpb.RawWriteRequest.newBuilder().setBatch(batch).build();
    for (RegionStoreClient client : clientList) {
      client.streamObserverRequest.onNext(request);
    }
  }

  private void finishWriteBatch() {
    for (RegionStoreClient client : clientList) {
      client.streamObserverRequest.onCompleted();
    }
  }

  private void ingest() {
    ImportSstpb.RawWriteResponse response = null;
    int returnNumber = 0;
    while (returnNumber < clientList.size()) {
      returnNumber = 0;
      for (RegionStoreClient client : clientList) {
        response = client.getRawWriteResponse();
        if (response != null) {
          returnNumber++;
        }
      }

      if (returnNumber < clientList.size()) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }

    clientLeader.multiIngest(region.getContext(), response.getMetasList());
  }
}
