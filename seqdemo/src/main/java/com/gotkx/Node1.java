package com.gotkx;

import com.alipay.sofa.jraft.rhea.LeaderStateListener;
import com.alipay.sofa.jraft.rhea.client.DefaultRheaKVStore;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.options.PlacementDriverOptions;
import com.alipay.sofa.jraft.rhea.options.RheaKVStoreOptions;
import com.alipay.sofa.jraft.rhea.options.StoreEngineOptions;
import com.alipay.sofa.jraft.rhea.options.configured.*;
import com.alipay.sofa.jraft.rhea.storage.StorageType;
import com.alipay.sofa.jraft.util.Endpoint;
import lombok.extern.log4j.Log4j2;

import java.util.concurrent.atomic.AtomicLong;

@Log4j2
public class Node1 {

    private static final AtomicLong leaderTerm = new AtomicLong(-1);

    public static void main(String[] args) {
        String ip = "127.0.0.1";
        int port = 8891;

        String dataPath = "G:\\Temp\\seq\\server1";

        String serverList = "127.0.0.1:8891,127.0.0.1:8892,127.0.0.1:8893,127.0.0.1:8894,127.0.0.1:8895";

        final StoreEngineOptions storeOpts = StoreEngineOptionsConfigured
                .newConfigured()
                .withStorageType(StorageType.Memory)
                .withMemoryDBOptions(MemoryDBOptionsConfigured.newConfigured().config())
                .withRaftDataPath(dataPath)
                .withServerAddress(new Endpoint(ip,port))
                .config();

        final PlacementDriverOptions pdOpts = PlacementDriverOptionsConfigured.newConfigured().withFake(true).config();

        final RheaKVStoreOptions opts = RheaKVStoreOptionsConfigured
                .newConfigured()
                .withInitialServerList(serverList)
                .withStoreEngineOptions(storeOpts)
                .withPlacementDriverOptions(pdOpts).config();

        RheaKVStore rheaKVStore = new DefaultRheaKVStore();
        rheaKVStore.init(opts);

        rheaKVStore.addLeaderStateListener(-1, new LeaderStateListener() {
            @Override
            public void onLeaderStart(long newTerm) {
                log.info("node become leader, newTerm={}", newTerm);
            }

            @Override
            public void onLeaderStop(long newTerm) {
                leaderTerm.set(-1);
            }
        });

    }
}
