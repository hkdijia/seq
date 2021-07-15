import com.alipay.sofa.jraft.rhea.client.DefaultRheaKVStore;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.options.PlacementDriverOptions;
import com.alipay.sofa.jraft.rhea.options.RegionRouteTableOptions;
import com.alipay.sofa.jraft.rhea.options.RheaKVStoreOptions;
import com.alipay.sofa.jraft.rhea.options.configured.MultiRegionRouteTableOptionsConfigured;
import com.alipay.sofa.jraft.rhea.options.configured.PlacementDriverOptionsConfigured;
import com.alipay.sofa.jraft.rhea.options.configured.RheaKVStoreOptionsConfigured;
import lombok.extern.log4j.Log4j2;

import java.util.List;

import static jdk.nashorn.internal.runtime.regexp.joni.Config.log;

@Log4j2
public class TestGet {

    public static void main(String[] args) {
        String serverList = "127.0.0.1:8891,127.0.0.1:8892,127.0.0.1:8893,127.0.0.1:8894,127.0.0.1:8895";

        final RheaKVStore rheaKVStore = new DefaultRheaKVStore();

        final List<RegionRouteTableOptions> routeTableOptions = MultiRegionRouteTableOptionsConfigured
                .newConfigured()
                .withInitialServerList(-1L,serverList)
                .config();

        final PlacementDriverOptions pdOpts = PlacementDriverOptionsConfigured
                .newConfigured()
                .withFake(true)
                .withRegionRouteTableOptionsList(routeTableOptions)
                .config();

        final RheaKVStoreOptions options = RheaKVStoreOptionsConfigured
                .newConfigured()
                .withInitialServerList(serverList)
                .withPlacementDriverOptions(pdOpts)
                .config();

        rheaKVStore.init(options);

        String key = "test";
        byte[] get = rheaKVStore.bGet(key);
        String s = new String(get);
        log.info("value={}",s);
    }

}
