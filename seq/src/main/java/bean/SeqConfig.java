package bean;

import com.alipay.sofa.jraft.rhea.options.PlacementDriverOptions;
import com.alipay.sofa.jraft.rhea.options.RheaKVStoreOptions;
import com.alipay.sofa.jraft.rhea.options.StoreEngineOptions;
import com.alipay.sofa.jraft.rhea.options.configured.MemoryDBOptionsConfigured;
import com.alipay.sofa.jraft.rhea.options.configured.PlacementDriverOptionsConfigured;
import com.alipay.sofa.jraft.rhea.options.configured.RheaKVStoreOptionsConfigured;
import com.alipay.sofa.jraft.rhea.options.configured.StoreEngineOptionsConfigured;
import com.alipay.sofa.jraft.rhea.storage.StorageType;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.rpc.config.ConsumerConfig;
import com.alipay.sofa.rpc.listener.ChannelListener;
import com.alipay.sofa.rpc.transport.AbstractChannel;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.*;
import lombok.extern.log4j.Log4j2;
import thirdpart.codec.IBodyCodec;
import thirdpart.fetchsurv.IFetchService;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;

@Log4j2
@ToString
@RequiredArgsConstructor
public class SeqConfig {

    private String dataPath;

    private String serveUrl;

    private String serverList;

    @NonNull
    private String fileName;

    @Getter
    private Node node;

    public void start() throws IOException {
        // 1. 读取配置文件
        initConfig();
        // 2. 初始化kv store 集群
        startSeqDbCluster();
        // 3.启动下游广播

        // 4.初始化网关连接
        startupFetch();
    }

    /////////////////////////////抓取逻辑/////////////////////////////////////////////
    private String fetchUrls;

    @ToString.Exclude
    @Getter
    private Map<String, IFetchService> fetchServiceMap = Maps.newConcurrentMap();

    @NonNull
    @ToString.Exclude
    @Getter
    private IBodyCodec codec;

    @RequiredArgsConstructor
    private class FetchChannelListener implements ChannelListener {

        @NonNull
        private ConsumerConfig<IFetchService> config;

        @Override
        public void onConnected(AbstractChannel channel) {
            String remoteAddr = channel.remoteAddress().toString();
            log.info("connect to gatewat : {}",remoteAddr);
            fetchServiceMap.put(remoteAddr,config.refer());
        }

        @Override
        public void onDisconnected(AbstractChannel channel) {
            String remoteAddr = channel.remoteAddress().toString();
            log.info("disconnect from gatewat : {}",remoteAddr);
            fetchServiceMap.remove(remoteAddr);
        }

    }

    /**
     * 1 从哪些网关抓取
     * 2 通信方式
     *
     */
    private void startupFetch() {
        //1.建立所有到网关的连接
        String[] urls = fetchUrls.split(";");
        for(String url : urls){
            ConsumerConfig<IFetchService> consumerConfig = new ConsumerConfig<IFetchService>()
                    //通信接口
                    .setInterfaceId(IFetchService.class.getName())
                    //RPC通信协议
                    .setProtocol("bolt")
                    //超时时间
                    .setTimeout(5000)
                    //直连地址
                    .setDirectUrl(url);
            consumerConfig.setOnConnect(Lists.newArrayList(new FetchChannelListener(consumerConfig)));
            fetchServiceMap.put(url,consumerConfig.refer());
        }

        //2.定时抓取数据的任务
        new Timer().schedule(new FetchTask(this),5000,1000);
    }


    /**
     * 初始化kv store 集群
     */
    private void startSeqDbCluster(){
        final PlacementDriverOptions pdOpts = PlacementDriverOptionsConfigured
                .newConfigured()
                .withFake(true)
                .config();

        String[] split = serveUrl.split(":");

        final StoreEngineOptions storeOpts = StoreEngineOptionsConfigured
                .newConfigured()
                .withStorageType(StorageType.Memory)
                .withMemoryDBOptions(MemoryDBOptionsConfigured.newConfigured().config())
                .withRaftDataPath(dataPath)
                .withServerAddress(new Endpoint(split[0], Integer.parseInt(split[1])))
                .config();

        final RheaKVStoreOptions opts = RheaKVStoreOptionsConfigured
                .newConfigured()
                .withInitialServerList(serverList)
                .withStoreEngineOptions(storeOpts)
                .withPlacementDriverOptions(pdOpts)
                .config();

        node = new Node(opts);
        node.start();
        Runtime.getRuntime().addShutdownHook(new Thread(node::stop));
        log.info("start seq node success on port : {}", split[1]);
    }

    /**
     * 配置文件初始化
     * @throws IOException
     */
    private void initConfig() throws IOException {
        Properties properties = new Properties();
        properties.load(Object.class.getResourceAsStream("/" + fileName));

        dataPath = properties.getProperty("dataPath");
        serveUrl = properties.getProperty("serveUrl");
        serverList = properties.getProperty("serverList");
        fetchUrls = properties.getProperty("fetchUrls");

        log.info("read config :{}", this);
    }




}
