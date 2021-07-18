package bean;

import com.alipay.sofa.jraft.rhea.LeaderStateListener;
import com.alipay.sofa.jraft.rhea.client.DefaultRheaKVStore;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.options.RheaKVStoreOptions;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author HuangKai
 */
@Log4j2
@RequiredArgsConstructor
public class Node {

    @NonNull
    private final RheaKVStoreOptions options;

    @Getter
    private RheaKVStore rheaKVStore;

    private final AtomicLong leaderTerm  = new AtomicLong(-1);

    /**
     * 是否leader节点
     * @return
     */
    public boolean isLeader(){
        return leaderTerm.get() > 0;
    }

    /**
     * 停止Node
     */
    public void stop(){
        rheaKVStore.shutdown();
    }

    /**
     * 启动
     */
    public void start(){
        rheaKVStore = new DefaultRheaKVStore();
        rheaKVStore.init(this.options);
        // 监听节点状态
        rheaKVStore.addLeaderStateListener(-1, new LeaderStateListener() {
            @Override
            public void onLeaderStart(long newTerm) {
                log.info("node become leader");
                leaderTerm.set(newTerm);
            }

            @Override
            public void onLeaderStop(long l) {
                leaderTerm.set(-1);
            }
        });
    }

}
