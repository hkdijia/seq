package bean;

import com.google.common.collect.Lists;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import thirdpart.fetchsurv.IFetchService;
import thirdpart.order.OrderCmd;
import thirdpart.order.OrderDirection;

import java.util.List;
import java.util.Map;
import java.util.TimerTask;

@Log4j2
@RequiredArgsConstructor
public class FetchTask extends TimerTask {

    @NonNull
    private SeqConfig config;

    @Override
    public void run() {
        //遍历网关
        if (!config.getNode().isLeader()) {
            return;
        }

        Map<String, IFetchService> fetchServiceMap = config.getFetchServiceMap();
        if (MapUtils.isEmpty(fetchServiceMap)) {
            return;
        }

        //获取数据
        List<OrderCmd> cmds = collectAllOrders(fetchServiceMap);
        if (CollectionUtils.isEmpty(cmds)) {
            return;
        }

        log.info(cmds);


        //对数据进行排序
        // 排序 时间优先 价格优先 量优先
        cmds.sort((o1, o2) -> {
            //第一种写法
//            if(o1.timestamp > o2.timestamp){
//                return 1;
//            }else if(o1.timestamp < o2.timestamp){
//                return -1;
//            }else {
//                //比优势价格
//                if(o1.direction == OrderDirection.BUY){
//                    if(o1.direction == o2.direction){
//                        if(o1.price > o2.price){
//                            return -1;
//                        }else if(o1.price < o2.price){
//                            return 1;
//                        }else {
//                            //量比较
//                            if(o1.volume > o2.volume){
//                                return -1;
//                            }else if(o1.volume < o2.volume){
//                                return 1;
//                            }else {
//                                return 0;
//                            }
//                        }
//                    }else {
//                        //方向不同 不影响成交结果 顺序不变
//                        return 0;
//                    }
//                }else if(o1.direction == OrderDirection.SELL){
//                    if(o1.direction == o2.direction){
//                        if(o1.price < o2.price){
//                            return -1;
//                        }else if(o1.price > o2.price){
//                            return 1;
//                        }else {
//                            //量比较
//                            if(o1.volume > o2.volume){
//                                return -1;
//                            }else if(o1.volume < o2.volume){
//                                return 1;
//                            }else {
//                                return 0;
//                            }
//                        }
//                    }else {
//                        //方向不同 不影响成交结果 顺序不变
//                        return 0;
//                    }
//                }else {
//                    return 1;
//                }
//            }

            //第二种写法
            //时间优先 价格优先 量优先

            //第二种写法
            int res = compareTime(o1, o2);
            if (res != 0) {
                return res;
            }

            res = comparePrice(o1, o2);
            if (res != 0) {
                return res;
            }

            res = compareVolume(o1, o2);
            return res;
        });

        //存储到KV Store，发送到撮合核心

    }

    private int compareVolume(OrderCmd o1, OrderCmd o2) {
        if (o1.volume > o2.volume) {
            return -1;
        } else if (o1.volume < o2.volume) {
            return 1;
        } else {
            return 0;
        }
    }

    private int comparePrice(OrderCmd o1, OrderCmd o2) {
        if (o1.direction == o2.direction) {
            if (o1.price > o2.price) {
                return o1.direction == OrderDirection.BUY ? -1 : 1;
            } else if (o1.price < o2.price) {
                return o1.direction == OrderDirection.BUY ? 1 : -1;
            } else {
                return 0;
            }
        }
        return 0;
    }

    private int compareTime(OrderCmd o1, OrderCmd o2) {
        if (o1.timestamp > o2.timestamp) {
            return 1;
        } else if (o1.timestamp < o2.timestamp) {
            return -1;
        } else {
            return 0;
        }
    }

    private List<OrderCmd> collectAllOrders(Map<String, IFetchService> fetchServiceMap) {
        //不推荐 性能不可控 调试不方便 可读性差
//       List<OrderCmd> res =  fetchServiceMap.values().stream()
//                .map(t -> t.fetchData())
//                .filter(msg -> CollectionUtils.isEmpty(msg))
//                .flatMap(List::stream)
//                .collect(Collectors.toList());

        //推荐~~
        List<OrderCmd> msgs = Lists.newArrayList();
        fetchServiceMap.values().forEach(t -> {
            List<OrderCmd> orderCmds = t.fetchData();
            if (CollectionUtils.isNotEmpty(orderCmds)) {
                msgs.addAll(orderCmds);
            }
        });
        return msgs;
    }
}
