package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.RedisIdWorker;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.SHOP_GEO_KEY;
@SpringBootTest
public class HmDianPingApplicationTests {

    @Resource
    private ShopServiceImpl shopService;


    @Resource
    private StringRedisTemplate stringRedisTemplate;


    @Test
    public void loadShopData(){
      //1.查询店铺信息
      List<Shop> list = shopService.list();
      //2.把店铺分组，按照typeId分组，typeId一致的放到一个集合中
      Map<Long,List<Shop>> map = list.stream().collect(Collectors.groupingBy(Shop::getTypeId));
      //3.分批完成写入Redis
        for(Map.Entry<Long,List<Shop>> entry : map.entrySet()){
            //3.1获取类型id
            Long typeId = entry.getKey();
            String key = SHOP_GEO_KEY + typeId;
            //3.2 获取同类型的店铺集合
            List<Shop> value = entry.getValue();
            //3.3写入redis GEOADD key 经度 维度 member
            List<RedisGeoCommands.GeoLocation<String>> locations = new ArrayList<>(value.size());
            for(Shop shop : value){
               //stringRedisTemplate.opsForGeo().add(key,  new Point(shop.getX(),shop.getY()),shop.getId().toString());
                locations.add(new RedisGeoCommands.GeoLocation<>
                        (shop.getId().toString(),
                        new Point(shop.getX(),shop.getY())));
            }
            stringRedisTemplate.opsForGeo().add(key,locations);
        }
    }

}