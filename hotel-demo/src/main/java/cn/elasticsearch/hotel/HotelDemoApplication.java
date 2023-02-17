package cn.elasticsearch.hotel;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

/**
 * 启动类
 * @author 9K
 */
@SpringBootApplication
@MapperScan("cn.elasticsearch.hotel.mapper")
public class HotelDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(HotelDemoApplication.class, args);
    }

    /**
     * 初始化RestHighLevelClient并注入
     */

    @Bean
    public RestHighLevelClient restHighLevelClient() {
         return new RestHighLevelClient(RestClient.builder(
                //指定elasticsearch地址
                HttpHost.create("http://192.168.136.129:9200")
        ));
    }
}
