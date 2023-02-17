package cn.elasticsearch.hotel;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

import static cn.elasticsearch.hotel.constants.HotelConstants.MAPPING_TEMPLATE;

/**
 * elasticsearch索引库操作
 */

@SpringBootTest
class HotelIndexTests {

    private RestHighLevelClient restHighLevelClient;

    /**
     * 创建索引库
     */

    @Test
    void createHotelIndex() throws IOException {
        //1.获取创建createRequest对象 即PUT /hotel
        CreateIndexRequest createRequest = new CreateIndexRequest("hotel");
        //2.请求参数 MAPPING_TEMPLATE：静态常量字符串，内容是创建索引库的DSL语句 XContentType.JSON：类型
        createRequest.source(MAPPING_TEMPLATE, XContentType.JSON);
        //3.发送请求 indices()：返回的对象中包含索引库操作的所有方法
        restHighLevelClient.indices().create(createRequest, RequestOptions.DEFAULT);
    }

    /**
     * 删除索引库
     */

    @Test
    void deleteHotelIndex() throws IOException {
        //1.获取删除deleteRequest对象 即DELETE /hotel
        DeleteIndexRequest deleteRequest = new DeleteIndexRequest("hotel");
        //2.发送请求
        restHighLevelClient.indices().delete(deleteRequest,RequestOptions.DEFAULT);
    }

    /**
     * 判断索引库是否存在
     */

    @Test
    void isExistsHotelIndex() throws IOException {
        //1.获取查询request对象 即GET /hotel
        GetIndexRequest getRequest = new GetIndexRequest("hotel");
        //2.发送请求
        boolean exists = restHighLevelClient.indices().exists(getRequest, RequestOptions.DEFAULT);
        //3.输出布尔值
        System.out.println(exists ? "索引库已存在" : "索引库不存在");
    }

    /**
     * 初始化RestHighLevelClient
     */

    @BeforeEach
    void setUp(){
        this.restHighLevelClient=new RestHighLevelClient(RestClient.builder(
                //指定elasticsearch地址
                HttpHost.create("http://192.168.136.129:9200")
        ));
    }

    /**
     * 销毁RestHighLevelClient
     */

    @AfterEach
    void tearDown() throws IOException {
        this.restHighLevelClient.close();
    }
}
