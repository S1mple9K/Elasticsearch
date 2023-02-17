package cn.elasticsearch.hotel;

import cn.elasticsearch.hotel.pojo.Hotel;
import cn.elasticsearch.hotel.pojo.HotelDoc;
import cn.elasticsearch.hotel.service.HotelService;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;

/**
 * elasticsearch文档操作
 */

@SpringBootTest
class HotelDocument {

    @Autowired
    private HotelService hotelService;

    private RestHighLevelClient restHighLevelClient;

    /**
     * 根据id从数据库查询数据
     * 添加酒店数据到索引库中
     */

    @Test
    void createDocumentById() throws IOException {
        //根据id查询数据库 即POST /hotel/_doc/36934
        Hotel hotel = hotelService.getById(36934L);
        //转换为HotelDoc 因为elasticsearch中的经纬度由geo_point拼接，所以需要将hotel转为符合他的hotelDoc对象
        HotelDoc hotelDoc=new HotelDoc(hotel);
        //1.获取indexRequest对象 即POST /hotel/_doc/36934
        IndexRequest indexRequest = new IndexRequest("hotel").id((hotelDoc.getId()).toString());
        //2.请求参数
        indexRequest.source(JSON.toJSONString(hotelDoc), XContentType.JSON);
        //3.发送请求
        restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
    }

    /**
     * 从数据库查询数据
     * 批量添加到索引库
     */

    @Test
    void createDocument() throws IOException {
        //批量查询酒店数据
        List<Hotel> hotels = hotelService.list();
        //1.获取BulkRequest对象
        BulkRequest bulkRequest = new BulkRequest();
        //2.批量添加
        for (Hotel hotel: hotels) {
            //转换文档类型
            HotelDoc hotelDoc=new HotelDoc(hotel);
            //新增文档
            bulkRequest.add(new IndexRequest("hotel")
                    .id(hotelDoc.getId().toString())
                    .source(JSON.toJSONString(hotelDoc),XContentType.JSON));
        }
        //3.发送请求
        restHighLevelClient.bulk(bulkRequest,RequestOptions.DEFAULT);
    }

    /**
     * 根据id查询索引库中的文档
     */

    @Test
    void selectDocumentById() throws IOException {
        //1.获取getRequest对象 即GET /hotel/_doc/36934
        GetRequest getRequest = new GetRequest("hotel","36934");
        //2.发送请求，获取响应结果
        GetResponse response = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
        //3.解析响应结果
        String json = response.getSourceAsString();
        //4.反序列化
        HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
        //5.输出结果
        System.out.println(hotelDoc);
    }

    /**
     * 根据id局部更新索引库中的文档
     */

    @Test
    void updateDocumentById() throws IOException {
        //1.获取updateRequest对象 即POST /hotel/_update/36934
        UpdateRequest updateRequest = new UpdateRequest("hotel","36934");
        //2.准备修改的参数 格式为：字段,值
        updateRequest.doc(
                "price","400"
        );
        //3.发送请求
        restHighLevelClient.update(updateRequest,RequestOptions.DEFAULT);
    }

    /**
     * 根据id删除索引库中的文档
     */

    @Test
    void deleteDocumentById() throws IOException {
        //1.获取deleteRequest对象 即DELETE /hotel/_doc/36934
        DeleteRequest deleteRequest = new DeleteRequest("hotel", "36934");
        //2.发送请求
        restHighLevelClient.delete(deleteRequest,RequestOptions.DEFAULT);
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
