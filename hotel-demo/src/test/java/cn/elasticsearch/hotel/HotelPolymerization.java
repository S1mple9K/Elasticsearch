package cn.elasticsearch.hotel;

import cn.elasticsearch.hotel.pojo.HotelDoc;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.join.aggregations.ParentAggregationBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.List;

/**
 * elasticsearch聚合
 */
class HotelPolymerization {
    private RestHighLevelClient restHighLevelClient;

    /**
     * Bucket聚合
     * 根据品牌分组
     * aggregation(AggregationBuilder aggregation)
     * 1.聚合构造器
     * terms(String name).field(String field).size(int size)
     * 1.聚合名称
     * 2.字段名称
     * 3.显示条数
     */

    @Test
    void Bucket() throws IOException {
        //1.获取searchRequest对象
        SearchRequest searchRequest = new SearchRequest("hotel");
        //2.请求参数
        //2.1.设置文档数量
        searchRequest.source().size(0);
        //2.2.设置聚合参数
        searchRequest.source().aggregation(AggregationBuilders.terms("brandAgg").field("brand").size(20));
        //3.发送请求
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        responseFilters(response);
    }

    /**
     * 封装解析聚合响应方法
     */

    void responseFilters(SearchResponse response){
        //1.解析响应
        Aggregations aggregations = response.getAggregations();
        //2.根据名称获取聚合结果
        Terms brandTerms = aggregations.get("brandAgg");
        //3.获取聚合结果集合
        List<? extends Terms.Bucket> buckets = brandTerms.getBuckets();
        //4.遍历
        for (Terms.Bucket bucket : buckets) {
            //4.1.获取key
            String key = bucket.getKeyAsString();
            //4.2.打印key
            System.out.println(key);
        }
    }

    /**
     * 初始化RestHighLevelClient
     */

    @BeforeEach
    void setUp() {
        this.restHighLevelClient = new RestHighLevelClient(RestClient.builder(
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
