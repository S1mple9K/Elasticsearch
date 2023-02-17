package cn.elasticsearch.hotel;

import cn.elasticsearch.hotel.pojo.HotelDoc;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * elasticsearch查询操作
 */
class HotelQuery {

    private RestHighLevelClient restHighLevelClient;


    /**
     * match_all查询：查询全部
     */

    @Test
    void matchAll() throws IOException {
        //1.获取searchRequest对象
        SearchRequest searchRequest = new SearchRequest("hotel");
        //2.请求参数
        searchRequest.source().query(QueryBuilders.matchAllQuery());
        //3.发送请求
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        //4.调用解析响应方法
        responseJson(response);
    }

    /**
     * 全文检索查询(单字段)-match
     * matchQuery(String name, Object text)
     * 1.要查询的字段
     * 2.查询的值
     */

    @Test
    void match() throws IOException {
        //1.创建searchRequest对象
        SearchRequest searchRequest = new SearchRequest("hotel");
        //2.请求参数
        searchRequest.source().query(QueryBuilders.matchQuery("all","外滩如家"));
        //3.发送请求
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        //4.调用解析响应方法
        responseJson(response);
    }

    /**
     * 全文检索查询(多字段)-multi_match
     * multiMatchQuery(Object text, String... fieldNames)
     * 1.查询的值
     * 2.要查询的字段
     */

    @Test
    void multiMatch() throws IOException {
        //1.创建searchRequest对象
        SearchRequest searchRequest = new SearchRequest("hotel");
        //2.请求参数
        searchRequest.source().query(QueryBuilders.multiMatchQuery("外滩如家","brand","name","business"));
        //3.发送请求
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        //4.调用解析响应方法
        responseJson(response);
    }

    /**
     * 精确查询(精确值)-term
     * termQuery(String name, String value)
     * 1.要查询的字段
     * 2.查询的值
     */

    @Test
    void term() throws IOException {
        //1.创建searchRequest对象
        SearchRequest searchRequest = new SearchRequest("hotel");
        //2.请求参数
        searchRequest.source().query(QueryBuilders.termQuery("city","北京"));
        //3.发送请求
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        //4.调用解析响应方法
        responseJson(response);
    }

    /**
     * 精确查询(范围)-range
     * rangeQuery(String name).gte(Object from).lte(Object to)
     * 1.查询范围的字段
     * 2.大于等于的值
     * 3.小于等于的值
     */

    @Test
    void range() throws IOException {
        //1.创建searchRequest对象
        SearchRequest searchRequest = new SearchRequest("hotel");
        //2.请求参数
        searchRequest.source().query(QueryBuilders.rangeQuery("price").gte(1000).lte(3000));
        //3.发送请求
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        //4.调用解析响应方法
        responseJson(response);
    }

    /**
     * 地理查询-geo_distance
     * eoDistanceSort(String fieldName, GeoPoint... points)
     * 1.字段名称
     * 2.经纬度
     * order(SortOrder order)
     * 1.排序方式
     * unit(DistanceUnit unit)
     * 1.单位
     */

    @Test
    void geoDistance() throws IOException {
        //1.创建searchRequest对象
        SearchRequest searchRequest = new SearchRequest("hotel");
        //2.请求参数
        searchRequest.source().sort(SortBuilders.geoDistanceSort("location",new GeoPoint(31.21,121.5))
                .order(SortOrder.ASC).unit(DistanceUnit.KILOMETERS));
        //3.发送请求
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        //4.调用解析响应方法
        responseJson(response);
    }


    /**
     * 复合查询(多条件查询)-booleanQuery
     * termQuery(String name, String value)
     * 1.要查询的字段
     * 2.查询的值
     * rangeQuery(String name).lte(Object to)
     * 1.查询范围的字段
     * 2.小于等于的值
     */

    @Test
    void distance() throws IOException {
        //1.创建searchRequest对象
        SearchRequest searchRequest = new SearchRequest("hotel");
        //2.创建booleanBuilder对象
        BoolQueryBuilder booleanBuilder = QueryBuilders.boolQuery();
        //3.1.添加must条件 必须匹配
        booleanBuilder.must(QueryBuilders.termQuery("city","上海"));
        //3.2.添加filter条件 必须匹配，不参与算分
        booleanBuilder.filter(QueryBuilders.rangeQuery("price").lte("250"));
        //4.请求参数
        searchRequest.source().query(booleanBuilder);
        //5.发送请求
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        //6.调用解析响应方法
        responseJson(response);
    }

    /**
     * 增加权重-functionScore
     * functionScoreQuery(QueryBuilder queryBuilder, FunctionScoreQueryBuilder.FilterFunctionBuilder[] filterFunctionBuilders)
     * 1.原始查询
     * 2.functionScore数组
     * FilterFunctionBuilder(QueryBuilder filter, ScoreFunctionBuilder<?> scoreFunction)
     * 1.过滤字段（一般为是否推广的字段，排除值为true的，代表推广置顶）
     * 2.算分函数
     */

    @Test
    void functionScore() throws IOException {
        //1.创建searchRequest对象
        SearchRequest searchRequest = new SearchRequest("hotel");
        //2.请求参数
        FunctionScoreQueryBuilder functionScoreQueryBuilder = QueryBuilders.functionScoreQuery(
                    QueryBuilders.matchQuery("name", "外滩"),
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                            new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                                QueryBuilders.termQuery("isAD", true),
                                ScoreFunctionBuilders.weightFactorFunction(5)
                            )
                        });
        searchRequest.source().query(functionScoreQueryBuilder);
        //3.发送请求
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        //4.调用解析响应方法
        responseJson(response);
    }



    /**
     * 查询结果的排序和分页
     * source().from(int from).size(int size)
     * 1.分页开始的位置
     * 2.每页显示条数
     * source().sort(String name, SortOrder order)
     * 1.排序字段
     * 2.排序方式
     */

    @Test
    void sortAndPage() throws IOException {
        //1.创建searchRequest对象
        SearchRequest searchRequest = new SearchRequest("hotel");
        //2.请求参数
        searchRequest.source().query(QueryBuilders.matchAllQuery());
        //3.分页
        searchRequest.source().from(0).size(5);
        //4.排序
        searchRequest.source().sort("price", SortOrder.ASC);
        //5.发送请求
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        //6.调用解析响应方法
        responseJson(response);
    }

    /**
     * 查询结果的高亮
     */

    @Test
    void highlighter() throws IOException {
        //1.创建searchRequest对象
        SearchRequest searchRequest = new SearchRequest("hotel");
        //2.请求参数
        searchRequest.source().query(QueryBuilders.matchQuery("all","如家"));
        //3.高亮显示
        searchRequest.source().highlighter(new HighlightBuilder().field("name").requireFieldMatch(false));
        //4.发送请求
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        //5.调用解析响应方法
        responseJson(response);
    }

    /**
     * 搜索框自动补全
     * suggest(SuggestBuilder suggestBuilder)
     * 1.自动补全构造器
     * addSuggestion(String name, SuggestionBuilder<?> suggestion)
     * 1.自动补全名称
     * 2.自动补全构造器
     * completionSuggestion(String fieldname).prefix(String prefix).skipDuplicates(boolean skipDuplicates).size(int size)
     * 1.字段名称
     * 2.前缀
     * 3.去掉重复
     * 4.显示条数
     */

    @Test
    void autoCompletion() throws IOException {
        //1.获取searchRequest对象
        SearchRequest searchRequest = new SearchRequest("hotel");
        //2.请求参数
        searchRequest.source().suggest(new SuggestBuilder().addSuggestion(
                "suggestions", SuggestBuilders.completionSuggestion("suggestion")
                        .prefix("rj")
                        .skipDuplicates(true)
                        .size(10)
        ));
        //3.发送请求
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        //4.解析结果
        responseCompletion(response);
    }

    /**
     * 封装自动补全解析响应方法
     */
    void responseCompletion(SearchResponse response){
        //1.解析响应
        Suggest suggest = response.getSuggest();
        //2.根据名称获取补全结果
        CompletionSuggestion suggestions = suggest.getSuggestion("suggestions");
        //3.获取options
        List<CompletionSuggestion.Entry.Option> options = suggestions.getOptions();
        //4.遍历集合
        for (CompletionSuggestion.Entry.Option option : options) {
            //4.获取option中的text
            String text = option.getText().string();
            System.out.println(text);
        }
    }



    /**
     * 封装解析响应方法
     */

    void responseJson(SearchResponse response){
        //1.解析响应
        SearchHits searchHits = response.getHits();
        //1.1.查询的总条数
        long total = searchHits.getTotalHits().value;
        //1.2.查询的结果数组
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit:hits) {
            //1.3.获取source
            String json = hit.getSourceAsString();
            //1.4.反序列化
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            //2.高亮显示
            //2.1.获取高亮结果
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            //2.2.判断高亮结果是否为空
            if (!CollectionUtils.isEmpty(highlightFields)) {
                //2.3.根据字段名获取高亮结果
                HighlightField highlightField = highlightFields.get("name");
                if (highlightField!=null) {
                    //2.4.获取高亮值
                    String name = highlightField.getFragments()[0].string();
                    //2.5.覆盖非高亮结果
                    hotelDoc.setName(name);
                }
            }
            //3.打印
            System.out.println(hotelDoc);
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
