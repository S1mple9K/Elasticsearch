package cn.elasticsearch.hotel.service.impl;

import cn.elasticsearch.hotel.mapper.HotelMapper;
import cn.elasticsearch.hotel.pojo.Hotel;
import cn.elasticsearch.hotel.pojo.HotelDoc;
import cn.elasticsearch.hotel.pojo.PageResult;
import cn.elasticsearch.hotel.pojo.RequestParams;
import cn.elasticsearch.hotel.service.HotelService;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 酒店Service实现类
 *
 * @author 9K
 */
@Service
public class HotelServiceImpl extends ServiceImpl<HotelMapper, Hotel> implements HotelService {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    /**
     * 搜索功能
     * 过滤条件
     * 当用户选择过滤标签 city brand starName price时做过滤 price为范围过滤
     *
     * @param requestParams
     * @return
     */

    @Override
    public PageResult search(RequestParams requestParams) {
        try {
            //1.创建searchRequest对象
            SearchRequest searchRequest = new SearchRequest("hotel");
            //2.请求参数
            //2.1.query条件
            condition(requestParams, searchRequest);
            //2.2.分页
            int page = requestParams.getPage();
            int size = requestParams.getSize();
            searchRequest.source().from((page - 1) * size).size(size);
            //2.3.排序-根据附近酒店的价格升序排序
            String location = requestParams.getLocation();
            if (location != null && !location.equals("")) {
                searchRequest.source().sort(SortBuilders.geoDistanceSort("location", new GeoPoint(location))
                        .order(SortOrder.ASC).unit(DistanceUnit.KILOMETERS));
            }
            //2.4.增加权重

            //3.发送请求
            SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            //4.解析响应并返回
            return responseJson(response);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 聚合过滤标签接口
     * @param requestParams
     * @return
     */

    @Override
    public Map<String, List<String>> filters(RequestParams requestParams) {
        try {
            //1.获取searchRequest对象
            SearchRequest searchRequest = new SearchRequest("hotel");
            //2.请求参数
            //2.1.查询参数 调用封装的过滤条件方法
            condition(requestParams, searchRequest);
            //2.2.设置文档数量
            searchRequest.source().size(0);
            //2.3.聚合
            buildAggregation(searchRequest);
            //3.发出请求
            SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            //4.解析聚合响应结果
            Map<String, List<String>> filters = responseFilters(response);
            return filters;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 搜索框自动补全接口
     * @param key
     * @return
     */

    @Override
    public List<String> suggestion(String key) {
        try {
            //1.获取searchRequest对象
            SearchRequest searchRequest = new SearchRequest("hotel");
            //2.请求参数
            searchRequest.source().suggest(new SuggestBuilder().addSuggestion(
                    "suggestions", SuggestBuilders.completionSuggestion("suggestion")
                            .prefix(key)
                            .skipDuplicates(true)
                            .size(10)
            ));
            //3.发送请求
            SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            //4.解析结果
            return responseCompletion(response);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }


    /**
     * 添加和修改elasticsearch
     * @param id
     */

    @Override
    public void insertById(Long id) {
        try {
            //1.根据id查询酒店数据
            Hotel hotel = getById(id);
            //2.将hotel转换为hotelDoc
            HotelDoc hotelDoc = new HotelDoc(hotel);
            //3.获取indexRequest对象
            IndexRequest indexRequest = new IndexRequest("hotel").id(hotelDoc.getId().toString());
            //4.Json文档
            indexRequest.source(JSON.toJSONString(hotelDoc), XContentType.JSON);
            //5.发送请求
            restHighLevelClient.index(indexRequest,RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 删除elasticsearch
     * @param id
     */

    @Override
    public void deleteById(Long id) {
        try {
            //1.获取deleteRequest对象
            DeleteRequest deleteRequest = new DeleteRequest();
            //2.发送请求
            restHighLevelClient.delete(deleteRequest,RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 封装自动补全解析响应方法
     */
    public List<String> responseCompletion(SearchResponse response){
        //1.解析响应
        Suggest suggest = response.getSuggest();
        //2.根据名称获取补全结果
        CompletionSuggestion suggestions = suggest.getSuggestion("suggestions");
        //3.获取options
        List<CompletionSuggestion.Entry.Option> options = suggestions.getOptions();
        //4.遍历集合
        List<String> list = new ArrayList<>(options.size());
        for (CompletionSuggestion.Entry.Option option : options) {
            //4.获取option中的text
            String text = option.getText().string();
            list.add(text);
        }
        return list;
    }

    /**
     * 封装标签聚合项方法
     */
    public void buildAggregation(SearchRequest searchRequest){
        searchRequest.source().aggregation(AggregationBuilders
                .terms("brandAgg")
                .field("brand")
                .size(100));
        searchRequest.source().aggregation(AggregationBuilders
                .terms("cityAgg")
                .field("city")
                .size(100));
        searchRequest.source().aggregation(AggregationBuilders
                .terms("starAgg")
                .field("starName")
                .size(100));
    }

    /**
     * 封装解析聚合响应方法
     */

    public Map<String, List<String>> responseFilters(SearchResponse response) {
        //1.解析响应
        Aggregations aggregations = response.getAggregations();
        Map<String, List<String>> result = new HashMap<>();
        //2.将list添加到map中
        List<String> brandList=getAggKey(aggregations,"brandAgg");
        result.put("brand",brandList);
        List<String> cityList=getAggKey(aggregations,"cityAgg");
        result.put("city",cityList);
        List<String> starList=getAggKey(aggregations,"starAgg");
        result.put("starName",starList);
        return result;
    }

    /**
     * 封装解析响应结果遍历获取key
     */
    public List<String> getAggKey(Aggregations aggregations,String aggName){
        //1.根据名称获取聚合结果
        Terms brandTerms = aggregations.get(aggName);
        //2.获取聚合结果集合
        List<? extends Terms.Bucket> buckets = brandTerms.getBuckets();
        List<String> arrayList = new ArrayList<>();
        //3.遍历集合
        for (Terms.Bucket bucket : buckets) {
            //3.1.获取key
            String key = bucket.getKeyAsString();
            //3.2.添加key
            arrayList.add(key);
        }
        return arrayList;
    }

    /**
     * 封装过滤条件方法
     */
    private void condition(RequestParams requestParams, SearchRequest searchRequest) {
        //1.构建boolQuery对象
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        //must部分-全文检索
        String key = requestParams.getKey();
        if (key == null || key == "") {
            boolQuery.must(QueryBuilders.matchAllQuery());
        } else {
            boolQuery.must(QueryBuilders.matchQuery("all", key));
        }
        //条件过滤
        //1.1.城市过滤
        String city = requestParams.getCity();
        if (city != null && !city.equals("")) {
            boolQuery.filter(QueryBuilders.termQuery("city", city));
        }
        //1.2.品牌匹配
        String brand = requestParams.getBrand();
        if (brand != null && !brand.equals("")) {
            boolQuery.filter(QueryBuilders.termQuery("brand", brand));
        }
        //1.3.星级匹配
        String starName = requestParams.getStarName();
        if (starName != null && !starName.equals("")) {
            boolQuery.filter(QueryBuilders.termQuery("starName", starName));
        }
        //1.4.价格匹配
        Integer minPrice = requestParams.getMinPrice();
        Integer maxPrice = requestParams.getMaxPrice();
        if (minPrice != null && maxPrice != null) {
            boolQuery.filter(QueryBuilders.rangeQuery("price").gte(minPrice).lte(maxPrice));
        }
        //2.算分控制-权重
        FunctionScoreQueryBuilder functionScoreQueryBuilder =
                QueryBuilders.functionScoreQuery
                        (boolQuery, new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                                        QueryBuilders.termQuery("isAD", true),
                                        ScoreFunctionBuilders.weightFactorFunction(10)
                                )
                        });
        searchRequest.source().query(functionScoreQueryBuilder);
    }

    /**
     * 封装解析响应方法
     */

    private PageResult responseJson(SearchResponse response) {
        //4.解析响应
        SearchHits searchHits = response.getHits();
        //4.1.查询的总条数
        long total = searchHits.getTotalHits().value;
        //4.2.查询的结果数组
        SearchHit[] hits = searchHits.getHits();
        //4.3.遍历
        List<HotelDoc> hotels = new ArrayList<>();
        for (SearchHit hit : hits) {
            //4.3.获取source
            String json = hit.getSourceAsString();
            //4.4.反序列化
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            //4.5.获取距离排序值
            Object[] sortValues = hit.getSortValues();
            if (sortValues.length > 0) {
                Object sortValue = sortValues[0];
                hotelDoc.setDistance(sortValue);
            }
            hotels.add(hotelDoc);
        }
        //5.封装返回
        return new PageResult(total, hotels);
    }
}
