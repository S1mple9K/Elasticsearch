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
 * ??????Service?????????
 *
 * @author 9K
 */
@Service
public class HotelServiceImpl extends ServiceImpl<HotelMapper, Hotel> implements HotelService {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    /**
     * ????????????
     * ????????????
     * ??????????????????????????? city brand starName price???????????? price???????????????
     *
     * @param requestParams
     * @return
     */

    @Override
    public PageResult search(RequestParams requestParams) {
        try {
            //1.??????searchRequest??????
            SearchRequest searchRequest = new SearchRequest("hotel");
            //2.????????????
            //2.1.query??????
            condition(requestParams, searchRequest);
            //2.2.??????
            int page = requestParams.getPage();
            int size = requestParams.getSize();
            searchRequest.source().from((page - 1) * size).size(size);
            //2.3.??????-???????????????????????????????????????
            String location = requestParams.getLocation();
            if (location != null && !location.equals("")) {
                searchRequest.source().sort(SortBuilders.geoDistanceSort("location", new GeoPoint(location))
                        .order(SortOrder.ASC).unit(DistanceUnit.KILOMETERS));
            }
            //2.4.????????????

            //3.????????????
            SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            //4.?????????????????????
            return responseJson(response);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * ????????????????????????
     * @param requestParams
     * @return
     */

    @Override
    public Map<String, List<String>> filters(RequestParams requestParams) {
        try {
            //1.??????searchRequest??????
            SearchRequest searchRequest = new SearchRequest("hotel");
            //2.????????????
            //2.1.???????????? ?????????????????????????????????
            condition(requestParams, searchRequest);
            //2.2.??????????????????
            searchRequest.source().size(0);
            //2.3.??????
            buildAggregation(searchRequest);
            //3.????????????
            SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            //4.????????????????????????
            Map<String, List<String>> filters = responseFilters(response);
            return filters;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * ???????????????????????????
     * @param key
     * @return
     */

    @Override
    public List<String> suggestion(String key) {
        try {
            //1.??????searchRequest??????
            SearchRequest searchRequest = new SearchRequest("hotel");
            //2.????????????
            searchRequest.source().suggest(new SuggestBuilder().addSuggestion(
                    "suggestions", SuggestBuilders.completionSuggestion("suggestion")
                            .prefix(key)
                            .skipDuplicates(true)
                            .size(10)
            ));
            //3.????????????
            SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            //4.????????????
            return responseCompletion(response);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }


    /**
     * ???????????????elasticsearch
     * @param id
     */

    @Override
    public void insertById(Long id) {
        try {
            //1.??????id??????????????????
            Hotel hotel = getById(id);
            //2.???hotel?????????hotelDoc
            HotelDoc hotelDoc = new HotelDoc(hotel);
            //3.??????indexRequest??????
            IndexRequest indexRequest = new IndexRequest("hotel").id(hotelDoc.getId().toString());
            //4.Json??????
            indexRequest.source(JSON.toJSONString(hotelDoc), XContentType.JSON);
            //5.????????????
            restHighLevelClient.index(indexRequest,RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * ??????elasticsearch
     * @param id
     */

    @Override
    public void deleteById(Long id) {
        try {
            //1.??????deleteRequest??????
            DeleteRequest deleteRequest = new DeleteRequest();
            //2.????????????
            restHighLevelClient.delete(deleteRequest,RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * ????????????????????????????????????
     */
    public List<String> responseCompletion(SearchResponse response){
        //1.????????????
        Suggest suggest = response.getSuggest();
        //2.??????????????????????????????
        CompletionSuggestion suggestions = suggest.getSuggestion("suggestions");
        //3.??????options
        List<CompletionSuggestion.Entry.Option> options = suggestions.getOptions();
        //4.????????????
        List<String> list = new ArrayList<>(options.size());
        for (CompletionSuggestion.Entry.Option option : options) {
            //4.??????option??????text
            String text = option.getText().string();
            list.add(text);
        }
        return list;
    }

    /**
     * ???????????????????????????
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
     * ??????????????????????????????
     */

    public Map<String, List<String>> responseFilters(SearchResponse response) {
        //1.????????????
        Aggregations aggregations = response.getAggregations();
        Map<String, List<String>> result = new HashMap<>();
        //2.???list?????????map???
        List<String> brandList=getAggKey(aggregations,"brandAgg");
        result.put("brand",brandList);
        List<String> cityList=getAggKey(aggregations,"cityAgg");
        result.put("city",cityList);
        List<String> starList=getAggKey(aggregations,"starAgg");
        result.put("starName",starList);
        return result;
    }

    /**
     * ????????????????????????????????????key
     */
    public List<String> getAggKey(Aggregations aggregations,String aggName){
        //1.??????????????????????????????
        Terms brandTerms = aggregations.get(aggName);
        //2.????????????????????????
        List<? extends Terms.Bucket> buckets = brandTerms.getBuckets();
        List<String> arrayList = new ArrayList<>();
        //3.????????????
        for (Terms.Bucket bucket : buckets) {
            //3.1.??????key
            String key = bucket.getKeyAsString();
            //3.2.??????key
            arrayList.add(key);
        }
        return arrayList;
    }

    /**
     * ????????????????????????
     */
    private void condition(RequestParams requestParams, SearchRequest searchRequest) {
        //1.??????boolQuery??????
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        //must??????-????????????
        String key = requestParams.getKey();
        if (key == null || key == "") {
            boolQuery.must(QueryBuilders.matchAllQuery());
        } else {
            boolQuery.must(QueryBuilders.matchQuery("all", key));
        }
        //????????????
        //1.1.????????????
        String city = requestParams.getCity();
        if (city != null && !city.equals("")) {
            boolQuery.filter(QueryBuilders.termQuery("city", city));
        }
        //1.2.????????????
        String brand = requestParams.getBrand();
        if (brand != null && !brand.equals("")) {
            boolQuery.filter(QueryBuilders.termQuery("brand", brand));
        }
        //1.3.????????????
        String starName = requestParams.getStarName();
        if (starName != null && !starName.equals("")) {
            boolQuery.filter(QueryBuilders.termQuery("starName", starName));
        }
        //1.4.????????????
        Integer minPrice = requestParams.getMinPrice();
        Integer maxPrice = requestParams.getMaxPrice();
        if (minPrice != null && maxPrice != null) {
            boolQuery.filter(QueryBuilders.rangeQuery("price").gte(minPrice).lte(maxPrice));
        }
        //2.????????????-??????
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
     * ????????????????????????
     */

    private PageResult responseJson(SearchResponse response) {
        //4.????????????
        SearchHits searchHits = response.getHits();
        //4.1.??????????????????
        long total = searchHits.getTotalHits().value;
        //4.2.?????????????????????
        SearchHit[] hits = searchHits.getHits();
        //4.3.??????
        List<HotelDoc> hotels = new ArrayList<>();
        for (SearchHit hit : hits) {
            //4.3.??????source
            String json = hit.getSourceAsString();
            //4.4.????????????
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            //4.5.?????????????????????
            Object[] sortValues = hit.getSortValues();
            if (sortValues.length > 0) {
                Object sortValue = sortValues[0];
                hotelDoc.setDistance(sortValue);
            }
            hotels.add(hotelDoc);
        }
        //5.????????????
        return new PageResult(total, hotels);
    }
}
