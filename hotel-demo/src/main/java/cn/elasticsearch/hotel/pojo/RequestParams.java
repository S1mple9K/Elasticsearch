package cn.elasticsearch.hotel.pojo;

import lombok.Data;

/**
 * 搜索请求参数
 * @author 9K
 */
@Data
public class RequestParams {
    private String key;
    private Integer page;
    private Integer size;
    private String sortBy;
    private String city;
    private String brand;
    private String starName;
    private Integer minPrice;
    private Integer maxPrice;
    private String location;
}
