package cn.elasticsearch.hotel.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * 请求参数
 * @author 9K
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PageResult {
    /**
     * 总条数
     */
    private Long total;

    /**
     * 酒店数据
     */
    private List<HotelDoc> hotels;

}
