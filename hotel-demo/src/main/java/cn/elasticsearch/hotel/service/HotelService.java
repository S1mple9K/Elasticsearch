package cn.elasticsearch.hotel.service;

import cn.elasticsearch.hotel.pojo.Hotel;
import cn.elasticsearch.hotel.pojo.PageResult;
import cn.elasticsearch.hotel.pojo.RequestParams;
import com.baomidou.mybatisplus.extension.service.IService;

import java.util.List;
import java.util.Map;

/**
 * 酒店Service层
 * @author 9K
 */
public interface HotelService extends IService<Hotel> {
    /**
     * 搜索功能和过滤条件接口
     * @param requestParams
     * @return
     */
    PageResult search(RequestParams requestParams);

    /**
     * 聚合过滤标签接口
     * @param requestParams
     * @return
     */
    Map<String, List<String>> filters(RequestParams requestParams);

    /**
     * 搜索框自动补全接口
     * @param key
     * @return
     */

    List<String> suggestion(String key);

    /**
     * 新增数据
     * @param id
     */

    void insertById(Long id);

    /**
     * 删除数据
     * @param id
     */

    void deleteById(Long id);

}
