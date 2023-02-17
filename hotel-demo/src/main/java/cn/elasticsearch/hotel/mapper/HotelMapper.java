package cn.elasticsearch.hotel.mapper;

import cn.elasticsearch.hotel.pojo.Hotel;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;

/**
 * 酒店Mapper层
 * @author 9K
 */
@Mapper
public interface HotelMapper extends BaseMapper<Hotel> {
}
