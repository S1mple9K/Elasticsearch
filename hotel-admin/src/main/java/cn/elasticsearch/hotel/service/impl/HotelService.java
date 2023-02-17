package cn.elasticsearch.hotel.service.impl;

import cn.elasticsearch.hotel.mapper.HotelMapper;
import cn.elasticsearch.hotel.pojo.Hotel;
import cn.elasticsearch.hotel.service.IHotelService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

@Service
public class HotelService extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {
}
