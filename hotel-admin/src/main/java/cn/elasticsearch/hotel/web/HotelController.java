package cn.elasticsearch.hotel.web;

import cn.elasticsearch.hotel.constants.MqConstants;
import cn.elasticsearch.hotel.pojo.Hotel;
import cn.elasticsearch.hotel.pojo.PageResult;
import cn.elasticsearch.hotel.service.IHotelService;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.security.InvalidParameterException;

@RestController
@RequestMapping("hotel")
public class HotelController {

    @Autowired
    private IHotelService hotelService;

    /**
     * rabbitmqAPI
     */

    @Autowired
    private RabbitTemplate rabbitTemplate;


    @GetMapping("/{id}")
    public Hotel queryById(@PathVariable("id") Long id){
        return hotelService.getById(id);
    }

    @GetMapping("/list")
    public PageResult hotelList(
            @RequestParam(value = "page", defaultValue = "1") Integer page,
            @RequestParam(value = "size", defaultValue = "1") Integer size
    ){
        Page<Hotel> result = hotelService.page(new Page<>(page, size));

        return new PageResult(result.getTotal(), result.getRecords());
    }

    /**
     * 保存到数据库并发送mq
     * @param hotel
     */

    @PostMapping
    public void saveHotel(@RequestBody Hotel hotel){
        hotelService.save(hotel);
        rabbitTemplate.convertAndSend(MqConstants.HOTEL_EXCHANGE,MqConstants.HOTEL_INSERT_KEY,hotel.getId());
    }

    /**
     * 修改数据库并发送mq
     * @param hotel
     */

    @PutMapping()
    public void updateById(@RequestBody Hotel hotel){
        if (hotel.getId() == null) {
            throw new InvalidParameterException("id不能为空");
        }
        hotelService.updateById(hotel);
        rabbitTemplate.convertAndSend(MqConstants.HOTEL_EXCHANGE,MqConstants.HOTEL_INSERT_KEY,hotel.getId());
    }

    /**
     * 删除数据库并发送mq
     * @param id
     */

    @DeleteMapping("/{id}")
    public void deleteById(@PathVariable("id") Long id) {
        hotelService.removeById(id);
        rabbitTemplate.convertAndSend(MqConstants.HOTEL_EXCHANGE,MqConstants.HOTEL_DELETE_KEY,id);
    }
}
