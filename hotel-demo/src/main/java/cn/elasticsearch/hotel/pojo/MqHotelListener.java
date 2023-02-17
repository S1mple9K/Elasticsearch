package cn.elasticsearch.hotel.pojo;

import cn.elasticsearch.hotel.constants.MqConstants;
import cn.elasticsearch.hotel.service.HotelService;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * mq消息监听
 * @author 9K
 */
@Component
public class MqHotelListener {

    @Autowired
    private HotelService hotelService;

    /**
     * 监听酒店新增或修改的业务
     * @param id
     */

    @RabbitListener(queues = MqConstants.HOTEL_INSERT_QUEUE)
    public void listenerInsertOrUpdate(Long id){
        hotelService.insertById(id);
    }

    /**
     * 监听酒店删除的业务
     * @param id
     */
    @RabbitListener(queues = MqConstants.HOTEL_DELETE_QUEUE)
    public void listenerDelete(Long id){
        hotelService.deleteById(id);
    }
}
