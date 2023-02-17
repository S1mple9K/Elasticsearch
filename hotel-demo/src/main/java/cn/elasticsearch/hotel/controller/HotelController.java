package cn.elasticsearch.hotel.controller;

import cn.elasticsearch.hotel.pojo.PageResult;
import cn.elasticsearch.hotel.pojo.RequestParams;
import cn.elasticsearch.hotel.service.HotelService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;


/**
 * @author 9K
 */
@Slf4j
@RestController
@RequestMapping("/hotel")
public class HotelController {

    @Autowired
    private HotelService hotelService;

    @PostMapping("/list")
    public PageResult search(@RequestBody RequestParams requestParams){
        return hotelService.search(requestParams);
    }

    @PostMapping("/filters")
    public Map<String, List<String>> filters(@RequestBody RequestParams requestParams){
        return hotelService.filters(requestParams);
    }

    @GetMapping("/suggestion")
    public List<String> suggestion(@RequestParam("key") String key){
        return hotelService.suggestion(key);
    }

}
