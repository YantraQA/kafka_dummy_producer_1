package com.yantraQA;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.service.FakeValuesService;
import com.github.javafaker.service.RandomService;
import com.yantraQA.model.Notification;
import com.yantraQA.model.NotificationType;
import io.restassured.response.Response;
import lombok.extern.log4j.Log4j2;
import lombok.extern.slf4j.Slf4j;

import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static io.restassured.RestAssured.given;

@Log4j2
public class EventProducer {

    public static void main(String[] args) throws JsonProcessingException, InterruptedException {
        FakeValuesService fakeValuesService = new FakeValuesService(new Locale("en-GB"),new RandomService());
        Map<String,String> header = new HashMap<>();
        header.put("accept","*/*");
        header.put("Content-Type","application/json");

        String topicName = "notification"; // this could be sent while triggering the container

        int timeOutInSec=1000;
        for (int i=0;i<timeOutInSec;i++){
            Long id = Long.parseLong(fakeValuesService.numerify("#############"));
            String content = (new Date()) + fakeValuesService.letterify("-DummyLogEvent-?????????????");
            NotificationType type = NotificationType.INFO;

            Notification notification = new Notification(id, topicName,content,type);

            ObjectMapper objectMapper = new ObjectMapper();
            String bodyString = objectMapper.writeValueAsString(notification);

            int code = given()
                    .headers(header).body(bodyString)
                    .when()
                    .post("http://localhost:8082/notification/send").getStatusCode();

            log.info("Kafka Response Code: " + code + " for log id: " + id);
            Thread.sleep(1000);
        }



    }
}
