package com.yantraQA;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.service.FakeValuesService;
import com.github.javafaker.service.RandomService;
import com.yantraQA.model.Msg;
import com.yantraQA.model.Notification;
import com.yantraQA.model.NotificationType;
import lombok.extern.log4j.Log4j2;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static io.restassured.RestAssured.given;

@Log4j2
public class InjectHousingData {

    static String  _producerUrl = "http://localhost:8001/sendForwardMsg";
    static List<String> _records = new ArrayList<>();

    public static void main(String[] args) throws IOException, InterruptedException {
        //FakeValuesService fakeValuesService = new FakeValuesService(new Locale("en-GB"),new RandomService());
        log.info("Hiting Kafka Producer API: ' " + _producerUrl + " '");
        Map<String,String> header = new HashMap<>();
        header.put("accept","*/*");
        header.put("Content-Type","application/json");

        //Read the CSV file and get resul in arrayList
        InjectHousingData injectHousingData = new InjectHousingData();
        injectHousingData.readDataFromCSVReturnArray();

        for (int i=1;i<_records.size();i++){
            try{
                log.info("Trying Injecting Seq number: " + i + " (waiting for response...)");
                Msg msgObj = new Msg(_records.get(i));
                ObjectMapper objectMapper = new ObjectMapper();
                String bodyString = objectMapper.writeValueAsString(msgObj);

                int code = given()
                        .headers(header).body(bodyString)
                        .when()
                        .post(_producerUrl).getStatusCode();

                log.info("Kafka Response Code: " + code + " for record id: " + _records.get(i));
                Thread.sleep(1000);
            }catch(Exception e){
                Thread.sleep(1000);
                log.fatal("Exception thrown for entry: " + _records.get(i));
                //e.printStackTrace();
            }

        }
    }

    public  void readDataFromCSVReturnArray() throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream("test_housing.csv");
        InputStreamReader streamReader =
                new InputStreamReader(inputStream, StandardCharsets.UTF_8);

        try (BufferedReader br = new BufferedReader(streamReader)) {
            String line;
            while ((line = br.readLine()) != null) {
                _records.add(line);
            }
        }

        log.info("CSV file read success");

    }
}
