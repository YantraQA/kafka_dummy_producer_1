package com.yantraQA;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.service.FakeValuesService;
import com.github.javafaker.service.RandomService;
import com.yantraQA.model.HouseData;
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
    static List<HouseData>_records_house_data = new ArrayList<>();

    public static void main(String[] args) throws IOException, InterruptedException {
        log.info("Hiting Kafka Producer API: ' " + _producerUrl + " '");

        Map<String,String> header = new HashMap<>();
        header.put("accept","*/*");
        header.put("Content-Type","application/json");

        //Read the CSV file and get resul in arrayList
        InjectHousingData injectHousingData = new InjectHousingData();
        injectHousingData.readDataReturnListOfHouseData();

        for (int i=1;i<_records_house_data.size();i++){
            try{
                log.info("Trying Injecting Seq number: " + i + " (waiting for response...)");
                Msg msgObj = new Msg(_records_house_data.get(i));
                ObjectMapper objectMapper = new ObjectMapper();
                String bodyString = objectMapper.writeValueAsString(msgObj).replace("_1stFlrSF","1stFlrSF").replace("_2ndFlrSF","2ndFlrSF").replace("_3SsnPorch","3SsnPorch");
                log.info(bodyString);
                int code = given()
                        .headers(header).body(bodyString)
                        .when()
                        .post(_producerUrl).getStatusCode();

                log.info("Kafka Response Code: " + code + " for record id: " + _records_house_data.get(i));
                Thread.sleep(2000);
            }catch(Exception e){
                Thread.sleep(2000);
                log.fatal("Exception thrown for entry: " + _records_house_data.get(i));
                //e.printStackTrace();
            }

        }
    }

    public void readDataReturnListOfHouseData() throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream("test_housing.csv");
        InputStreamReader streamReader =
                new InputStreamReader(inputStream, StandardCharsets.UTF_8);

        try (BufferedReader br = new BufferedReader(streamReader)) {
            String line;
            while ((line = br.readLine()) != null) {
                _records_house_data.add(new HouseData(line.split(",")[0],line.split(",")[1],line.split(",")[2],line.split(",")[3],line.split(",")[4],line.split(",")[5],line.split(",")[6],line.split(",")[7],line.split(",")[8],line.split(",")[9],line.split(",")[10],line.split(",")[11],line.split(",")[12],line.split(",")[13],line.split(",")[14],line.split(",")[15],line.split(",")[16],line.split(",")[17],line.split(",")[18],line.split(",")[19],line.split(",")[20],line.split(",")[21],line.split(",")[22],line.split(",")[23],line.split(",")[24],line.split(",")[25],line.split(",")[26],line.split(",")[27],line.split(",")[28],line.split(",")[29],line.split(",")[30],line.split(",")[31],line.split(",")[32],line.split(",")[33],line.split(",")[34],line.split(",")[35],line.split(",")[36],line.split(",")[37],line.split(",")[38],line.split(",")[39],line.split(",")[40],line.split(",")[41],line.split(",")[42],line.split(",")[43],line.split(",")[44],line.split(",")[45],line.split(",")[46],line.split(",")[47],line.split(",")[48],line.split(",")[49],line.split(",")[50],line.split(",")[51],line.split(",")[52],line.split(",")[53],line.split(",")[54],line.split(",")[55],line.split(",")[56],line.split(",")[57],line.split(",")[58],line.split(",")[59],line.split(",")[60],line.split(",")[61],line.split(",")[62],line.split(",")[63],line.split(",")[64],line.split(",")[65],line.split(",")[66],line.split(",")[67],line.split(",")[68],line.split(",")[69],line.split(",")[70],line.split(",")[71],line.split(",")[72],line.split(",")[73],line.split(",")[74],line.split(",")[75],line.split(",")[76],line.split(",")[77],line.split(",")[78],line.split(",")[79]));
            }
        }
        log.info("CSV file read success");


    }





}
