package org.example.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;
import org.example.postgres.PostgresService;


import java.sql.*;
import java.util.Random;

public class Message {

    public final String firstName;
    public final String lastName;
    public int cip;
    public Double price;

    public int pharmacyId;



    public Message() {
        Faker faker = new Faker();
        this.firstName = faker.name().firstName();
        this.lastName = faker.name().lastName();
        this.getDBData();
        this.setPharmacy();
    }

    /**
     * this method picks a random pharmacy from the database
     * and sets the pharmacyId to the id of the pharmacy
     */
    private void setPharmacy() {
        try {
            PostgresService postgresService = new PostgresService();
            ResultSet rs = postgresService.getPharmacies();
            while (rs.next()) {
                this.pharmacyId = Integer.parseInt(rs.getString("id"));
            }
            postgresService.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void getDBData() {
        try {
            PostgresService postgresService = new PostgresService();
            ResultSet rs = postgresService.getDrugs();
            while (rs.next()) {
                this.cip = Integer.parseInt(rs.getString("cip"));
                var tmpPrice = Double.parseDouble(rs.getString("prix"));
                double variation = 0.1 * tmpPrice;
                Random random = new Random();
                this.price = Math.round((tmpPrice + (random.nextBoolean() ? variation : -variation)) * 100.0) / 100.0;
            }
            postgresService.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public String toString() {
        return "Message{" +
                "firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", cip=" + cip +
                ", price=" + price +
                '}';
    }

    public static String createMessage() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
//        mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
//        mapper.setSerializationConfig(mapper.getSerializationConfig().withVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY));
        String json = null;
        var mgs = new Message();
        json = mapper.writeValueAsString(mgs);
        return json;
    }



}
