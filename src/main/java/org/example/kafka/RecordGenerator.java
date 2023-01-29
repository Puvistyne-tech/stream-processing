package org.example.kafka;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.example.client.Producer;
import org.example.postgres.PostgresService;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.Random;

/**
 * this class generates a random record with a random client, a random drug and a random pharmacy
 * it implements the SpecificRecord interface to be able to use the avro generated classes
 * it implements the GenericRecord interface to be able to use the avro generated classes
 */
public class RecordGenerator implements SpecificRecord, GenericRecord {

    public String firstName;
    public String lastName;
    public int cip;
    public double price;
    public int pharmacyId;

    /**
     * this method generates a random record with a random client, a random drug and a random pharmacy
     */
    public RecordGenerator() {
        Faker faker = new Faker();
        this.firstName = faker.name().firstName();
        this.lastName = faker.name().lastName();
        serDrug();
        setPharmacy();
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

    /**
     * this method picks a random drug from the database and generates a price with 5% of variation
     * sets the cip and price of the drug
     */
    private void serDrug() {
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
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * this method generates a random client
     */
    @Override
    public String toString() {
        return "Message{" +
                "firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", cip=" + cip +
                ", price=" + price +
                ", pharmacyId=" + pharmacyId +
                '}';
    }

    /**
     * this method generates a json method of this class
     *
     * @return a json message
     * @throws JsonProcessingException if the json message cannot be generated
     */
//    @JsonValue
    String createJson() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        var json = mapper.writeValueAsString(this);
        System.out.println(json);
        return json;
//        return mapper.writeValueAsString(this);
    }




    /**
     * this method generates a random avro record from the given schema
     *
     * @param schema the schema to use
     * @return a random avro record
     */
    public static GenericRecord generateAvroRecord(Schema schema) {
        var random = new RecordGenerator();
        GenericRecord record = new GenericData.Record(schema);
        record.put("firstName", random.firstName);
        record.put("lastName", random.lastName);
        record.put("cip", random.cip);
        record.put("price", random.price);
        record.put("pharmacyId", random.pharmacyId);
        return record;
    }

    /**
     * this method generates a random avro record from the given schema
     *
     * @param i the index of the field to set
     * @param v the value to set
     */
    @Override
    public void put(int i, Object v) {
        switch (i) {
            case 0 -> firstName = String.valueOf(v);
            case 1 -> lastName = String.valueOf(v);
            case 2 -> cip = (int) v;
            case 3 -> price = (double) v;
            case 4 -> pharmacyId = (int) v;
            default -> throw new AvroRuntimeException("Invalid index: " + i);
        }
    }

    /**
     * this method gets the value of the field at the given index
     *
     * @param i the index of the field to get
     * @return the value of the field at the given index
     */
    @Override
    public Object get(int i) {
        return switch (i) {
            case 0 -> firstName;
            case 1 -> lastName;
            case 2 -> cip;
            case 3 -> price;
            case 4 -> pharmacyId;
            default -> throw new AvroRuntimeException("Invalid index: " + i);
        };
    }


    /**
     * this method gets the schema of the record
     *
     * @return the schema of the record
     */
    @Override
    public Schema getSchema() {
        try {
            return new Schema.Parser().parse(Producer.class.getClassLoader().getResourceAsStream("record.avsc"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public int getCip() {
        return cip;
    }

    public void setCip(int cip) {
        this.cip = cip;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public int getPharmacyId() {
        return pharmacyId;
    }

    public void setPharmacyId(int pharmacyId) {
        this.pharmacyId = pharmacyId;
    }

    /**
     * this method puts the given value in the field with the given key
     *
     * @param key the key of the field to set
     * @param v   the value to set
     */
    @Override
    public void put(String key, Object v) {

        switch (key) {
            case "firstName" -> firstName = String.valueOf(v);
            case "lastName" -> lastName = String.valueOf(v);
            case "cip" -> cip = (int) v;
            case "price" -> price = (double) v;
            case "pharmacyId" -> pharmacyId = (int) v;
            default -> throw new AvroRuntimeException("Invalid index: " + key);
        }

    }

    /**
     * this method gets the value of the field with the given key
     *
     * @param key the key of the field to get
     * @return the value of the field with the given key
     */
    @Override
    public Object get(String key) {
        return switch (key) {
            case "firstName" -> firstName;
            case "lastName" -> lastName;
            case "cip" -> cip;
            case "price" -> price;
            case "pharmacyId" -> pharmacyId;
            default -> throw new AvroRuntimeException("Invalid index: " + key);
        };
    }

    public static void main(String[] args) {
        Random random = new Random();
        double price = 100.0;
        double variation = 0.1 * price;
        double newPrice = price + (random.nextBoolean() ? variation : -variation);
        System.out.println("Original price: " + price + ", New price: " + newPrice);
    }

}
