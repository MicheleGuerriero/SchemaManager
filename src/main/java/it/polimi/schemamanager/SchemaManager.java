package it.polimi.schemamanager;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import it.polimi.schemamanager.exception.SchemaException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.ReadOnlyFileSystemException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.io.*;
import org.apache.avro.reflect.ReflectData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SchemaManager 
{
    
    private static final Logger logger = LoggerFactory
            .getLogger(SchemaManager.class);
    
    public static void main( String[] args ) throws IOException, RestClientException
    {
        
        // identityMapaCapacity says how many schema can be registered under the same subject
        SchemaRegistryClient registryClient = new CachedSchemaRegistryClient("http://localhost:8081", 10);
        
        SchemaManager manager = new SchemaManager();
        
        /*
        Schema city = SchemaBuilder
                .record("City").namespace("it.polimi.mydb")
                .fields()
                    .requiredString("name")
                    .optionalInt("popSize")
                .endRecord();
        
        Schema date = SchemaBuilder
                .record("Date").namespace("it.polimi.mydb")
                .fields()
                    .optionalInt("Day")
                    .optionalInt("Month")
                    .optionalInt("Year")
                .endRecord();  
        
        Schema task = SchemaBuilder
                .record("Task").namespace("it.polimi.mydb")
                .fields()
                    .requiredString("name")
                    .name("deadline").type(date).noDefault()
                .endRecord();             
                
        Schema user = SchemaBuilder
        .record("User").namespace("it.polimi.mydb")
        .fields()
          .requiredString("name")
          .optionalInt("age")
          .name("city").type(city).noDefault()
          .name("tasks").type().array().items(task).noDefault()
        .endRecord();
                
        int i = registryClient.register("mydb", user);

        
        String returned = readFile("/Users/michele/workspace/schema-manager/sample.json");
                
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(returned);
        
        System.out.println(schema.toString());
        
        PrintWriter writer = new PrintWriter("/Users/michele/workspace/schema-manager/src/main/avro/sample-schema.avsc", "UTF-8");
        writer.print(manager.getSchemaFromPojo(AnotherPojo.class));
        writer.close();
        */

        
        System.out.println(manager.getSchemaFromPojo(AnotherPojo.class));
        AnotherPojo obj = new AnotherPojo();
        
    }
    
    private static String readFile(String file) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader (file));
        String         line = null;
        StringBuilder  stringBuilder = new StringBuilder();
        String         ls = System.getProperty("line.separator");

        try {
            while((line = reader.readLine()) != null) {
                stringBuilder.append(line);
                stringBuilder.append(ls);
            }

            return stringBuilder.toString();
        } finally {
            reader.close();
        }
    }
    
    public Schema getSchemaFromAvroJsonString(String schemaString){
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(schemaString);
    }
    
    public Schema getSchemaFromAvroJsonFile(File file) throws SchemaException{
        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode actualObj = mapper.readTree(file);
            return this.getSchemaFromAvroJsonString(actualObj.asText());
        } catch (IOException e) {
            logger.error("There were problems reading the schema from the input file. Check the file contains a valid Avro JSON schema.", e.getMessage());
            throw new SchemaException(e.getMessage());
        }
    }
    
    public Schema getSchemaFromPojo(Class pojo){
        return ReflectData.get().getSchema(pojo);
    }
    
    public Schema getSchemaFromId(int id){
        return null;
    }
    
    public Boolean storeSchema(Schema toStore){
        return null;
    }
}