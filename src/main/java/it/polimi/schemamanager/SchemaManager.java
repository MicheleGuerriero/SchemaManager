package it.polimi.schemamanager;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class SchemaManager 
{
    public static void main( String[] args ) throws IOException, RestClientException
    {
        
        // identityMapaCapacity says how many schema can be registered under the same subject
        SchemaRegistryClient registryClient = new CachedSchemaRegistryClient("http://localhost:8081", 10);
        
        
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
        
        registryClient.getByID(i);
        
    }
}