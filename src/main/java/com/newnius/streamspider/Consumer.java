package com.newnius.streamspider;

import com.google.gson.Gson;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.newnius.streamspider.model.MQMessage;
import com.rabbitmq.client.*;
import org.bson.Document;

import java.io.IOException;
import java.util.Date;

/**
 * Created by newnius on 4/20/17.
 *
 */
public class Consumer {
    private static MongoDatabase mongoDatabase;

    public static void main(String[] args) {
        try {
            // 连接到 mongodb 服务
            MongoClient mongoClient = new MongoClient("ss-mongo", 27017);
            // 连接到数据库
            mongoDatabase = mongoClient.getDatabase("StreamSpider");
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            String QUEUE_NAME = "ss-pages";
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("ss-rabbitmq");
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

            com.rabbitmq.client.Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                        throws IOException {
                    String message = new String(body, "UTF-8");
                    //System.out.println(" [x] Received '" + message + "'");
                    save(message);
                }
            };
            channel.basicConsume(QUEUE_NAME, true, consumer);
        }catch (Exception ex){
            ex.printStackTrace();
        }
    }

    private static void save(String message){
        try {
            MQMessage msg = new Gson().fromJson(message, MQMessage.class);
            //System.out.println("Saving "+msg.getUrl()+","+ (new Date()));
            MongoCollection<Document> collection = mongoDatabase.getCollection("pages");
            Document document = new Document("url", msg.getUrl());
            Document newDocument = new Document("url", msg.getUrl()).append("html", msg.getHtml()).append("last_update", msg.getTime()).append("charset", msg.getCharset());
            FindIterable<Document> ite = collection.find(document).limit(1);
            if (ite.first() == null) {
                collection.insertOne(newDocument);
                System.out.println("INSERT "+msg.getUrl()+" ("+ (new Date())+")");
            } else {
                collection.replaceOne(ite.first(), newDocument);
                System.out.println("REPLACE "+msg.getUrl()+" ("+ (new Date())+")");
            }
        }catch (Exception ex){
            ex.printStackTrace();
        }
    }
}
