package com.easy.study;


import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

/**
 * @auther Administrator jin
 * @create 2017/4/13
 */
public class ProducerKFK {
    private Producer<String,String> inner;
    public ProducerKFK() throws Exception{
        InputStream in = getClass().getResourceAsStream("/producer.properties");
        Properties properties = new Properties();
//        properties.load(ClassLoader.getSystemResourceAsStream("producer.properties"));
        properties.load(in);
        ProducerConfig config = new ProducerConfig(properties);
        inner = new Producer<String, String>(config);
    }


    public void send(String topicName,String message) {
        if(topicName == null || message == null){
            return;
        }
        KeyedMessage<String, String> km = new KeyedMessage(topicName,message);
        inner.send(km);
    }

    public void send(String topicName,Collection<String> messages) {
        if(topicName == null || messages == null){
            return;
        }
        if(messages.isEmpty()){
            return;
        }
        List<KeyedMessage<String, String>> kms = new ArrayList();
        for(String entry : messages){
            KeyedMessage<String, String> km = new KeyedMessage(topicName,entry);
            kms.add(km);
        }
        inner.send(kms);
    }

    public void close(){
        inner.close();
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        ProducerKFK producer = null;
        try{
            producer = new ProducerKFK();
            int i=0;
            while(true){
                System.err.println("this is a sample" + i);
                producer.send("helloWorld", "this is a sample" + i);
                i++;
                Thread.sleep(2000);
            }
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            if(producer != null){
                producer.close();
            }
        }

    }
}
