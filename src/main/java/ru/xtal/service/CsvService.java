package ru.xtal.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;

@Service
public class CsvService {

    @Autowired
    private Environment environment;

    public void serveFile(){
        Jedis jedis = new Jedis(environment.getProperty("redis.connection"));
        jedis.rpush("queue", "sdfsdf");
    }

    public void recieve(){
        Jedis jedis = new Jedis(environment.getProperty("redis.connection"));
        while(true){
            System.out.println("Waiting for a message in thequeue");
            System.out.println(jedis.blpop(0,"queue"));
        }
    }
}
