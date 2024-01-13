package com.sid.demospringcloudstreamskafka.services;

import com.sid.demospringcloudstreamskafka.entities.PageEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Date;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@Configuration
public class pageEventService {
    @Bean
    public Consumer<PageEvent> pageEventConsumer(){
        return (input)->{
            System.out.println("*****************************");
            System.out.println(input.toString());
            System.out.println("*****************************");
        };
    }


    //Il joue ke role d'un producer
    //Chaque seconde on envoie un message vers le topic R2
    @Bean
    public Supplier<PageEvent> pageEventSupplier(){
        return ()->new PageEvent(Math.random()>0.5?"Darhoni1":"Darhoni2",Math.random()>0.5?"Othmane1":"Othmane2",new Date(), new Random().nextInt(9000));
    }

    @Bean
    public Function<PageEvent, PageEvent> PageEventFunction(){
        return (input)->{
            input.setName("L:"+input.getName().length());
            input.setUser("UUUUU");
            return input;
        };
    }

    @Bean
    public Function<KStream<String,PageEvent>,KStream<String,Long>> kStreamFunction(){
        return (input)->{
            return input
                    .filter((k,v)->v.getDuration()>100)
                    .map((k,v)->new KeyValue<>(v.getName(),0L))
                    .groupBy((k,v)->k, Grouped.with(Serdes.String(),Serdes.Long()))
                    .windowedBy(TimeWindows.of(Duration.ofDays(5000)))
                    .count(Materialized.as("page-count"))//stocker les resultats dans un table dans memoire
                    .toStream()
                    .map((k,v)->new KeyValue<>("=>"+k.window().startTime()+k.window().endTime()+k.key(),v));
        };
    }


}
