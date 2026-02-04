package com.example.demo;

import akka.Done;
import akka.NotUsed;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletionStage;

@Service
@Log4j2
public class SimpleStreamService {

    public Source<Integer, NotUsed> source() {
        Source<Integer, NotUsed> source = Source.range(1, 10);

        Sink<String, CompletionStage<Done>> sink = Sink.foreach(log::info);


        return source;
    }

}
