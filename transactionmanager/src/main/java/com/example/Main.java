package com.example;

import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import akka.stream.ClosedShape;
import akka.stream.FanInShape2;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.ZipWith;
import lombok.extern.log4j.Log4j2;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;

@Log4j2
public class Main {

    private static final ActorSystem<String> ACTOR_SYSTEM = ActorSystem.create(Behaviors.empty(), "actor-system");

    public static void main(String[] args) {

        Map<Integer, Account> accounts = new HashMap<>();

        //set up accounts
        for (int i = 1; i <= 10; i++) {
            accounts.put(i, new Account(i, new BigDecimal(1000)));
        }

        //source to generate 1 transaction every second
        Source<Integer, NotUsed> source = Source.repeat(1)
                .throttle(1, Duration.ofSeconds(10));

        final Random random = new Random();
        //flow to create a random transfer
        Flow<Integer, Transfer, NotUsed> generateTransfer = Flow.of(Integer.class).map(x -> {
            int accountFrom = random.nextInt(9) + 1;
            int accountTo;
            do {
                accountTo = random.nextInt(9) + 1;
            } while (accountTo == accountFrom);

            BigDecimal amount = new BigDecimal(random.nextInt(100000)).divide(new BigDecimal(100));
            Date date = new Date();

            Transaction from = new Transaction(accountFrom, BigDecimal.ZERO.subtract(amount), date);
            Transaction to = new Transaction(accountTo, amount, date);
            return new Transfer(from, to);
        });

        Flow<Transfer, Transaction, NotUsed> transactionsFromTransfer = Flow.of(Transfer.class)
                        .mapConcat(transfer -> List.of(transfer.getFrom(), transfer.getTo()));

        Source<Integer, NotUsed> transactionIDsSource = Source.fromIterator(() ->
                Stream.iterate(1, i -> i + 1).limit(10).iterator());

        Sink.foreach((Transfer transfer) ->  {
            log.info("tranfer from {} to {} of{}", transfer.getFrom().getAccountNumber(),
                    transfer.getTo().getAccountNumber(), transfer.getFrom().getAmount());
        });

        RunnableGraph.fromGraph(
                GraphDSL.create(Sink.foreach(log::info), (builder, out) -> {

                    FanInShape2<Transaction, Integer, Transaction> assignTransaction = builder.add(
                            ZipWith.create((trans, id) -> {
                                trans.setUniqueId(id);
                                return trans;
                            }));

                    builder.from(builder.add(source))
                            .via(builder.add(generateTransfer))
                            .via(builder.add(Flow.of(Transfer.class)
                                    .mapConcat(transfer -> List.of(transfer.getFrom(), transfer.getTo())
                                    )
                            ))
                            .toInlet(assignTransaction.in0());

                    builder.from(builder.add(
                                    Source.fromIterator(() -> Stream.iterate(1, i -> i + 1)
                                            .iterator()
                                    )
                            )
                    ).toInlet(assignTransaction.in1());


                    builder.from(assignTransaction.out())
                            .to(out);

                    return ClosedShape.getInstance();
                })
        ).run(ACTOR_SYSTEM);
    }
}
