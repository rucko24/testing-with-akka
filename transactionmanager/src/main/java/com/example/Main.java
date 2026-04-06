package com.example;

import akka.Done;
import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.stream.FanInShape2;
import akka.stream.Graph;
import akka.stream.SourceShape;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.ZipWith;
import akka.stream.typed.javadsl.ActorFlow;
import akka.stream.typed.javadsl.ActorSink;
import lombok.extern.log4j.Log4j2;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

@Log4j2
public class Main {

    public static void main(String[] args) {

        //source to generate 1 transaction every second
        Source<Integer, NotUsed> source = Source.repeat(1)
                .throttle(1, Duration.ofSeconds(3));

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

        Flow<Transfer, Transaction, NotUsed> getTransactionsFromTransfer = Flow.of(Transfer.class)
                .mapConcat(transfer -> List.of(transfer.getFrom(), transfer.getTo()));

        Source<Integer, NotUsed> transactionIDsSource = Source.fromIterator(() ->
                Stream.iterate(1, i -> i + 1).limit(10).iterator());

        Sink<Transfer, CompletionStage<Done>> transferLogger = Sink.foreach((Transfer transfer) -> {
            log.info("tranfer from {} to {} of {}", transfer.getFrom().getAccountNumber(),
                    transfer.getTo().getAccountNumber(), transfer.getFrom().getAmount());
        });

        Graph<SourceShape<Transaction>, NotUsed> sourcePartialGraph = GraphDSL.create(
                builder -> {

                    FanInShape2<Transaction, Integer, Transaction> assignTransactionIDs = builder.add(
                            ZipWith.create((trans, id) -> {
                                trans.setUniqueId(id);
                                return trans;
                            }));

                    builder.from(builder.add(source))
                            .via(builder.add(generateTransfer.alsoTo(transferLogger)))
                            .via(builder.add(getTransactionsFromTransfer))
                            .toInlet(assignTransactionIDs.in0());

                    builder.from(builder.add(transactionIDsSource))
                            .toInlet(assignTransactionIDs.in1());

                    return SourceShape.of(assignTransactionIDs.out());
                }
        );

        final ActorSystem<AccountManager.AccountManagerCommand> accountManager = ActorSystem.create(
                AccountManager.create(), "account-manager"
        );

        Flow<Transaction, AccountManager.AddTransactionResponse, NotUsed> attempToApplyTransaction =
                ActorFlow.ask(accountManager, Duration.ofSeconds(10), AccountManager.AddTransactionCommand::new);

        Sink<AccountManager.AddTransactionResponse, CompletionStage<Done>> rejectedTransactionsSink = Sink.foreach(
                (trans) -> log.info("REJECTED transaction {}", trans.getTransaction()));

        Flow<AccountManager.AddTransactionResponse, AccountManager.AccountManagerCommand, NotUsed> receiveResult =
                Flow.of(AccountManager.AddTransactionResponse.class)
                        .map(result -> {
                            log.info("Logging {}", result.getTransaction());
                            return new AccountManager.DisplayBalanceCommand(result.getTransaction().getAccountNumber());
                        });

        Sink<AccountManager.AccountManagerCommand, NotUsed> displayBalanceSink =
                ActorSink.actorRef(accountManager, new AccountManager.CompleteCommand(),
                        (throwable) -> new AccountManager.FailedCommand());

        Source.fromGraph(sourcePartialGraph)
                .via(attempToApplyTransaction
                        .divertTo(rejectedTransactionsSink, result -> !result.getSucceeded()))
                .via(receiveResult)
                .to(displayBalanceSink)
                .run(accountManager);

    }
}
