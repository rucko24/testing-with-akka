package com.example;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

public class AccountManager extends AbstractBehavior<AccountManager.AccountManagerCommand> {

    public interface AccountManagerCommand {};

    public static class AddTransactionCommand implements AccountManagerCommand {

        private Transaction transaction;
        private ActorRef<AddTransactionResponse> sender;

        public AddTransactionCommand(Transaction transaction, ActorRef<AddTransactionResponse> sender) {
            this.transaction = transaction;
            this.sender = sender;
        }

        public Transaction getTransaction() {
            return transaction;
        }

        public ActorRef<AddTransactionResponse> getSender() {
            return sender;
        }
    }

    public static class DisplayBalanceCommand implements AccountManagerCommand {
        private int accountNo;

        public DisplayBalanceCommand(int accountNo) {
            this.accountNo = accountNo;
        }

        public int getAccountNo() {
            return accountNo;
        }

    }

    public static class CompleteCommand implements AccountManagerCommand {}
    public static class FailedCommand implements  AccountManagerCommand {}

    public class AddTransactionResponse {
        private Transaction transaction;
        private Boolean succeeded;

        public AddTransactionResponse(Transaction transaction, Boolean succeeded) {
            this.transaction = transaction;
            this.succeeded = succeeded;
        }

        public Transaction getTransaction() {
            return transaction;
        }

        public Boolean getSucceeded() {
            return succeeded;
        }
    }

    private AccountManager(ActorContext<AccountManagerCommand> context) {
        super(context);
        //set up accounts
        for (int i  = 1; i <= 10; i++) {
            accounts.put(i, new Account(i, new BigDecimal(1000)));
        }
    }

    public static Behavior<AccountManagerCommand> create() {
        return Behaviors.setup(AccountManager::new);
    }

    Map<Integer, Account> accounts = new HashMap<>();

    @Override
    public Receive<AccountManagerCommand> createReceive() {
        return newReceiveBuilder()

                .onMessage(AddTransactionCommand.class, msg -> {
                    Account account = accounts.get(msg.getTransaction().getAccountNumber());
                    if(account.getBalance().add(msg.transaction.getAmount()).compareTo(BigDecimal.ZERO) > 0) {
                        account.addTransaction(msg.getTransaction());
                        msg.getSender().tell(new AddTransactionResponse(msg.getTransaction(), true));
                    }
                    else {
                        msg.getSender().tell(new AddTransactionResponse(msg.getTransaction(), false));
                    }
                    return Behaviors.same();
                })
                .onMessage(DisplayBalanceCommand.class, msg -> {
                    Account account = accounts.get(msg.getAccountNo());
                    System.out.println("Account " + account.getId() + " now has balance " + account.getBalance());
                    return Behaviors.same();
                })
                .onMessage(CompleteCommand.class, msg -> {
                    return Behaviors.stopped();
                })
                .onMessage(FailedCommand.class, msg -> {
                    return Behaviors.stopped();
                })
                .build();
    }
}
