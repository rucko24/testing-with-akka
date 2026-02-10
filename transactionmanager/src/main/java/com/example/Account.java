package com.example;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Account {

    private int id;
    private BigDecimal balance;
    private List<Transaction> transactions;

    public Account(int id, BigDecimal openingBalance) {
        synchronized (this) {
            this.id = id;
            balance = openingBalance;
            transactions = new ArrayList<>();
            transactions.add(new Transaction(this.id, balance, new Date()));
        }
    }

    public void addTransaction(Transaction transaction) {
        if (transaction.getUniqueId() < 0) throw new RuntimeException("com.example.Transaction does not have a valid ID");
        if (transaction.getAccountNumber() != id) throw new RuntimeException("The transaction is not for this account");
        synchronized (this) {
            transactions.add(transaction);
            balance = balance.add(transaction.getAmount());
        }
    }

    public BigDecimal getBalance() {
        return balance;
    }

    public int getId() {
        return id;
    }
}
