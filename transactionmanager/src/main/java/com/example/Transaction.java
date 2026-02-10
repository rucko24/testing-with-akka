package com.example;

import java.math.BigDecimal;
import java.util.Date;

public class Transaction {

    private int uniqueId;
    private int accountNumber;
    private BigDecimal amount;
    private Date date;

    public Transaction(int accountNumber, BigDecimal amount, Date date) {
        this.accountNumber = accountNumber;
        this.amount = amount;
        this.date = date;
        this.uniqueId = -1;
    }

    public void setUniqueId(int uniqueId) {
        this.uniqueId = uniqueId;
    }

    public int getUniqueId() {
        return uniqueId;
    }

    public int getAccountNumber() {
        return accountNumber;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public Date getDate() {
        return date;
    }

    public String toString() {
        return "ID : " + uniqueId + " com.example.Account : " + accountNumber + " Amount : " + amount + " Date : " + date;
    }
}
