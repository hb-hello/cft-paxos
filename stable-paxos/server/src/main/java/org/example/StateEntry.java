package org.example;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAdder;

public final class StateEntry {
    private final String clientId;
    private final DoubleAdder bankBalance;
    private final AtomicLong lastTimestampRepliedTo;

    public StateEntry(String clientId, double bankBalance, long lastTimestampRepliedTo) {
        this.clientId = clientId;
        this.bankBalance = new DoubleAdder();
        this.bankBalance.add(bankBalance);
        this.lastTimestampRepliedTo = new AtomicLong(lastTimestampRepliedTo);
    }

    public StateEntry(String clientId, double bankBalance) {
        this(clientId, bankBalance, 0L);
    }

    public String getClientId() {
        return clientId;
    }

    public double getBankBalance() {
        return bankBalance.sum();
    }

    public long getLastTimestampRepliedTo() {
        return lastTimestampRepliedTo.get();
    }

    public void setBankBalance(double newBalance) {
        synchronized(bankBalance) {
            bankBalance.reset();
            bankBalance.add(newBalance);
        }
    }

    public void setLastTimestampRepliedTo(long timestamp) {
        lastTimestampRepliedTo.set(timestamp);
    }

    public boolean addToBankBalance(double amount) {
        if (amount < 0) {
            return false;
        }
        bankBalance.add(amount);
        return true;
    }

    public boolean subtractFromBankBalance(double amount) {
        if (amount < 0) {
            return false;
        }

        // We need to check balance and subtract atomically
        synchronized(bankBalance) {
            double currentBalance = bankBalance.sum();
            if (currentBalance >= amount) {
                bankBalance.add(-amount);
                return true;
            }
            return false;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (StateEntry) obj;
        return Objects.equals(this.clientId, that.clientId) &&
                Double.doubleToLongBits(this.getBankBalance()) == Double.doubleToLongBits(that.getBankBalance()) &&
                this.getLastTimestampRepliedTo() == that.getLastTimestampRepliedTo();
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientId, getBankBalance(), getLastTimestampRepliedTo());
    }

    @Override
    public String toString() {
        return "ClientRecord[" +
                "clientId=" + clientId + ", " +
                "bankBalance=" + getBankBalance() + ", " +
                "lastTimestampRepliedTo=" + getLastTimestampRepliedTo() + ']';
    }
}
