package org.example;

import java.util.Objects;

public final class StateEntry {
    private final String clientId;
    private double bankBalance;
    private long lastTimestampRepliedTo;

    public StateEntry(String clientId, double bankBalance, long lastTimestampRepliedTo) {
        this.clientId = clientId;
        this.bankBalance = bankBalance;
        this.lastTimestampRepliedTo = lastTimestampRepliedTo;
    }

    public StateEntry(String clientId, double bankBalance) {
        this(clientId, bankBalance, 0L);
    }

    public String getClientId() {
        return clientId;
    }

    public double getBankBalance() {
        return bankBalance;
    }

    public long getLastTimestampRepliedTo() {
        return lastTimestampRepliedTo;
    }

    public void setBankBalance(double newBalance) {
        bankBalance = newBalance;
    }

    public void setLastTimestampRepliedTo(long timestamp) {
        lastTimestampRepliedTo = timestamp;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (StateEntry) obj;
        return Objects.equals(this.clientId, that.clientId) &&
                Double.doubleToLongBits(this.bankBalance) == Double.doubleToLongBits(that.bankBalance) &&
                this.lastTimestampRepliedTo == that.lastTimestampRepliedTo;
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientId, bankBalance, lastTimestampRepliedTo);
    }

    @Override
    public String toString() {
        return "ClientRecord[" +
                "clientId=" + clientId + ", " +
                "bankBalance=" + bankBalance + ", " +
                "lastTimestampRepliedTo=" + lastTimestampRepliedTo + ']';
    }

}
