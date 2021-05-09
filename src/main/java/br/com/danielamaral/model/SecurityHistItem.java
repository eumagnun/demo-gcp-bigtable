package br.com.danielamaral.model;

public class SecurityHistItem {

    private String security;
    private String time;
    private String close;
    private String volume;
    private String open;
    private String high;
    private String low;

    public SecurityHistItem(String security, String time,String close, String volume, String open, String high, String low) {

        this.security = security;
        this.time = time;
        this.close = close;
        this.volume = volume;
        this.open = open;
        this.high = high;
        this.low = low;
    }

    public String getSecurity() {
        return security;
    }

    public void setSecurity(String security) {
        this.security = security;
    }

    public String getClose() {
        return close;
    }

    public void setClose(String close) {
        this.close = close;
    }

    public String getVolume() {
        return volume;
    }

    public void setVolume(String volume) {
        this.volume = volume;
    }

    public String getOpen() {
        return open;
    }

    public void setOpen(String open) {
        this.open = open;
    }

    public String getHigh() {
        return high;
    }

    public void setHigh(String high) {
        this.high = high;
    }

    public String getLow() {
        return low;
    }

    public void setLow(String low) {
        this.low = low;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return "SecurityHistItem{" +
                "security='" + security + '\'' +
                ", time='" + time + '\'' +
                ", close='" + close + '\'' +
                ", volume='" + volume + '\'' +
                ", open='" + open + '\'' +
                ", high='" + high + '\'' +
                ", low='" + low + '\'' +
                '}';
    }
}
