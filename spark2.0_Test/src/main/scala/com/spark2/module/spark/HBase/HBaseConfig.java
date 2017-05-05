package com.spark2.module.spark.HBase;

/**
 * Created by cluster on 2017/4/6.
 */
public class HBaseConfig implements java.io.Serializable {

    private String Quorum = "192.168.31.63,192.168.31.61,192.168.31.62";
    private String Port = "2181";
    private String IsDistributed = "true";

    public String getQuorum() {
        return Quorum;
    }

    public void setQuorum(String quorum) {
        Quorum = quorum;
    }

    public String getIsDistributed() {
        return IsDistributed;
    }

    public void setIsDistributed(String isDistributed) {
        IsDistributed = isDistributed;
    }

    public String getPort() {
        return Port;
    }

    public void setPort(String port) {
        Port = port;
    }
}