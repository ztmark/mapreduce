package io.github.ztmark.master;


import io.netty.channel.Channel;

/**
 * @Author: Mark
 * @Date : 2020/2/14
 */
public class ChannelWrapper {

    private long lastPingTime;
    private Channel channel;

    public long getLastPingTime() {
        return lastPingTime;
    }

    public void setLastPingTime(long lastPingTime) {
        this.lastPingTime = lastPingTime;
    }

    public Channel getChannel() {
        return channel;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }
}
