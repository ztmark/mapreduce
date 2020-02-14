package io.github.ztmark.master;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import io.github.ztmark.common.NamedThreadFactory;
import io.github.ztmark.common.NettyDecoder;
import io.github.ztmark.common.NettyEncoder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * @Author: Mark
 * @Date : 2020/2/12
 */
public class MasterServer {

    private Logger logger = Logger.getLogger(MasterServer.class.getName());

    private ServerBootstrap bootstrap;
    private NioEventLoopGroup boss;
    private NioEventLoopGroup worker;

    private Map<String, ChannelWrapper> workers;

    public MasterServer() {
        workers = new ConcurrentHashMap<>();
    }


    public void start() throws InterruptedException {
        bootstrap = new ServerBootstrap();
        boss = new NioEventLoopGroup(1, new NamedThreadFactory("Master-Boss"));
        worker = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors(), new NamedThreadFactory("Master-Worker"));
        bootstrap.group(boss, worker)
                 .channel(NioServerSocketChannel.class)
                 .localAddress(new InetSocketAddress(8000))
                 .childHandler(new ChannelInitializer<SocketChannel>() {
                     @Override
                     protected void initChannel(SocketChannel ch) throws Exception {
                         ch.pipeline().addLast(new NettyEncoder(), new NettyDecoder(), new CommandHandler(MasterServer.this));
                     }
                 });
        final ChannelFuture future = bootstrap.bind().sync();
        future.addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
        logger.info("master start at 8000");
    }


    public void stop() {

    }

    public void addWorker(String workerId, Channel channel) {
        final ChannelWrapper channelWrapper = new ChannelWrapper();
        channelWrapper.setChannel(channel);
        channelWrapper.setLastPingTime(System.currentTimeMillis());
        workers.put(workerId, channelWrapper);
    }

    public void ping(String workerId) {
        final ChannelWrapper channelWrapper = workers.get(workerId);
        if (channelWrapper != null) {
            channelWrapper.setLastPingTime(System.currentTimeMillis());
        }
    }

}
