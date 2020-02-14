package io.github.ztmark;

import java.net.InetSocketAddress;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * @Author: Mark
 * @Date : 2020/2/12
 */
public class MasterServer {

    public MasterServer() throws InterruptedException {
        final ServerBootstrap bootstrap = new ServerBootstrap();
        final NioEventLoopGroup boss = new NioEventLoopGroup(1, new NamedThreadFactory("Master-Boss"));
        final NioEventLoopGroup worker = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors(), new NamedThreadFactory("Master-Worker"));
        bootstrap.group(boss, worker)
                 .channel(NioServerSocketChannel.class)
                 .option(ChannelOption.SO_BACKLOG, 1024)
                 .option(ChannelOption.SO_REUSEADDR, true)
                 .option(ChannelOption.SO_KEEPALIVE, true)
                 .option(ChannelOption.TCP_NODELAY, true)
                 .option(ChannelOption.SO_SNDBUF, 65535)
                 .option(ChannelOption.SO_RCVBUF, 65535)
                 .localAddress(new InetSocketAddress(8000))
                 .childHandler(new ChannelInitializer<SocketChannel>() {
                     @Override
                     protected void initChannel(SocketChannel ch) throws Exception {
                         ch.pipeline().addLast(new NettyEncoder(), new NettyDecoder(), new HeartBeatHandler());
                     }
                 });
        final ChannelFuture future = bootstrap.bind().sync();
        future.addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
        System.out.println("master start at 8000");
    }


    public void start() {

    }


    public void stop() {

    }

}
