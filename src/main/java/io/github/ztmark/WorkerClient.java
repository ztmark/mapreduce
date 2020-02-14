package io.github.ztmark;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * @Author: Mark
 * @Date : 2020/2/14
 */
public class WorkerClient {

    private Bootstrap bootstrap;
    private NioEventLoopGroup workerGroup;
    private ChannelFuture channelFuture;

    public WorkerClient() {
        bootstrap = new Bootstrap();
        workerGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors(),
                new NamedThreadFactory("worker-client-group"));
        bootstrap.group(workerGroup)
                 .channel(NioSocketChannel.class)
                 .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 6000)
                 .handler(new ChannelInitializer<SocketChannel>() {
                     @Override
                     protected void initChannel(SocketChannel ch) throws Exception {
                         ch.pipeline().addLast(new NettyDecoder(), new NettyEncoder());
                     }
                 });
        channelFuture = bootstrap.connect("localhost", 8000);
        System.out.println("connect to master");
    }

    public void sendHeartBeat(Command command) {
        channelFuture.channel().writeAndFlush(command);
    }
}
