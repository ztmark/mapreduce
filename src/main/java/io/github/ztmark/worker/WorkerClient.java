package io.github.ztmark.worker;

import java.util.logging.Logger;

import io.github.ztmark.common.Command;
import io.github.ztmark.common.CommandCode;
import io.github.ztmark.common.HeartBeat;
import io.github.ztmark.common.NamedThreadFactory;
import io.github.ztmark.common.NettyDecoder;
import io.github.ztmark.common.NettyEncoder;
import io.github.ztmark.common.Registration;
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

    private Logger logger = Logger.getLogger(Worker.class.getName());

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
        logger.info("connect to master");
    }

    public boolean register(String workerId) throws InterruptedException {
        final Registration registration = new Registration(workerId);
        return sendCommand(new Command(CommandCode.REGISTRATION, registration));
    }

    public void sendHeartBeat(HeartBeat heartBeat) throws InterruptedException {
        sendCommand(new Command(CommandCode.HEART_BEAT, heartBeat));
    }

    private boolean sendCommand(Command command) throws InterruptedException {
        if (channelFuture != null && channelFuture.channel().isWritable()) {
            channelFuture.channel().writeAndFlush(command).sync();
            return true;
        }
        return false;
    }
}
