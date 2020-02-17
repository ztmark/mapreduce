package io.github.ztmark.worker;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import io.github.ztmark.common.Command;
import io.github.ztmark.common.CommandCode;
import io.github.ztmark.common.DoneJob;
import io.github.ztmark.common.FetchJob;
import io.github.ztmark.common.HeartBeat;
import io.github.ztmark.common.NamedThreadFactory;
import io.github.ztmark.common.NettyDecoder;
import io.github.ztmark.common.NettyEncoder;
import io.github.ztmark.common.Registration;
import io.github.ztmark.common.ResponseFuture;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
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

    private AtomicInteger requestIdSeq = new AtomicInteger(1);

    private Bootstrap bootstrap;
    private NioEventLoopGroup workerGroup;
    private ChannelFuture channelFuture;

    private Map<Integer, ResponseFuture> responseMap = new ConcurrentHashMap<>();

    public WorkerClient() throws InterruptedException {
        bootstrap = new Bootstrap();
        workerGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors(),
                new NamedThreadFactory("worker-client-group"));
        bootstrap.group(workerGroup)
                 .channel(NioSocketChannel.class)
                 .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 6000)
                 .handler(new ChannelInitializer<SocketChannel>() {
                     @Override
                     protected void initChannel(SocketChannel ch) throws Exception {
                         ch.pipeline().addLast(new NettyDecoder(), new NettyEncoder(), new ClientCommandHandler(WorkerClient.this));
                     }
                 });
        channelFuture = bootstrap.connect("localhost", 8000).sync();
        logger.info("connect to master " + channelFuture.isSuccess());
    }

    public void stop() {
        if (channelFuture != null) {
            channelFuture.cancel(true);
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
    }

    public boolean register(String workerId) throws InterruptedException {
        final Registration registration = new Registration(workerId);
        return sendOneWayCommand(new Command(CommandCode.REGISTRATION, registration));
    }

    public void sendHeartBeat(HeartBeat heartBeat) throws InterruptedException {
        sendOneWayCommand(new Command(CommandCode.HEART_BEAT, heartBeat));
    }

    public Command fetchJob(FetchJob fetchJob) throws InterruptedException {
        return sendSyncCommand(new Command(CommandCode.FETCH_JOB, fetchJob));
    }

    public void sendDoneJob(DoneJob doneJob) throws InterruptedException {
        int count = 5;
        while (sendOneWayCommand(new Command(CommandCode.DONE_JOB, doneJob)) && --count > 0) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
    }

    public void setResponse(Command command) {
        final ResponseFuture responseFuture = responseMap.get(command.getCommandId());
        if (responseFuture != null) {
            responseFuture.setResponse(command);
        }
    }

    private boolean sendOneWayCommand(Command command) throws InterruptedException {
        if (channelFuture != null && channelFuture.channel().isActive()) {
            final ChannelFuture sync = channelFuture.channel().writeAndFlush(command).sync();
            return sync.isSuccess();
        }
        return false;
    }

    private Command sendSyncCommand(Command command) throws InterruptedException {
        if (channelFuture != null && channelFuture.channel().isActive()) {
            command.setCommandId(requestIdSeq.getAndIncrement());
            final ResponseFuture responseFuture = new ResponseFuture();
            channelFuture.channel().writeAndFlush(command).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (future.isSuccess()) {
                        responseFuture.setSendSuccess(true);
                        responseFuture.setCommandId(command.getCommandId());
                        responseMap.put(command.getCommandId(), responseFuture);
                    } else {
                        responseFuture.setSendSuccess(false);
                        responseFuture.setSendCause(future.cause());
                    }
                }
            });
            final Command resp = responseFuture.await(6000, TimeUnit.MILLISECONDS);
            responseMap.remove(command.getCommandId());
            if (resp == null) {
                throw new RuntimeException(responseFuture.getSendCause());
            }
            return resp;
        }
        throw new RuntimeException("channel not active");
    }

}
