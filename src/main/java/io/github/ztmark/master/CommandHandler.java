package io.github.ztmark.master;

import java.util.logging.Logger;

import io.github.ztmark.common.Command;
import io.github.ztmark.common.CommandCode;
import io.github.ztmark.common.HeartBeat;
import io.github.ztmark.common.Registration;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * @Author: Mark
 * @Date : 2020/2/14
 */
public class CommandHandler extends SimpleChannelInboundHandler<Command> {

    private Logger logger = Logger.getLogger(HeartBeat.class.getName());

    private MasterServer masterServer;

    public CommandHandler(MasterServer masterServer) {
        this.masterServer = masterServer;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Command msg) throws Exception {
        processCommand(ctx, msg);
    }

    public void processCommand(ChannelHandlerContext ctx, Command command) {
        switch (command.getCode()) {
            case CommandCode.HEART_BEAT:
                processHeartBeat((HeartBeat) command.getBody());
                break;
            case CommandCode.REGISTRATION:
                processRegistration((Registration) command.getBody(), ctx);

        }
    }

    public void processHeartBeat(HeartBeat heartBeat) {
        masterServer.ping(heartBeat.getWorkerId());
    }

    public void processRegistration(Registration registration, ChannelHandlerContext ctx) {
        masterServer.addWorker(registration.getWorkerId(), ctx.channel());
    }
}
