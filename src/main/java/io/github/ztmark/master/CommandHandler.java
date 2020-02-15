package io.github.ztmark.master;

import java.util.logging.Logger;

import io.github.ztmark.common.Command;
import io.github.ztmark.common.CommandBody;
import io.github.ztmark.common.CommandCode;
import io.github.ztmark.common.FetchJob;
import io.github.ztmark.common.FetchJobResp;
import io.github.ztmark.common.HeartBeat;
import io.github.ztmark.common.Job;
import io.github.ztmark.common.Registration;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * @Author: Mark
 * @Date : 2020/2/14
 */
public class CommandHandler extends SimpleChannelInboundHandler<Command> {

    private Logger logger = Logger.getLogger(HeartBeat.class.getName());

    private Master master;

    public CommandHandler(Master master) {
        this.master = master;
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
                break;
            case CommandCode.FETCH_JOB:
                processFetchJob(command, ctx);

        }
    }

    public void processHeartBeat(HeartBeat heartBeat) {
        master.ping(heartBeat.getWorkerId());
    }

    public void processRegistration(Registration registration, ChannelHandlerContext ctx) {
        master.addWorker(registration.getWorkerId(), ctx.channel());
    }

    public void processFetchJob(Command command, ChannelHandlerContext ctx) {
        FetchJob fetchJob = (FetchJob) command.getBody();
        final Job job = master.fetchJob(fetchJob.getWorkerId());
        final FetchJobResp fetchJobResp = new FetchJobResp(job);
        final Command resp = new Command(CommandCode.FETCH_JOB_RESP, fetchJobResp);
        resp.setCommandId(command.getCommandId());
        ctx.channel().writeAndFlush(resp);
    }
}
