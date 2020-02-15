package io.github.ztmark.worker;

import io.github.ztmark.common.Command;
import io.github.ztmark.common.CommandCode;
import io.github.ztmark.common.FetchJobResp;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * @Author: Mark
 * @Date : 2020/2/15
 */
public class ClientCommandHandler extends SimpleChannelInboundHandler<Command> {

    private WorkerClient client;

    public ClientCommandHandler(WorkerClient client) {
        this.client = client;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Command msg) throws Exception {
        processCommand(ctx, msg);
    }

    private void processCommand(ChannelHandlerContext ctx, Command msg) {
        switch (msg.getCode()) {
            case CommandCode.FETCH_JOB_RESP:
                processFetchJobResp(msg);
        }
    }

    private void processFetchJobResp(Command command) {
        client.setResponse(command);
    }
}
