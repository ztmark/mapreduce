package io.github.ztmark;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * @Author: Mark
 * @Date : 2020/2/14
 */
public class HeartBeatHandler extends SimpleChannelInboundHandler<Command> {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Command msg) throws Exception {
        final HeartBeat body = (HeartBeat) msg.getBody();
        System.out.println(msg.getCode() + body.getContent());
    }
}
