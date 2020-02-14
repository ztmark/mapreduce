package io.github.ztmark;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * @Author: Mark
 * @Date : 2020/2/12
 */
public class NettyEncoder extends MessageToByteEncoder<Command> {
    @Override
    protected void encode(ChannelHandlerContext ctx, Command msg, ByteBuf out) throws Exception {
        try {
            final ByteBuf encode = msg.encode();
            out.writeBytes(encode);
        } catch (Exception e) {
            e.printStackTrace();
            ctx.channel().close();
        }
    }
}
