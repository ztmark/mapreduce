package io.github.ztmark;

import java.nio.charset.StandardCharsets;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * @Author: Mark
 * @Date : 2020/2/12
 */
public class HeartBeat implements CommandBody {

    private String content;

    public HeartBeat(String content) {
        this.content = content;
    }

    public HeartBeat() {
    }

    @Override
    public ByteBuf encode() {
        final ByteBuf buffer = Unpooled.buffer();
        if (content != null) {
            buffer.writeBytes(content.getBytes(StandardCharsets.UTF_8));
        }
        return buffer;
    }

    @Override
    public void decode(ByteBuf byteBuf) {
        final int len = byteBuf.readableBytes();
        if (len > 0) {
            byte[] bytes = new byte[len];
            byteBuf.readBytes(bytes);
            this.content = new String(bytes, StandardCharsets.UTF_8);
        }

    }

    public String getContent() {
        return content;
    }
}
