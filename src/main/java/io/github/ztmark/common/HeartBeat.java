package io.github.ztmark.common;

import java.nio.charset.StandardCharsets;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * @Author: Mark
 * @Date : 2020/2/12
 */
public class HeartBeat implements CommandBody {

    private String workerId;

    public HeartBeat(String workerId) {
        this.workerId = workerId;
    }

    public HeartBeat() {
    }

    @Override
    public ByteBuf encode() {
        final ByteBuf buffer = Unpooled.buffer();
        if (workerId != null) {
            buffer.writeBytes(workerId.getBytes(StandardCharsets.UTF_8));
        }
        return buffer;
    }

    public static HeartBeat decode(ByteBuf byteBuf) {
        final HeartBeat heartBeat = new HeartBeat();
        final int len = byteBuf.readableBytes();
        if (len > 0) {
            byte[] bytes = new byte[len];
            byteBuf.readBytes(bytes);
            heartBeat.workerId = new String(bytes, StandardCharsets.UTF_8);
        }
        return heartBeat;
    }

    public String getWorkerId() {
        return workerId;
    }
}
