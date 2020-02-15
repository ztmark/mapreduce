package io.github.ztmark.common;

import java.nio.charset.StandardCharsets;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * @Author: Mark
 * @Date : 2020/2/15
 */
public class FetchJob implements CommandBody {

    private String workerId;

    public FetchJob(String workerId) {
        this.workerId = workerId;
    }

    @Override
    public ByteBuf encode() {
        final ByteBuf buffer = Unpooled.buffer();
        buffer.writeBytes(workerId.getBytes(StandardCharsets.UTF_8));
        return buffer;
    }

    public static FetchJob decode(ByteBuf byteBuf) {
        final int len = byteBuf.readableBytes();
        if (len == 0) {
            throw new RuntimeException("no data to read");
        }
        byte[] bytes = new byte[len];
        byteBuf.readBytes(bytes);
        return new FetchJob(new String(bytes, StandardCharsets.UTF_8));
    }

    public String getWorkerId() {
        return workerId;
    }
}
