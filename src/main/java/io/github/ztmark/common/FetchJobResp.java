package io.github.ztmark.common;

import java.nio.charset.StandardCharsets;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * @Author: Mark
 * @Date : 2020/2/15
 */
public class FetchJobResp implements CommandBody {

    private Job job;

    public FetchJobResp(Job job) {
        this.job = job;
    }

    public static CommandBody decode(ByteBuf byteBuf) {
        final Job job = new Job();
        job.setJobType(byteBuf.readInt());
        final int len = byteBuf.readableBytes();
        if (len != 0) {
            byte[] bytes = new byte[len];
            byteBuf.readBytes(bytes);
            job.setArg(new String(bytes, StandardCharsets.UTF_8));
        }
        return new FetchJobResp(job);
    }

    @Override
    public ByteBuf encode() {
        final ByteBuf buffer = Unpooled.buffer();
        if (job != null) {
            buffer.writeInt(job.getJobType());
            if (job.getArg() != null) {
                buffer.writeBytes(job.getArg().getBytes(StandardCharsets.UTF_8));
            }
        }
        return buffer;
    }

    public Job getJob() {
        return job;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FetchJobResp{");
        sb.append("job=").append(job);
        sb.append('}');
        return sb.toString();
    }
}
