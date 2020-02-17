package io.github.ztmark.common;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * @Author: Mark
 * @Date : 2020/2/17
 */
public class DoneJob implements CommandBody {

    private String workerId;
    private int jobType;
    private String arg;
    private Set<String> result;

    public static CommandBody decode(ByteBuf byteBuf) {
        final DoneJob doneJob = new DoneJob();
        final int workerIdLen = byteBuf.readInt();
        byte[] byteArr = new byte[workerIdLen];
        byteBuf.readBytes(byteArr);
        doneJob.workerId = new String(byteArr, StandardCharsets.UTF_8);
        doneJob.jobType = byteBuf.readInt();
        if (byteBuf.isReadable()) {
            final int argLen = byteBuf.readInt();
            if (argLen > workerIdLen) {
                byteArr = new byte[argLen];
            }
            byteBuf.readBytes(byteArr, 0, argLen);
            doneJob.arg = new String(byteArr, 0, argLen, StandardCharsets.UTF_8);
            if (byteBuf.isReadable()) {
                final int resultLen = byteBuf.readInt();
                byteArr = new byte[resultLen];
                byteBuf.readBytes(byteArr);
                final String string = new String(byteArr, StandardCharsets.UTF_8);
                doneJob.result = Arrays.stream(string.split(";")).collect(Collectors.toSet());
            }
        }
        return doneJob;
    }

    @Override
    public ByteBuf encode() {
        final ByteBuf buffer = Unpooled.buffer();
        byte[] bytes = workerId.getBytes(StandardCharsets.UTF_8);
        buffer.writeInt(bytes.length); // workerId length
        buffer.writeBytes(bytes);
        buffer.writeInt(jobType);
        if (arg != null) {
            bytes = arg.getBytes(StandardCharsets.UTF_8);
            buffer.writeInt(bytes.length); // arg length
            buffer.writeBytes(bytes);
            if (result != null) {
                final String r = String.join(";", result);
                bytes = r.getBytes(StandardCharsets.UTF_8);
                buffer.writeInt(bytes.length); // result length
                buffer.writeBytes(bytes);
            }
        }
        return buffer;
    }

    public int getJobType() {
        return jobType;
    }

    public void setJobType(int jobType) {
        this.jobType = jobType;
    }

    public String getWorkerId() {
        return workerId;
    }

    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }

    public String getArg() {
        return arg;
    }

    public void setArg(String arg) {
        this.arg = arg;
    }

    public Set<String> getResult() {
        return result;
    }

    public void setResult(Set<String> result) {
        this.result = result;
    }
}
