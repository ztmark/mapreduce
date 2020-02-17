package io.github.ztmark.common;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * @Author: Mark
 * @Date : 2020/2/12
 */
public class Command {

    private int commandId;
    protected int code;
    private CommandBody body;

    public Command(int code, CommandBody body) {
        this.code = code;
        this.body = body;
    }

    public Command() {
    }

    public static Command decode(ByteBuf byteBuf) {
        final Command command = new Command();
        command.commandId = byteBuf.readInt();
        command.code = byteBuf.readInt();
        switch (command.code) {
            case CommandCode.HEART_BEAT:
                command.body = HeartBeat.decode(byteBuf);
                break;
            case CommandCode.REGISTRATION:
                command.body = Registration.decode(byteBuf);
                break;
            case CommandCode.FETCH_JOB:
                command.body = FetchJob.decode(byteBuf);
                break;
            case CommandCode.FETCH_JOB_RESP:
                command.body = FetchJobResp.decode(byteBuf);
                break;
            case CommandCode.DONE_JOB:
                command.body = DoneJob.decode(byteBuf);
                break;
        }
        return command;
    }

    public ByteBuf encode() {
        final ByteBuf buffer = Unpooled.buffer();
        final ByteBuf encode = body.encode();
        buffer.writeInt(encode.readableBytes() + 8);
        buffer.writeInt(commandId);
        buffer.writeInt(code);
        buffer.writeBytes(encode);
        return buffer;
    }

    public int getCommandId() {
        return commandId;
    }

    public void setCommandId(int commandId) {
        this.commandId = commandId;
    }

    public int getCode() {
        return code;
    }

    public CommandBody getBody() {
        return body;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Command{");
        sb.append("commandId=").append(commandId);
        sb.append(", code=").append(code);
        sb.append(", body=").append(body);
        sb.append('}');
        return sb.toString();
    }
}
