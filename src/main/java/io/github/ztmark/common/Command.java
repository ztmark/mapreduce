package io.github.ztmark.common;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * @Author: Mark
 * @Date : 2020/2/12
 */
public class Command {

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
        command.code = byteBuf.readInt();
        switch (command.code) {
            case CommandCode.HEART_BEAT:
                command.body = HeartBeat.decode(byteBuf);
                break;
            case CommandCode.REGISTRATION:
                command.body = Registration.decode(byteBuf);
                break;
        }
        return command;
    }

    public ByteBuf encode() {
        final ByteBuf buffer = Unpooled.buffer();
        final ByteBuf encode = body.encode();
        buffer.writeInt(encode.readableBytes() + 4);
        buffer.writeInt(code);
        buffer.writeBytes(encode);
        return buffer;
    }

    public int getCode() {
        return code;
    }

    public CommandBody getBody() {
        return body;
    }
}
