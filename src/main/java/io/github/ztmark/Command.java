package io.github.ztmark;

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
            case 0: {
                command.body = new HeartBeat();
                command.body.decode(byteBuf);
            }
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
