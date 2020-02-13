package io.github.ztmark;

import io.netty.buffer.ByteBuf;

/**
 * @Author: Mark
 * @Date : 2020/2/12
 */
public class Command {

    protected int code;
    private CommandBody body;

    public static Command decode(ByteBuf byteBuf) {
        final Command command = new Command();
        command.code = byteBuf.readInt();
        switch (command.code) {
            case 0:
                command.body = HeartBeat.create(byteBuf);
        }
        return command;
    }

    public ByteBuf encode() {
        return null;
    }

    public int getCode() {
        return code;
    }

    public CommandBody getBody() {
        return body;
    }
}
