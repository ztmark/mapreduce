package io.github.ztmark;

import io.netty.buffer.ByteBuf;

/**
 * @Author: Mark
 * @Date : 2020/2/12
 */
public class HeartBeat implements CommandBody {

    public static HeartBeat create(ByteBuf byteBuf) {
        return new HeartBeat();
    }
}
