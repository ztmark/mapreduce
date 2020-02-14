package io.github.ztmark.common;

import io.netty.buffer.ByteBuf;

/**
 * @Author: Mark
 * @Date : 2020/2/13
 */
public interface CommandBody {

    ByteBuf encode();

}
