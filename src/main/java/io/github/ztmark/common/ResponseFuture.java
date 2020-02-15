package io.github.ztmark.common;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @Author: Mark
 * @Date : 2020/2/15
 */
public class ResponseFuture {

    private int commandId;
    private Command responseCommand;
    private boolean sendSuccess;
    private Throwable sendCause;
    private CountDownLatch waitLatch;

    public ResponseFuture() {
        waitLatch = new CountDownLatch(1);
    }

    public Command await(long timeout, TimeUnit unit) throws InterruptedException {
        waitLatch.await(timeout, unit);
        return responseCommand;
    }

    public void setResponse(Command command) {
        this.responseCommand = command;
        waitLatch.countDown();
    }

    public boolean isSendSuccess() {
        return sendSuccess;
    }

    public void setSendSuccess(boolean sendSuccess) {
        this.sendSuccess = sendSuccess;
    }

    public Throwable getSendCause() {
        return sendCause;
    }

    public void setSendCause(Throwable sendCause) {
        this.sendCause = sendCause;
    }

    public int getCommandId() {
        return commandId;
    }

    public void setCommandId(int commandId) {
        this.commandId = commandId;
    }

    public Command getResponseCommand() {
        return responseCommand;
    }

    public void setResponseCommand(Command responseCommand) {
        this.responseCommand = responseCommand;
    }
}
