package io.github.ztmark.common;

/**
 * @Author: Mark
 * @Date : 2020/2/15
 */
public class Job {

    public static final int MAP_JOB = 1;
    public static final int REDUCE_JOB = 2;
    public static final int POISON = 3;

    private int jobType;
    private String arg;

    public int getJobType() {
        return jobType;
    }

    public void setJobType(int jobType) {
        this.jobType = jobType;
    }

    public String getArg() {
        return arg;
    }

    public void setArg(String arg) {
        this.arg = arg;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Job{");
        sb.append("jobType=").append(jobType);
        sb.append(", arg='").append(arg).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
