package com.xxl.job.core.biz.impl;

import com.xxl.job.core.biz.ExecutorBiz;
import com.xxl.job.core.biz.model.LogResult;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.biz.model.TriggerParam;
import com.xxl.job.core.enums.ExecutorBlockStrategyEnum;
import com.xxl.job.core.executor.XxlJobExecutor;
import com.xxl.job.core.glue.GlueFactory;
import com.xxl.job.core.glue.GlueTypeEnum;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.impl.GlueJobHandler;
import com.xxl.job.core.handler.impl.ScriptJobHandler;
import com.xxl.job.core.log.XxlJobFileAppender;
import com.xxl.job.core.thread.JobThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * Created by xuxueli on 17/3/1.
 */
public class ExecutorBizImpl implements ExecutorBiz {
    private static Logger logger = LoggerFactory.getLogger(ExecutorBizImpl.class);

    @Override
    public ReturnT<String> beat() {
        return ReturnT.SUCCESS;
    }

    @Override
    public ReturnT<String> idleBeat(int jobId) {

        // isRunningOrHasQueue
        boolean isRunningOrHasQueue = false;
        JobThread jobThread = XxlJobExecutor.loadJobThread(jobId);
        if (jobThread != null && jobThread.isRunningOrHasQueue()) {
            isRunningOrHasQueue = true;
        }

        if (isRunningOrHasQueue) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, "job thread is running or has trigger queue.");
        }
        return ReturnT.SUCCESS;
    }

    @Override
    public ReturnT<String> kill(int jobId) {
        // kill handlerThread, and create new one
        JobThread jobThread = XxlJobExecutor.loadJobThread(jobId);
        if (jobThread != null) {
            XxlJobExecutor.removeJobThread(jobId, "scheduling center kill job.");
            return ReturnT.SUCCESS;
        }

        return new ReturnT<String>(ReturnT.SUCCESS_CODE, "job thread already killed.");
    }

    @Override
    public ReturnT<LogResult> log(long logDateTim, long logId, int fromLineNum) {
        // log filename: logPath/yyyy-MM-dd/9999.log
        String logFileName = XxlJobFileAppender.makeLogFileName(new Date(logDateTim), logId);

        LogResult logResult = XxlJobFileAppender.readLog(logFileName, fromLineNum);
        return new ReturnT<LogResult>(logResult);
    }

    /**
     * 执行方法
     * @param triggerParam
     * @return
     */
    @Override
    public ReturnT<String> run(TriggerParam triggerParam) {
        // 加载当前任务所在的任务线程   如果为null  一定是任务的第一次执行
        JobThread jobThread = XxlJobExecutor.loadJobThread(triggerParam.getJobId());
        IJobHandler jobHandler = jobThread!=null?jobThread.getHandler():null;
        String removeOldReason = null;

        // valid：jobHandler + jobThread   看看是什么类型的任务   本地java   web java  web python。。。。
        GlueTypeEnum glueTypeEnum = GlueTypeEnum.match(triggerParam.getGlueType());
        //如果是本地类型的java
        if (GlueTypeEnum.BEAN == glueTypeEnum) {

            /**
             * com.xxl.job.core.executor.impl.XxlJobSpringExecutor#initJobHandlerRepository(org.springframework.context.ApplicationContext)
             * 这里面的值在进行Spring容器初始化的时候就已经加载好了
             * new jobhandler  根据本地的   @JobHandler(value = "httpJobHandler")
             * 获取这个对象  所以他肯定是有值的  没值的话只有两个可能 1：没这个任务   2：jobHandler写错了
             */
            IJobHandler newJobHandler = XxlJobExecutor.loadJobHandler(triggerParam.getExecutorHandler());

            // 有效的旧jobThread
            if (jobThread!=null && jobHandler != newJobHandler) {
                /**
                 * 更改处理程序，需要杀死旧线程
                 * 更改作业处理程序或粘合类型，然后终止旧的作业线程。
                 */
                removeOldReason = "change jobhandler or glue type, and terminate the old job thread.";

                jobThread = null;
                jobHandler = null;
            }

            //如果说 此时这个是第一次运行 就走第一次的处理程序
            if (jobHandler == null) {
                jobHandler = newJobHandler;
                if (jobHandler == null) {
                    return new ReturnT<String>(ReturnT.FAIL_CODE, "job handler [" + triggerParam.getExecutorHandler() + "] not found.");
                }
            }
        //这个判断是判断  web java的
        } else if (GlueTypeEnum.GLUE_GROOVY == glueTypeEnum) {
            // 有效的旧jobThread  还是和上方一样 当程序或任务发生更改时 或者 不是属于GlueJobHandler子类的
            //或者getGlueUpdatetime 和任务的修改时间不一致（任务发生修改） 就会发清空旧线程 触发新线程的状态
            if (jobThread != null &&
                    !(jobThread.getHandler() instanceof GlueJobHandler
                        && ((GlueJobHandler) jobThread.getHandler()).getGlueUpdatetime()==triggerParam.getGlueUpdatetime() )) {
                // 更改处理程序或胶源更新，需要杀死旧线程
                removeOldReason = "change job source or glue type, and terminate the old job thread.";

                jobThread = null;
                jobHandler = null;
            }

            // valid handler有效的处理程序   这里其实是 web java 第一次执行的情况  或者任务发生改变的情况
            if (jobHandler == null) {
                try {
                    //第一次执行都会到这里 原始作业处理程序
                    //这里加载web java编写的代码  返回了一个任务的实例 通过接口回调 回调那个方法
                    IJobHandler originJobHandler = GlueFactory.getInstance().loadNewInstance(triggerParam.getGlueSource());
                    //调用 可以理解未这是个代理类  通过他 会调用子类的 execute 方法
                    jobHandler = new GlueJobHandler(originJobHandler, triggerParam.getGlueUpdatetime());
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    return new ReturnT<String>(ReturnT.FAIL_CODE, e.getMessage());
                }
            }
        //这里开始处理脚本程序    isScript = true 都是脚本程序   (GLUE(Shell)|GLUE(Python)|GLUE(PHP)|GLUE(Nodejs)|GLUE(PowerShell))
        } else if (glueTypeEnum!=null && glueTypeEnum.isScript()) {

            // 检验任务是否发生变动  详情见上
            if (jobThread != null &&
                    !(jobThread.getHandler() instanceof ScriptJobHandler
                            && ((ScriptJobHandler) jobThread.getHandler()).getGlueUpdatetime()==triggerParam.getGlueUpdatetime() )) {
                //更改脚本或胶源更新，需要杀死旧线程
                removeOldReason = "change job source or glue type, and terminate the old job thread.";

                jobThread = null;
                jobHandler = null;
            }

            // valid handler   不妨先看第一次执行
            if (jobHandler == null) {
                //任务id  任务的更新时间  任务的源代码   任务的类型   如果存在相同类型的  会删除掉
                jobHandler = new ScriptJobHandler(triggerParam.getJobId(), triggerParam.getGlueUpdatetime(), triggerParam.getGlueSource(), GlueTypeEnum.match(triggerParam.getGlueType()));
            }
        } else {
            return new ReturnT<String>(ReturnT.FAIL_CODE, "glueType[" + triggerParam.getGlueType() + "] is not valid.");
        }

        // 执行者阻止策略 阻塞处理策略  单机串行(SERIAL_EXECUTION)    丢弃后续调度(DISCARD_LATER)   覆盖之前调度(COVER_EARLY)
        if (jobThread != null) {
            //获取该任务的执行策略
            ExecutorBlockStrategyEnum blockStrategy = ExecutorBlockStrategyEnum.match(triggerParam.getExecutorBlockStrategy(), null);
            if (ExecutorBlockStrategyEnum.DISCARD_LATER == blockStrategy) {
                // 运行时丢弃
                if (jobThread.isRunningOrHasQueue()) {
                    return new ReturnT<String>(ReturnT.FAIL_CODE, "block strategy effect："+ExecutorBlockStrategyEnum.DISCARD_LATER.getTitle());
                }
            } else if (ExecutorBlockStrategyEnum.COVER_EARLY == blockStrategy) {
                // 杀死正在运行的jobThread
                if (jobThread.isRunningOrHasQueue()) {
                    removeOldReason = "block strategy effect：" + ExecutorBlockStrategyEnum.COVER_EARLY.getTitle();

                    jobThread = null;
                }
            } else {

                // 单机串行的情况 只是排队触发
            }
        }

        // 替换线程（新线程或存在无效线程）   这个真正的去加载到缓存中去了
        //把一个线程放置到  com.xxl.job.core.executor.XxlJobExecutor.jobThreadRepository 这个Map里面去了
        //这个构想挺好的   removeOldReason 是删除的原因
        //除了第一次启动任务 还有一个条件会调用这个   当任务发生更改的时候回清空jobThread  从新放置
        if (jobThread == null) {
            jobThread = XxlJobExecutor.registJobThread(triggerParam.getJobId(), jobHandler, removeOldReason);
        }

        // 将数据推送到队列
        ReturnT<String> pushResult = jobThread.pushTriggerQueue(triggerParam);
        return pushResult;
    }

}
