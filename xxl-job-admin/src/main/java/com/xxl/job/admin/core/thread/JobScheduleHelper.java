package com.xxl.job.admin.core.thread;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.cron.CronExpression;
import com.xxl.job.admin.core.model.XxlJobInfo;
import com.xxl.job.admin.core.trigger.TriggerTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * 这里开了两条线程 ：
 * 第一条线程是一直去预读数据库  ，将即将要执行的数据预先加载进刻度盘
 *      对于处理时间超时 5S以上的数据 会直接将这次任务抛弃   重新设置下次的执行时间
 *      对于处理时间 超过下一次的执行时间  但是不超过5s的数据 执行立即执行的操作
 * 第二条线程：
 *      这条线程的本质其实是推动刻度的执行 然后在执行当前时间所在刻度的情况下还会去校验上一个刻度是否执行没执行的话一并加入
 *      调度器
 * @author xuxueli 2019-05-21
 */
public class JobScheduleHelper {
    private static Logger logger = LoggerFactory.getLogger(JobScheduleHelper.class);

    public static void main(String[] args) {
        System.out.println((System.currentTimeMillis()/1000)%60);
        System.out.println(((Calendar.getInstance().get(Calendar.SECOND))+60-1)%60);
    }
    private static JobScheduleHelper instance = new JobScheduleHelper();
    public static JobScheduleHelper getInstance(){
        return instance;
    }

    public static final long PRE_READ_MS = 5000;    // pre read

    private Thread scheduleThread;
    private Thread ringThread;
    private volatile boolean scheduleThreadToStop = false;
    private volatile boolean ringThreadToStop = false;
    private volatile static Map<Integer, List<Integer>> ringData = new ConcurrentHashMap<>();

    public void start(){
        // 调度线程
        scheduleThread = new Thread(new Runnable() {
            @Override
            public void run() {

                try {
                    //TODO  不理解  这里为什么会让他睡一会呢？
                    TimeUnit.MILLISECONDS.sleep(5000 - System.currentTimeMillis()%1000 );
                } catch (InterruptedException e) {
                    if (!scheduleThreadToStop) {
                        logger.error(e.getMessage(), e);
                    }
                }
                logger.info(">>>>>>>>> init xxl-job admin scheduler success.");

                while (!scheduleThreadToStop) {

                    // 扫描作业
                    long start = System.currentTimeMillis();

                    Connection conn = null;
                    Boolean connAutoCommit = null;
                    PreparedStatement preparedStatement = null;

                    boolean preReadSuc = true;
                    try {

                        conn = XxlJobAdminConfig.getAdminConfig().getDataSource().getConnection();
                        connAutoCommit = conn.getAutoCommit();
                        conn.setAutoCommit(false);

                        //for update 加了一个锁
                        preparedStatement = conn.prepareStatement(  "select * from xxl_job_lock where lock_name = 'schedule_lock' for update" );
                        preparedStatement.execute();

                        // tx start

                        // 1、pre read  时间调度轮预读
                        long nowTime = System.currentTimeMillis();
                        //寻找此时正在运行中  并且下次运行小于5秒的时间
                        List<XxlJobInfo> scheduleList = XxlJobAdminConfig.getAdminConfig().getXxlJobInfoDao().scheduleJobQuery(nowTime + PRE_READ_MS);
                        if (scheduleList!=null && scheduleList.size()>0) {
                            // 2、push time-ring   推动时间环
                            for (XxlJobInfo jobInfo: scheduleList) {

                                /**
                                 * //TODO  待研究
                                 * time-ring jump 定时跳动  当前时间 > 下次执行时间+5
                                 * 超时时间大于5s的数据好像是直接放弃不执行了  直接设置下一次的执行时间
                                 */
                                if (nowTime > jobInfo.getTriggerNextTime() + PRE_READ_MS) {
                                    // 2.1、trigger-expire > 5s：pass && make next-trigger-time

                                    // fresh next 下一个有效时间
                                    Date nextValidTime = new CronExpression(jobInfo.getJobCron()).getNextValidTimeAfter(new Date());
                                    if (nextValidTime != null) {
                                        //设置上一次的触发时间
                                        jobInfo.setTriggerLastTime(jobInfo.getTriggerNextTime());
                                        //设置下一次的触发时间
                                        jobInfo.setTriggerNextTime(nextValidTime.getTime());
                                    } else {
                                        jobInfo.setTriggerStatus(0);
                                        jobInfo.setTriggerLastTime(0);
                                        jobInfo.setTriggerNextTime(0);
                                    }
                                    /**
                                     * 当前的时间   >  下一次执行时间   证明已经执行过了啊
                                     * 正常情况下肯定不会走到这来  但是会出现一种情况
                                     * 任务的执行时间过长活着下方的数据阻塞的情况下会出现一个问题就是明明
                                     * 这一次的数据还没有执行完，还有未执行的数据，但是，本次任务的下一次执行时间
                                     * 却小于当前的时间  证明已经被执行（但是还没执行）
                                     */
                                } else if (nowTime > jobInfo.getTriggerNextTime()) {
                                    // 2.2、trigger-expire < 5s：direct-trigger && make next-trigger-time
                                    //解析一下cron表达式
                                    CronExpression cronExpression = new CronExpression(jobInfo.getJobCron());
                                    //获取到一个执行时间
                                    long nextTime = cronExpression.getNextValidTimeAfter(new Date()).getTime();

                                    /**
                                     * 触发
                                     * @param jobId
                                     * @param triggerType
                                     * @param failRetryCount
                                     * 			>=0: 使用这个参数
                                     * 			<0: 使用作业信息配置中的参数
                                     * @param executorShardingParam
                                     * @param executorParam
                                     *          null: use 工作参数
                                     *          not null: 掩护工作参数
                                     */
                                    JobTriggerPoolHelper.trigger(jobInfo.getId(), TriggerTypeEnum.CRON, -1, null, null);
                                    logger.debug(">>>>>>>>>>> xxl-job, shecule push trigger : jobId = " + jobInfo.getId() );

                                    // 2、fresh next
                                    jobInfo.setTriggerLastTime(jobInfo.getTriggerNextTime());
                                    jobInfo.setTriggerNextTime(nextTime);


                                    /**
                                     * 下一次触发时间为5秒，请再次预读
                                     * 这次任务已经超时了  所以会立即检测它的下次任务是不是小于5S
                                     */
                                    if (jobInfo.getTriggerNextTime() - nowTime < PRE_READ_MS) {

                                        // 1、make ring second
                                        int ringSecond = (int)((jobInfo.getTriggerNextTime()/1000)%60);

                                        // 2、push time ring
                                        pushTimeRing(ringSecond, jobInfo.getId());

                                        // 3、fresh next
                                        Date nextValidTime = new CronExpression(jobInfo.getJobCron()).getNextValidTimeAfter(new Date(jobInfo.getTriggerNextTime()));
                                        if (nextValidTime != null) {
                                            jobInfo.setTriggerLastTime(jobInfo.getTriggerNextTime());
                                            jobInfo.setTriggerNextTime(nextValidTime.getTime());
                                        } else {
                                            jobInfo.setTriggerStatus(0);
                                            jobInfo.setTriggerLastTime(0);
                                            jobInfo.setTriggerNextTime(0);
                                        }

                                    }

                                } else {
                                    // 2.3、触发预读：定时触发&&进行下一次触发

                                    // 1、make ring second   设置时间轮
                                    int ringSecond = (int)((jobInfo.getTriggerNextTime()/1000)%60);

                                    // 2、push time ring  放置到环里面
                                    pushTimeRing(ringSecond, jobInfo.getId());

                                    // 3、fresh next  刷新下一次
                                    Date nextValidTime = new CronExpression(jobInfo.getJobCron()).getNextValidTimeAfter(new Date(jobInfo.getTriggerNextTime()));
                                    if (nextValidTime != null) {
                                        jobInfo.setTriggerLastTime(jobInfo.getTriggerNextTime());
                                        jobInfo.setTriggerNextTime(nextValidTime.getTime());
                                    } else {
                                        jobInfo.setTriggerStatus(0);
                                        jobInfo.setTriggerLastTime(0);
                                        jobInfo.setTriggerNextTime(0);
                                    }

                                }

                            }

                            // 3、更新触发信息
                            for (XxlJobInfo jobInfo: scheduleList) {
                                XxlJobAdminConfig.getAdminConfig().getXxlJobInfoDao().scheduleUpdate(jobInfo);
                            }

                        } else {
                            preReadSuc = false;
                        }

                        // tx stop


                    } catch (Exception e) {
                        if (!scheduleThreadToStop) {
                            logger.error(">>>>>>>>>>> xxl-job, JobScheduleHelper#scheduleThread error:{}", e);
                        }
                    } finally {

                        // commit
                        if (conn != null) {
                            try {
                                conn.commit();
                            } catch (SQLException e) {
                                if (!scheduleThreadToStop) {
                                    logger.error(e.getMessage(), e);
                                }
                            }
                            try {
                                conn.setAutoCommit(connAutoCommit);
                            } catch (SQLException e) {
                                if (!scheduleThreadToStop) {
                                    logger.error(e.getMessage(), e);
                                }
                            }
                            try {
                                conn.close();
                            } catch (SQLException e) {
                                if (!scheduleThreadToStop) {
                                    logger.error(e.getMessage(), e);
                                }
                            }
                        }

                        // close PreparedStatement
                        if (null != preparedStatement) {
                            try {
                                preparedStatement.close();
                            } catch (SQLException ignore) {
                                if (!scheduleThreadToStop) {
                                    logger.error(ignore.getMessage(), ignore);
                                }
                            }
                        }
                    }
                    long cost = System.currentTimeMillis()-start;


                    // Wait seconds, align second
                    if (cost < 1000) {  // scan-overtime, not wait
                        try {
                            // pre-read period: success > scan each second; fail > skip this period;
                            TimeUnit.MILLISECONDS.sleep((preReadSuc?1000:PRE_READ_MS) - System.currentTimeMillis()%1000);
                        } catch (InterruptedException e) {
                            if (!scheduleThreadToStop) {
                                logger.error(e.getMessage(), e);
                            }
                        }
                    }

                }

                logger.info(">>>>>>>>>>> xxl-job, JobScheduleHelper#scheduleThread stop");
            }
        });
        scheduleThread.setDaemon(true);
        scheduleThread.setName("xxl-job, admin JobScheduleHelper#scheduleThread");
        scheduleThread.start();


        // ring thread
        ringThread = new Thread(new Runnable() {
            @Override
            public void run() {

                // 对齐第二
                try {
                    TimeUnit.MILLISECONDS.sleep(1000 - System.currentTimeMillis()%1000 );
                } catch (InterruptedException e) {
                    if (!ringThreadToStop) {
                        logger.error(e.getMessage(), e);
                    }
                }

                while (!ringThreadToStop) {

                    try {
                        // 第二个数据
                        List<Integer> ringItemData = new ArrayList<>();
                        // 避免处理耗时太长，跨过刻度，向前校验一个刻度； 这个同样是获取当前时间/1000)%60 的数据
                        int nowSecond = Calendar.getInstance().get(Calendar.SECOND);
                        for (int i = 0; i < 2; i++) {
                            /**
                             * 这一步的计算
                             *   0：会获取到当前时间所在的时间轮盘位置
                             *   1：会获取时间轮的上一个位置
                             *
                             *   正如上面所说的  为了避免执行过程时间过长 会每一次都去看一下上一个刻度是不是还有没有执行的
                             *   如果有的话  会一并执行
                             */
                            List<Integer> tmpData = ringData.remove( (nowSecond+60-i)%60 );
                            if (tmpData != null) {
                                ringItemData.addAll(tmpData);
                            }
                        }

                        // ring trigger
                        logger.debug(">>>>>>>>>>> xxl-job, time-ring beat : " + nowSecond + " = " + Arrays.asList(ringItemData) );
                        if (ringItemData!=null && ringItemData.size()>0) {
                            // do trigger  触发
                            for (int jobId: ringItemData) {
                                // do trigger  触发
                                JobTriggerPoolHelper.trigger(jobId, TriggerTypeEnum.CRON, -1, null, null);
                            }
                            // clear
                            ringItemData.clear();
                        }
                    } catch (Exception e) {
                        if (!ringThreadToStop) {
                            logger.error(">>>>>>>>>>> xxl-job, JobScheduleHelper#ringThread error:{}", e);
                        }
                    }

                    // next second, align second
                    try {
                        TimeUnit.MILLISECONDS.sleep(1000 - System.currentTimeMillis()%1000);
                    } catch (InterruptedException e) {
                        if (!ringThreadToStop) {
                            logger.error(e.getMessage(), e);
                        }
                    }
                }
                logger.info(">>>>>>>>>>> xxl-job, JobScheduleHelper#ringThread stop");
            }
        });
        ringThread.setDaemon(true);
        ringThread.setName("xxl-job, admin JobScheduleHelper#ringThread");
        ringThread.start();
    }

    private void pushTimeRing(int ringSecond, int jobId){
        // 推异步环
        List<Integer> ringItemData = ringData.get(ringSecond);
        if (ringItemData == null) {
            ringItemData = new ArrayList<Integer>();
            ringData.put(ringSecond, ringItemData);
        }
        ringItemData.add(jobId);

        logger.debug(">>>>>>>>>>> xxl-job, shecule push time-ring : " + ringSecond + " = " + Arrays.asList(ringItemData) );
    }

    public void toStop(){

        // 1、stop schedule
        scheduleThreadToStop = true;
        try {
            TimeUnit.SECONDS.sleep(1);  // wait
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
        if (scheduleThread.getState() != Thread.State.TERMINATED){
            // interrupt and wait
            scheduleThread.interrupt();
            try {
                scheduleThread.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }

        // if has ring data
        boolean hasRingData = false;
        if (!ringData.isEmpty()) {
            for (int second : ringData.keySet()) {
                List<Integer> tmpData = ringData.get(second);
                if (tmpData!=null && tmpData.size()>0) {
                    hasRingData = true;
                    break;
                }
            }
        }
        if (hasRingData) {
            try {
                TimeUnit.SECONDS.sleep(8);
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }

        // stop ring (wait job-in-memory stop)
        ringThreadToStop = true;
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
        if (ringThread.getState() != Thread.State.TERMINATED){
            // interrupt and wait
            ringThread.interrupt();
            try {
                ringThread.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }

        logger.info(">>>>>>>>>>> xxl-job, JobScheduleHelper stop");
    }

}
