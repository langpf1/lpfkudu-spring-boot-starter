package org.fiend.kudu.starter.config;

import org.apache.kudu.client.Operation;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author langpf 2020-06-23 15:29:40
 */
public class Constants {
    public static String TABLE_NAME = "impala::performace_db.base_tag_50";
    public static String SCENE_TABLE_NAME = "impala::performace_db.base_tag";

    /**
     * 定时任务执行间隔周期（毫秒）
     * */
    public static final long SCHEDULE_INTERVAL_PERIOD = 500;

    /**
     * 存储客户端（用户）提交的设置控制命令
     */
    public final static BlockingQueue<Operation> KUDU_OPERATION_QUEUE = new LinkedBlockingQueue<>();

    /**
     * 每次KUDU_OPERATION_QUEUE队列存放OPERATION数量 处理阈值，高于此阈值时进行apply, flush
     */
    public static int MAX_KUDU_OPERATION_SIZE = 1000;

    /**
     * 最小间隔处理时间 500ms
     */
    public static int MAX_INTERVAL_FLUSH_MILLISECOND = 500;

    // public static final List<Integer> TIME_LIST = Lists.newArrayList();
    public static long START_TIME = 0;
    public static int DATA_SIZE = 0;
}
