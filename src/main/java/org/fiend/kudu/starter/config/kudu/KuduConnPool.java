package org.fiend.kudu.starter.config.kudu;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * kudu连接池管理:
 *      每次向kudu增删改数据，均从连接池取令牌，
 *      一共 2000 个令牌，
 *      令牌发完之后，新的连接请求被驳回,
 *      连接池的并发总数为10000(通过测试该并发数较优),
 *      每个拿到令牌的用户(连接)所获得的并发数, 时刻取决于令牌的发放量,
 *      令牌发的越多, 每个用户所获得的并发数越低
 * @author langpf 2020-06-29 11:36:00
 */
@Component
public class KuduConnPool {
    Logger log = LoggerFactory.getLogger(getClass());

    /**
     * 最大令牌数
     */
    public static int MAX_TOKEN_SIZE = 2000;

    /**
     * 已发放令牌数
     */
    public static Integer CURRENT_TOKEN_SIZE = 0;

    /**
     * 连接池的并发总数
     */
    public static int TOTAL_OPERATION_PARALLEL_SIZE = 10000;

    public static int CURRENT_OPERATION_PARALLEL_SIZE = TOTAL_OPERATION_PARALLEL_SIZE;

    /**
     * 获取令牌
     */
    public boolean getTokenFail() {
        {
            synchronized (CURRENT_TOKEN_SIZE) {
                if (CURRENT_TOKEN_SIZE < MAX_TOKEN_SIZE) {
                    CURRENT_TOKEN_SIZE++;
                    CURRENT_OPERATION_PARALLEL_SIZE = TOTAL_OPERATION_PARALLEL_SIZE / CURRENT_TOKEN_SIZE;

                    log.info("获取令牌成功！ CURRENT_TOKEN_SIZE = {}， CURRENT_OPERATION_PARALLEL_SIZE = {} ",
                            CURRENT_TOKEN_SIZE, CURRENT_OPERATION_PARALLEL_SIZE);

                    return false;
                }
            }

            return true;
        }
    }

    /**
     * 归还令牌
     */
    public void returnToken() {
        synchronized (this) {
            if (CURRENT_TOKEN_SIZE > 0) {
                if (--CURRENT_TOKEN_SIZE > 0) {
                    CURRENT_OPERATION_PARALLEL_SIZE = TOTAL_OPERATION_PARALLEL_SIZE / CURRENT_TOKEN_SIZE;
                }

                log.info("归还令牌成功！ CURRENT_TOKEN_SIZE = {}， CURRENT_OPERATION_PARALLEL_SIZE = {} ",
                        CURRENT_TOKEN_SIZE, CURRENT_OPERATION_PARALLEL_SIZE);
            }
        }
    }
}
