package org.fiend.kudu.starter.thread;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import org.fiend.kudu.starter.template.KuduTemplate;
import org.fiend.kudu.starter.utils.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Callable;

import static org.fiend.kudu.starter.config.Constants.MAX_KUDU_OPERATION_SIZE;
import static org.fiend.kudu.starter.config.Constants.TABLE_NAME;

/**
 * @author langpf 2020-06-23 14:51:09
 */
public class InsertTaskAsyn implements Callable<Integer> {
    Logger log = LoggerFactory.getLogger(getClass());

    private final KuduTemplate kuduTemplate;
    private final int num;

    public InsertTaskAsyn(KuduTemplate kuduTemplate, int num) {
        this.kuduTemplate = kuduTemplate;
        this.num = num;
    }

    @Override
    public Integer call() throws Exception {
        JSONObject data;
        List<JSONObject> dataList = Lists.newArrayList();

        for (int i = 0; i < num; i++) {
            data = new JSONObject();
            // data.put("id", kuduTemplate.getId());
            data.put("base_tag_id", "user_info");
            // data.put("user_id", kuduTemplate.getId());
            data.put("name", "郎鹏飞");
            data.put("phone", "13337825667");
            data.put("sex", "男");
            data.put("address", "沈阳市浑南区");
            data.put("job", "研发");
            data.put("birthday", DateUtil.str2Long2("1986.09.20"));

            data.put("id", kuduTemplate.getId());
            data.put("user_id", kuduTemplate.getId());

            for (int k = 1; k <= 41; k++) {
                data.put("high_" + k, "123123");
            }

            dataList.add(data);

            if (i % MAX_KUDU_OPERATION_SIZE == 0) {
                kuduTemplate.batchInsertAsyn(TABLE_NAME, dataList);
                dataList = Lists.newArrayList();
            }
        }

        kuduTemplate.batchInsertAsyn(TABLE_NAME, dataList);
        return num;
    }
}
