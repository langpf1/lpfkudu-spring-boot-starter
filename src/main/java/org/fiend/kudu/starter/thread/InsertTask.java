package org.fiend.kudu.starter.thread;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import org.fiend.kudu.starter.template.KuduTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Callable;

import static org.fiend.kudu.starter.config.Constants.*;

/**
 * 建表语句:
 *    CREATE TABLE base_tag_50
 * 		(
 * 		  id BIGINT,
 * 		  base_tag_id STRING,
 * 		  user_id BIGINT,
 * 		  name STRING,
 * 		  phone STRING,
 * 		  sex STRING,
 * 		  address STRING,
 * 		  job STRING,
 * 		  birthday BIGINT,
 * 		  high_1 STRING,
 * 		  high_2 STRING,
 * 		  high_3 STRING,
 * 		  high_4 STRING,
 * 		  high_5 STRING,
 * 		  high_6 STRING,
 * 		  high_7 STRING,
 * 		  high_8 STRING,
 * 		  high_9 STRING,
 * 		  high_10 STRING,
 * 		  high_11 STRING,
 * 		  high_12 STRING,
 * 		  high_13 STRING,
 * 		  high_14 STRING,
 * 		  high_15 STRING,
 * 		  high_16 STRING,
 * 		  high_17 STRING,
 * 		  high_18 STRING,
 * 		  high_19 STRING,
 * 		  high_20 STRING,
 * 		  high_21 STRING,
 * 		  high_22 STRING,
 * 		  high_23 STRING,
 * 		  high_24 STRING,
 * 		  high_25 STRING,
 * 		  high_26 STRING,
 * 		  high_27 STRING,
 * 		  high_28 STRING,
 * 		  high_29 STRING,
 * 		  high_30 STRING,
 * 		  high_31 STRING,
 * 		  high_32 STRING,
 * 		  high_33 STRING,
 * 		  high_34 STRING,
 * 		  high_35 STRING,
 * 		  high_36 STRING,
 * 		  high_37 STRING,
 * 		  high_38 STRING,
 * 		  high_39 STRING,
 * 		  high_40 STRING,
 * 		  high_41 STRING,
 * 		  PRIMARY KEY(id)
 * 		)
 * 		partition by hash(id) partitions 3
 * 		stored as kudu
 * @author langpf 2020-06-23 14:51:09
 */
public class InsertTask implements Callable<Integer> {
    Logger log = LoggerFactory.getLogger(getClass());

    private final KuduTemplate kuduTemplate;
    private final int num;

    public InsertTask(KuduTemplate kuduTemplate, int num) {
        this.kuduTemplate = kuduTemplate;
        this.num = num;
    }

    @Override
    public Integer call() throws Exception {
        String dataJsonStr = "{\"high_31\":\"123123\",\"high_30\":\"123123\",\"high_33\":\"123123\",\"high_32\":\"123123\",\"high_35\":\"123123\",\"high_34\":\"123123\",\"high_26\":\"123123\",\"high_25\":\"123123\",\"high_28\":\"123123\",\"high_27\":\"123123\",\"high_29\":\"123123\",\"high_1\":\"123123\",\"high_2\":\"123123\",\"high_3\":\"123123\",\"high_4\":\"123123\",\"high_5\":\"123123\",\"high_40\":\"123123\",\"high_6\":\"123123\",\"high_7\":\"123123\",\"high_8\":\"123123\",\"high_41\":\"123123\",\"high_9\":\"123123\",\"high_37\":\"123123\",\"high_36\":\"123123\",\"high_39\":\"123123\",\"high_38\":\"123123\",\"phone\":\"13337825667\",\"name\":\"郎鹏飞\",\"job\":\"研发\",\"birthday\":527529600000,\"high_11\":\"123123\",\"high_10\":\"123123\",\"high_13\":\"123123\",\"high_12\":\"123123\",\"base_tag_id\":\"user_info\",\"address\":\"沈阳市浑南区\",\"sex\":\"男\",\"high_20\":\"123123\",\"high_22\":\"123123\",\"high_21\":\"123123\",\"high_24\":\"123123\",\"high_23\":\"123123\",\"high_15\":\"123123\",\"high_14\":\"123123\",\"high_17\":\"123123\",\"high_16\":\"123123\",\"high_19\":\"123123\",\"high_18\":\"123123\"}";

        JSONObject data;
        List<JSONObject> dataList = Lists.newArrayList();
        for (int i = 1; i <= num; i++) {
            data = JSONObject.parseObject(dataJsonStr);
            data.put("id", kuduTemplate.getId());
            data.put("user_id", kuduTemplate.getId());

            dataList.add(data);

            if (i % MAX_KUDU_OPERATION_SIZE == 0) {
                kuduTemplate.batchInsert(TABLE_NAME, dataList);
                dataList = Lists.newArrayList();
            }
        }

        kuduTemplate.batchInsert(TABLE_NAME, dataList);
        return num;
    }
}
