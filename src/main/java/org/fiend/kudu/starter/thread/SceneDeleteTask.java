package org.fiend.kudu.starter.thread;

import com.alibaba.fastjson.JSONObject;
import org.fiend.kudu.starter.template.KuduTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Callable;

import static org.fiend.kudu.starter.config.Constants.SCENE_TABLE_NAME;

/**
 * forkjoin
 * @author langpf 2020-06-23 14:51:09
 */
public class SceneDeleteTask implements Callable<Integer> {
    Logger log = LoggerFactory.getLogger(getClass());

    private final KuduTemplate kuduTemplate;
    private final List<JSONObject> dataList;

    public SceneDeleteTask(KuduTemplate kuduTemplate,
                           List<JSONObject> dataList) {
        this.kuduTemplate = kuduTemplate;
        this.dataList = dataList;
    }

    @Override
    public Integer call() throws Exception {
        log.info("start batch delete ...");
        kuduTemplate.batchDelete(SCENE_TABLE_NAME, dataList);
        return dataList.size();
    }
}
