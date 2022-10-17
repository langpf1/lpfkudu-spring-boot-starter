package org.fiend.kudu.starter.thread;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import org.fiend.kudu.starter.template.KuduTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Callable;

import static org.fiend.kudu.starter.config.Constants.MAX_KUDU_OPERATION_SIZE;
import static org.fiend.kudu.starter.config.Constants.SCENE_TABLE_NAME;

/**
 * @author langpf 2020-06-23 14:51:09
 */
public class SceneInsertTask implements Callable<Integer> {
    Logger log = LoggerFactory.getLogger(getClass());

    private final KuduTemplate kuduTemplate;
    private final int num;

    public SceneInsertTask(KuduTemplate kuduTemplate, int num) {
        this.kuduTemplate = kuduTemplate;
        this.num = num / 2;
    }

    @Override
    public Integer call() throws Exception {
        String userInfoJsonStr = "{\"id\":2,\"base_tag_id\":\"user_info\",\"user_id\":\"220107199002028754\",\"phone\":\"13943082549\",\"sex\":\"男\",\"address\":\"北京市朝阳区北二环路\",\"job\":\"程序员\",\"birthday\":-307203927311,\"name\":\"陈建华\",\"cardno\":\"220107199002028754\"}";
        String physicExamJsonStr = "{\"id\":222,\"cardno\":\"220107199002028754\",\"base_tag_id\":\"physic_exam\",\"user_id\":\"220107199002028754\",\"height\":1.73,\"shousuoya\":146,\"shuzhangya\":100,\"dimidudanguchun\":6,\"weight\":95,\"gaomidudanguchun\":0.3,\"yaowei\":105,\"fpg\":20,\"disability\":\"否\",\"disease\":\"无\",\"food_hobby\":\"嗜糖、嗜盐\",\"danguchun\":6.3}";

        String userId;
        JSONObject userInfoJson;
        JSONObject physicExamJson;
        List<JSONObject> userInfoList = Lists.newArrayList();
        List<JSONObject> physicExamList = Lists.newArrayList();
        for (int i = 1; i <= num; i++) {
            userInfoJson   = JSONObject.parseObject(userInfoJsonStr);
            physicExamJson = JSONObject.parseObject(physicExamJsonStr);

            userId = kuduTemplate.getId() + "";

            userInfoJson.put("id", kuduTemplate.getId());
            userInfoJson.put("user_id", userId);

            physicExamJson.put("id", kuduTemplate.getId());
            physicExamJson.put("user_id", userId);

            userInfoList.add(userInfoJson);
            physicExamList.add(physicExamJson);
            if (i % MAX_KUDU_OPERATION_SIZE == 0) {
                kuduTemplate.batchInsert(SCENE_TABLE_NAME, userInfoList);
                kuduTemplate.batchInsert(SCENE_TABLE_NAME, physicExamList);
                userInfoList = Lists.newArrayList();
                physicExamList = Lists.newArrayList();
            }
        }

        kuduTemplate.batchInsert(SCENE_TABLE_NAME, userInfoList);
        kuduTemplate.batchInsert(SCENE_TABLE_NAME, physicExamList);
        return num;
    }
}
