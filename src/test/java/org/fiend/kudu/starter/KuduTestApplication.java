package org.fiend.kudu.starter;

import org.junit.runner.RunWith;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class KuduTestApplication {
    public static void main(String[] args) {
        SpringApplication.run(KuduTestApplication.class, args);
    }
}
