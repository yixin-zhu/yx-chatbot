package org.example.clients;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.example.client.ZhipuAIStreamClient;

import static java.lang.Thread.sleep;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ZhipuAIStreamClientTest {
    @Autowired
    ZhipuAIStreamClient zhipuAIStreamClient;

    @Test
    void streamChatCompletionProcessesValidResponse() throws Exception {
        zhipuAIStreamClient = new ZhipuAIStreamClient();
        try{
            zhipuAIStreamClient.streamChatCompletion("please introduce yourself");
            Thread.sleep(100000);
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}