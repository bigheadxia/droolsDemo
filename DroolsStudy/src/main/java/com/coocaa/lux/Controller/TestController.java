package com.coocaa.lux.Controller;

import com.coocaa.lux.core.Message;
import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;


/**
 * Created by chenfeilong on 2017/2/17.
 */
@Controller
@RequestMapping("/")
public class TestController {

    @RequestMapping("/index")
    @ResponseBody
    String index() {
        return "index Page!";
    }

    @RequestMapping("/droolsTest")
    @ResponseBody
    public String droolsTest() {
        try {
            // load up the knowledge base
            KieServices ks = KieServices.Factory.get();
            KieContainer kContainer = ks.getKieClasspathContainer();
            KieSession kSession = kContainer.newKieSession("session-test");

            // go !
            Message message = new Message();
            message.setMessage("Hello World");
            message.setStatus(Message.HELLO);
            kSession.insert(message);//插入
            kSession.fireAllRules();//执行规则
            kSession.dispose();
            return message.getMessage();
        } catch (Throwable t) {
            t.printStackTrace();
            return "error";
        }
    }

}
