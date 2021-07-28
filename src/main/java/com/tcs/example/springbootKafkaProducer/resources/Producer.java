package com.tcs.example.springbootKafkaProducer.resources;


import com.fasterxml.jackson.databind.JsonNode;
import com.tcs.example.springbootKafkaProducer.model.Employee;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("kafka")
public class Producer {


    @Value("${topic.name}")
    private String topicName;

    @Autowired
    KafkaTemplate<Integer, Employee> kafkaTemplateEm;
    private static final String TOPIC_EM = "employer";

    @Autowired
    KafkaTemplate<String, JsonNode> kafkaTemplate;
    private static final String TOPIC = "employee";

//    @Autowired
//    KafkaTemplate<String, Employer> kafkaTemplateE;
//    private static final String TOPIC_E = "employer";


//    @GetMapping("/publish/employee/{name}")
//    public String postEmployee(@PathVariable("name") final String name){
//
//        kafkaTemplateEm.send(TOPIC_EM, 1626093, new Employee(1626093, name, "SCM"));
//
//        return "Published Successfully";
//    }



    @PostMapping(value = "/publish/newEmployee",consumes = {"application/json"},produces = {"application/json"})
    public String postMessageWithKey(@RequestBody Employee emp){
        System.out.println(emp);
        kafkaTemplateEm.send(topicName, emp.getId(), new Employee(emp.getId(), emp.getName(), emp.getDept()));
        return "Message published successfully";
    }

    @PostMapping(value = "/publish/employee",consumes = {"application/json"},produces = {"application/json"})
    public String postMessage(@RequestBody JsonNode emp){
        System.out.println(emp);
        kafkaTemplate.send(topicName,emp);
//        new Employee(emp.getId(), emp.getName(), emp.getDept())
        return "Message published successfully";
    }

    @PostMapping(value = "/publish/employee/bkp",consumes = {"application/json"},produces = {"application/json"})
    public String postMessageBkp(@RequestBody JsonNode emp){
        System.out.println(emp);
        kafkaTemplate.send(TOPIC,emp);
//        new Employee(emp.getId(), emp.getName(), emp.getDept())
        return "Message published successfully";
    }

    @PostMapping(value = "/publish/newEmployee/bkp",consumes = {"application/json"},produces = {"application/json"})
    public String postMessageWithKeyBkp(@RequestBody Employee emp){
        System.out.println(emp);
        kafkaTemplateEm.send(TOPIC_EM, emp.getId(), new Employee(emp.getId(), emp.getName(), emp.getDept()));
        return "Message published successfully";
    }
//    @GetMapping("/publish/employer/{name}")
//    public String postEmployer(@PathVariable("name") final String name){
//
//        kafkaTemplateE.send(TOPIC_E, "1626093", new Employer(name));
//
//        return "Published Successfully";
//    }
}
