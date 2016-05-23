package ru.xtal;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.FileCopyUtils;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import redis.clients.jedis.Jedis;

import java.io.*;

@RestController
@RequestMapping("/api")
public class CsvController {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @RequestMapping(value = "/top", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public Object getTop(@RequestParam(required = true) Long count,
                         @RequestParam(required = true) String field){
        return field;
    }

    @RequestMapping(value = "/upload",method = RequestMethod.POST,produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    @ResponseBody
    public ResponseEntity<?> upload(@RequestParam("file") MultipartFile file) throws IOException{
        if(file.getContentType().equalsIgnoreCase("text/csv")){
            File targetFile = new File("downloaded/" + file.getOriginalFilename());
            targetFile.createNewFile();
            FileUtils.copyInputStreamToFile(file.getInputStream(), targetFile);

            jdbcTemplate.execute("DROP TABLE IF EXISTS test;");

            Jedis jedis = new Jedis("localhost");
            jedis.rpush("queue", "downloaded/" + file.getOriginalFilename());
            return new ResponseEntity<>(HttpStatus.OK);
        }
        return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
    }
}
