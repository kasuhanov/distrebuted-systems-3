package ru.xtal.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import ru.xtal.service.CsvService;

@RestController
@RequestMapping("/api")
public class CsvController {

    @Autowired
    private CsvService csvService;

    @RequestMapping(value = "/top", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public Object getTop(@RequestParam(required = true) Long count,
                         @RequestParam(required = true) String field){
        csvService.serveFile();
        return field;
    }

    @RequestMapping(value = "/upload",method = RequestMethod.POST,produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    @ResponseBody
    public ResponseEntity<?> upload(@RequestParam("file") MultipartFile file){
        csvService.recieve();
        if(file.getContentType().equalsIgnoreCase("text/csv"))
            return new ResponseEntity<>(HttpStatus.OK);
        return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
    }
}
