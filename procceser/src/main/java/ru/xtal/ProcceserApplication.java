package ru.xtal;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.core.JdbcTemplate;
import redis.clients.jedis.Jedis;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Map;
import java.util.UUID;

@SpringBootApplication
public class ProcceserApplication {

	@Autowired
	private JdbcTemplate jdbcTemplate;

	public static void main(String[] args) {
		SpringApplication.run(ProcceserApplication.class, args);
	}

	@PostConstruct
	public void post(){
		Jedis jedis = new Jedis("redis://redistogo:4477bf16103cb12b17951d945dbd4834@lab.redistogo.com:9771/");
		while(true){
			System.out.println("Waiting for a message in the queue");
			String message = jedis.blpop(0,"queue").get(1);
			System.out.println("message = "+message);
			try {
				Reader reader = new InputStreamReader(FileUtils.openInputStream(new File(message)), "UTF-8");
				Iterable<CSVRecord> records = CSVFormat.EXCEL.withFirstRecordAsHeader().parse(reader);
				String query = "CREATE TABLE IF NOT EXISTS  test(";
				for(String column:records.iterator().next().toMap().keySet()){
					query+=column.toUpperCase().replace(' ','_').replaceAll("[^A-Za-z0-9_]", "")+" TEXT,";
				}
				query+="id TEXT ,PRIMARY KEY (id));";
				System.out.println(query);
				jdbcTemplate.execute(query);
				for (final CSVRecord record : records) {
                    String columns = "";
                    String values = "'";

                    for(Map.Entry<String, String> entry:record.toMap().entrySet()){
                        columns += entry.getKey().toUpperCase().replace(' ','_').replaceAll("[^A-Za-z0-9_]", "")+",";
                        values += entry.getValue().toUpperCase().replace(' ','_').replaceAll("[^A-Za-z0-9_]", "")+"','";
                    }
                    columns += "id)";
                    values += UUID.randomUUID().toString()+"')";
                    query = "INSERT INTO test ("+columns+" VALUES ("+values;
				    jdbcTemplate.execute(query);
				}
				reader.close();
			}catch (Exception e){
				e.printStackTrace();
			}
		}
	}
}
