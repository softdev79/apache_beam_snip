

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.*;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class JoinTable {

    public static void main(String[] args) {
        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        options.setRunner(DataflowRunner.class); // Set the runner to DataflowRunner
        options.setProject("steady-burner-336411");
        options.setRegion("us-east1");
        //options.setGcpTempLocation("gs://dataflow-apache-quickstart_steady-burner-336411/temp/");
        //options.setStagingLocation("gs://dataflow_statging/stg");
        options.setJobName("join-bigquery-tables");
        Pipeline p = Pipeline.create(options);

        /*String query = "insert into `steady-burner-336411.vishal_dataset.output_table select name , d.dept_nm , emp_id from steady-burner-336411.vishal_dataset.employee e inner join steady-burner-336411.vishal_dataset.department d on e.dep_id=d.dept_id";

        PCollection<TableRow> join=p.apply("Read from BQ query",BigQueryIO.readTableRows()
                .fromQuery("select name , d.dept_nm , emp_id from `steady-burner-336411.vishal_dataset.employee` e inner join steady-burner-336411.vishal_dataset.department d on e.dep_id=d.dept_id ;")
                .usingStandardSql()
                .withTemplateCompatibility()
                .withQueryTempDataset("vishal_dataset")
                .withMethod(BigQueryIO.TypedRead.Method.DIRECT_READ));
        join.apply("write to bq",BigQueryIO.writeTableRows()
                .to("steady-burner-336411.vishal_dataset.output_table")
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                .withSchema(BiquerySchema.getOutputSchema())); */
        Map<String, Object> kafkaConfig = new HashMap<>();
        kafkaConfig.put("bootstrap.servers", "pkc-619z3.us-east1.gcp.confluent.cloud:9092");
        kafkaConfig.put("security.protocol", "SASL_SSL");
        kafkaConfig.put("sasl.mechanism", "PLAIN");
        kafkaConfig.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='AVVANESSOLCAIQ3K' password='1e3XJRF0mUhVYa2/binldvCFwk1euwNLIA3laQ0TqsIgKv9rglmVlTj+eYb7Sk9W';");
        kafkaConfig.put("ssl.endpoint.identification.algorithm", "https");
        kafkaConfig.put("client.dns.lookup", "use_all_dns_ips");
        kafkaConfig.put("request.timeout.ms", "30000");
        kafkaConfig.put("retry.backoff.ms", "500");
        ObjectMapper mapper = new ObjectMapper();
        p.apply("ReadFromBigQuery", BigQueryIO.readTableRows()
                        .from("steady-burner-336411.vishal_dataset.employee"))
                .apply("ConvertToJson", MapElements.into(TypeDescriptor.of(String.class))
                        .via((TableRow row) -> {
                            try {
                                // Convert TableRow to JSON string
                                return mapper.writeValueAsString(row);
                            } catch (Exception e) {
                                throw new RuntimeException("Error converting TableRow to JSON", e);
                            }
                        }))
                .apply("WriteToKafka", KafkaIO.<String, String>write()
                        .withBootstrapServers("pkc-619z3.us-east1.gcp.confluent.cloud:9092")
                        .withTopic("bq_to_kafka")
                        .withKeySerializer(StringSerializer.class)
                        .withValueSerializer(StringSerializer.class)
                        .withProducerConfigUpdates(kafkaConfig)
                        .values());

        p.run().waitUntilFinish();

    }
}


