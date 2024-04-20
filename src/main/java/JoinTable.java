

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
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

import org.apache.beam.sdk.options.ValueProvider;




public class JoinTable {

    public static void main(String[] args) {
        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        options.setRunner(DataflowRunner.class); // Set the runner to DataflowRunner
        options.setProject("steady-burner-336411");
        options.setRegion("us-east1");
        //options.setGcpTempLocation("gs://dataflow_statging/tmp");
        //options.setStagingLocation("gs://dataflow_statging/stg");
        options.setJobName("join-bigquery-tables");
        Pipeline p = Pipeline.create(options);

        String query = "insert into `steady-burner-336411.vishal_dataset.output_table select name , d.dept_nm , emp_id from steady-burner-336411.vishal_dataset.employee e inner join steady-burner-336411.vishal_dataset.department d on e.dep_id=d.dept_id";

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
                .withSchema(BiquerySchema.getOutputSchema()));
        p.run().waitUntilFinish();
    }
}


