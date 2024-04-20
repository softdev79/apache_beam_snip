

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.extensions.sql.SqlTransform;

import org.apache.beam.sdk.options.ValueProvider;

public interface MyOptions extends PipelineOptions, DataflowPipelineOptions {
    ValueProvider<String> getInputTransactionsTable();
    void setInputTransactionsTable(ValueProvider<String> value);

    ValueProvider<String> getInputUsersTable();
    void setInputUsersTable(ValueProvider<String> value);

    ValueProvider<String> getOutputTable();
    void setOutputTable(ValueProvider<String> value);

    }

