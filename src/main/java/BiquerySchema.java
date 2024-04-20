import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;

import java.util.ArrayList;
import java.util.List;

public class BiquerySchema {
    public static TableSchema getOutputSchema()

    {
        List<TableFieldSchema> fields =new ArrayList<>();
        fields.add(new TableFieldSchema().setName("emp_name").setType("STRING"));
        fields.add(new TableFieldSchema().setName("dept_name").setType("STRING"));
        fields.add(new TableFieldSchema().setName("emp_id").setType("INTEGER"));
        return new TableSchema().setFields(fields);
    }

}
