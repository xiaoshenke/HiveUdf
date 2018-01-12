package wuxian.me.hiveudf.d;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

//Todo:not working
public class GetSparkSessionUDF2 extends UserDefinedAggregateFunction {

    public GetSparkSessionUDF2() {

        List<StructField> inputFields = new ArrayList<>();
        //inputFields.add(DataTypes.createStructField("inputColumn", DataTypes.LongType, true));
        inputSchema = DataTypes.createStructType(inputFields);

        List<StructField> bufferFields = new ArrayList<>();
        bufferFields.add(DataTypes.createStructField("sum", DataTypes.LongType, true));
        //bufferFields.add(DataTypes.createStructField("count", DataTypes.LongType, true));
        bufferSchema = DataTypes.createStructType(bufferFields);
    }

    private StructType inputSchema;
    private StructType bufferSchema;

    public StructType inputSchema() {
        return inputSchema;
    }

    public StructType bufferSchema() {
        return bufferSchema;
    }

    public DataType dataType() {
        return DataTypes.BooleanType;
    }

    public boolean deterministic() {
        return true;
    }

    public void initialize(MutableAggregationBuffer buffer) {
    }

    public void update(MutableAggregationBuffer buffer, Row input) {
    }

    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
    }

    public Object evaluate(Row buffer) {
        return new Boolean(true);
    }
}
