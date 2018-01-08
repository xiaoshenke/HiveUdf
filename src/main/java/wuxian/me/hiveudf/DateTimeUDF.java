package wuxian.me.hiveudf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.NDV;
import org.apache.hadoop.hive.serde2.io.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.joda.time.DateTime;
import org.apache.hadoop.io.IntWritable;
import org.joda.time.format.DateTimeFormat;
import wuxian.me.hiveudf.util.UDFUtil;

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Set;

import static wuxian.me.hiveudf.util.UDFUtil.DATE_TIME_FORMAT;
import static wuxian.me.hiveudf.util.UDFUtil.tryGetConverterFrom;

/**
 * single function supported:
 * 1 select date_time();
 * 2 select date_time(current_date(),'minus_day',1)
 * 3 select date_time(current_date(),'day_first_second')
 * or combine them:
 * select date_time(date_time(current_date(),'minus_day',1),'day_first_second') etc.
 */

@UDFType(deterministic = true)
@Description(name = "date_time",
        value = "_FUNC_() - a bounch of date_time util functions,minus_day,minus_month,day_first_second.")
@NDV(maxNdv = 1)
public class DateTimeUDF extends GenericUDF {

    public static final String MINUS_DAY = "minus_day";
    public static final String PLUS_DAY = "plus_day";
    public static final String MINUS_MONTH = "minus_month";

    public static final String DAY_FIRST_SECOND = "day_first_second";
    public static final String DAY_LAST_SECOND = "day_last_second";

    private static Set<String> supportedFunction = new HashSet<String>();

    static {
        supportedFunction.add(MINUS_DAY);
        supportedFunction.add(PLUS_DAY);
        supportedFunction.add(MINUS_MONTH);
        supportedFunction.add(DAY_FIRST_SECOND);
        supportedFunction.add(DAY_LAST_SECOND);
    }


    private ObjectInspector[] objectInspectors;

    private DateTime dateTime;

    /**
     * 和evaluate函数一样,对于length长度为0的情况做特殊处理
     */
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        this.objectInspectors = objectInspectors;

        return PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
    }

    private DateTime getDateTime(ObjectInspector[] objectInspectors, DeferredObject[] deferredObjects)
            throws HiveException, UDFArgumentException {

        ObjectInspector dateOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
                PrimitiveObjectInspector.PrimitiveCategory.DATE);
        ObjectInspector timestampOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
                PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMP);

        ObjectInspector stringOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
                PrimitiveObjectInspector.PrimitiveCategory.STRING);

        ObjectInspectorConverters.Converter dateConverter = tryGetConverterFrom(objectInspectors[0], dateOI);
        ObjectInspectorConverters.Converter timestampConverter = tryGetConverterFrom(objectInspectors[0], timestampOI);
        ObjectInspectorConverters.Converter stringConverter = tryGetConverterFrom(objectInspectors[0], stringOI);

        if (dateConverter == null && timestampConverter == null && stringConverter == null) {
            throw new UDFArgumentException("first argument must be date or timestamp!");
        }

        DateTime dateTime = null;

        if (stringConverter != null) {
            Text hiveCharWritable = (Text) stringConverter.convert(deferredObjects[0].get());
            if (hiveCharWritable != null && hiveCharWritable.toString().length() != 0) {

                String time = hiveCharWritable.toString();

                try {
                    dateTime = DateTimeFormat.forPattern(UDFUtil.DATE_FORMAT).parseDateTime(time);
                } catch (Exception e) {
                    ;
                }
                if (dateTime == null) {
                    try {
                        dateTime = DateTimeFormat.forPattern(UDFUtil.DATE_TIME_FORMAT).parseDateTime(time);
                    } catch (Exception e) {
                        ;
                    }
                }
                if (dateTime == null) {
                    throw new UDFArgumentException("cannot parse date string: " + time + " ,please check out it!");
                }

            } else {
                throw new UDFArgumentException("date string is not illegal!");
            }

        } else if (timestampConverter != null) {
            TimestampWritable timestampWritable = (TimestampWritable) timestampConverter.convert(deferredObjects[0].get());
            dateTime = new DateTime(timestampWritable.getTimestamp().getTime());

        } else {
            DateWritable dateWritable = (DateWritable) dateConverter.convert(deferredObjects[0].get());
            dateTime = new DateTime(dateWritable.get());
        }

        return dateTime;
    }

    private Text getFuncName(ObjectInspector[] objectInspectors, DeferredObject[] deferredObjects)
            throws HiveException, UDFArgumentException {

        ObjectInspector stringOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
                PrimitiveObjectInspector.PrimitiveCategory.STRING);
        ObjectInspectorConverters.Converter stringConverter = tryGetConverterFrom(objectInspectors[1], stringOI);
        if (stringConverter == null) {
            throw new UDFArgumentException("second argument must be string!");
        }

        Text hiveCharWritable = (Text) stringConverter.convert(deferredObjects[1].get());
        return hiveCharWritable;
    }

    private String functionName;

    private void checkPreservedObjectInspectors(ObjectInspector[] objectInspectors, DeferredObject[] deferredObjects)
            throws HiveException, UDFArgumentException {

        dateTime = getDateTime(objectInspectors, deferredObjects);

        if (objectInspectors.length >= 2) {
            functionName = getFuncName(objectInspectors, deferredObjects).toString();
            if (!supportedFunction.contains(functionName)) {
                throw new HiveException("func: " + functionName + " is not supported!");
            }
        }

    }

    private void handleMinusDay(ObjectInspector[] objectInspectors, DeferredObject[] deferredObjects)
            throws HiveException, UDFArgumentException {

        ObjectInspector intOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
                PrimitiveObjectInspector.PrimitiveCategory.INT);
        ObjectInspectorConverters.Converter numberConverter = tryGetConverterFrom(objectInspectors[2], intOI);
        if (numberConverter == null) {
            throw new UDFArgumentException("third argument must be int!");
        }

        IntWritable intWritable = (IntWritable) numberConverter.convert(deferredObjects[2].get());
        dateTime = dateTime.minusDays(intWritable.get());
    }

    private void handleMinusMonth(ObjectInspector[] objectInspectors, DeferredObject[] deferredObjects)
            throws HiveException, UDFArgumentException {

        ObjectInspector intoi = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
                PrimitiveObjectInspector.PrimitiveCategory.INT);
        ObjectInspectorConverters.Converter numberConverter = tryGetConverterFrom(objectInspectors[2], intoi);
        if (numberConverter == null) {
            throw new UDFArgumentException("third argument must be int!");
        }

        IntWritable intWritable = (IntWritable) numberConverter.convert(deferredObjects[2].get());
        dateTime = dateTime.minusMonths(intWritable.get());
    }


    private void handleFirstSecond(ObjectInspector[] objectInspectors, DeferredObject[] deferredObjects)
            throws HiveException, UDFArgumentException {
        dateTime = dateTime.hourOfDay().withMinimumValue().millisOfDay().withMinimumValue();
    }

    //Usage: select date_time(date_time(current_timestamp(),'minus_day',1),'day_first_second');
    private void handleLastSecond(ObjectInspector[] objectInspectors, DeferredObject[] deferredObjects)
            throws HiveException, UDFArgumentException {
        dateTime = dateTime.hourOfDay().withMaximumValue().millisOfDay().withMaximumValue();
    }

    private void handleFunction(String functionName, ObjectInspector[] objectInspectors, DeferredObject[] deferredObjects)
            throws HiveException, UDFArgumentException {
        if (functionName == null || functionName.length() == 0) {
            return;
        }

        if (functionName.equals(MINUS_DAY)) {
            handleMinusDay(objectInspectors, deferredObjects);
        } else if (functionName.equals(DAY_FIRST_SECOND)) {
            handleFirstSecond(objectInspectors, deferredObjects);

        } else if (functionName.equals(DAY_LAST_SECOND)) {
            handleLastSecond(objectInspectors, deferredObjects);

        } else if (functionName.equals(MINUS_MONTH)) {
            handleMinusMonth(objectInspectors, deferredObjects);

        } else {
            throw new HiveException("func: " + functionName + " is not supported!");
        }
    }

    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {

        if (deferredObjects.length == 0) {
            return new TimestampWritable(Timestamp.valueOf(DateTime.now().toString(DATE_TIME_FORMAT)));
        }
        checkPreservedObjectInspectors(this.objectInspectors, deferredObjects);
        if (functionName != null) {
            handleFunction(functionName, objectInspectors, deferredObjects);
        }

        return new TimestampWritable(Timestamp.valueOf(dateTime.toString(DATE_TIME_FORMAT)));
    }

    public String getDisplayString(String[] strings) {
        return "DATE_TIME()";
    }
}
