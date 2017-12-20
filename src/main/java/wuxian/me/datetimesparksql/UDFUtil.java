package wuxian.me.datetimesparksql;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;

public class UDFUtil {

    private UDFUtil() {
    }

    public static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String DATE_FORMAT = "yyyy-MM-dd";

    public static ObjectInspectorConverters.Converter tryGetConverterFrom(ObjectInspector objectInspector
            , ObjectInspector compare) {
        try {
            return ObjectInspectorConverters.getConverter(objectInspector, compare);
        } catch (Exception e) {

        }
        return null;
    }
}
