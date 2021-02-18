package com.allcircuits.kafka.udf;

import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.ArrayList;
import java.util.List;

@UdafDescription(
        name = "coyote_classify_panel",
        description = "Utilize the classifications of all algorithms in every window to classify the panel",
        version = "0.1.0",
        author = "Gabriel Perez"
)
public class ClassifyPanel {

    //public static final String INPUT_STRUCT_DESCRIPTOR = "STRUCT<" + "insp_st_time LONG," + "barcode STRING," + "PREDICTION STRING," + ">";

    public static final String OUTPUT_STRUCT_DESCRIPTOR = "STRUCT<" + "insp_st_time LONG," + "barcode STRING," + "PROB DOUBLE," + "PASS_CODE INT," + "ISREQUIRED INT," + ">";

    public static final Schema OUTPUT_SCHEMA = SchemaBuilder.struct().optional()
            .field("insp_st_time", Schema.OPTIONAL_INT64_SCHEMA)
            .field("barcode", Schema.OPTIONAL_STRING_SCHEMA)
            .field("SUM", Schema.OPTIONAL_INT64_SCHEMA)
            .field("COUNT", Schema.OPTIONAL_INT64_SCHEMA)
            .field("PASS_CODE", Schema.OPTIONAL_INT32_SCHEMA)
            .field("ISREQUIRED", Schema.OPTIONAL_INT32_SCHEMA)
            .build();

    @UdafFactory(description = "Compute average of column with type Long.", aggregateSchema = OUTPUT_STRUCT_DESCRIPTOR)
    public Udaf<String, Struct, Double> coyote_classify_panel(
            @UdfParameter(value = "insp_st_time", description = "the value of insp_st_time") final long insp_st_time,
            @UdfParameter(value = "barcode", description = "the value of barcode") final String barcode) {

        return new Udaf<String, Struct, Double>() {

            @Override
            public Struct initialize() {
                return new Struct(OUTPUT_SCHEMA)
                        .put("insp_st_time", insp_st_time)
                        .put("barcode", barcode)
                        .put("SUM", 0.0)
                        .put("COUNT", 0)
                        .put("PASS_CODE", 0)
                        .put("ISREQUIRED", 0);
            }

            @Override
            public Struct aggregate(final String newValue,
                                    final Struct aggregate) {

                if (newValue == null) {
                    return aggregate;
                }
                String [] c = newValue.split("-");

                if (c[1] == "true") {
                    return new Struct(OUTPUT_SCHEMA)
                            .put("insp_st_time", insp_st_time)
                            .put("barcode", barcode)
                            .put("SUM", aggregate.getFloat64("SUM") + Double.parseDouble(c[0]))
                            .put("COUNT", aggregate.getInt64("COUNT") + 1)
                            .put("PASS_CODE", aggregate.getInt32("PASS_CODE"))
                            .put("ISREQUIRED", c[2]);
                }

                return new Struct(OUTPUT_SCHEMA)
                        .put("insp_st_time", insp_st_time)
                        .put("barcode", barcode)
                        .put("SUM", aggregate.getFloat64("SUM") + Double.parseDouble(c[0]))
                        .put("COUNT", aggregate.getInt64("COUNT") + 1)
                        .put("PASS_CODE", 1)
                        .put("ISREQUIRED", c[2]);

            }

            @Override
            public Double map(final Struct aggregate) {
                final long count = aggregate.getInt64("COUNT");
                if (count == 0) {
                    return 0.0;
                }
                return aggregate.getInt64("SUM") / ((double)count);
            }

            @Override
            public Struct merge(final Struct agg1,
                                final Struct agg2) {

                return new Struct(OUTPUT_SCHEMA)
                        .put("SUM", agg1.getInt32("SUM") + agg2.getInt64("SUM"))
                        .put("COUNT", agg1.getInt64("COUNT") + agg2.getInt64("COUNT"));
            }

        };

    }
}
