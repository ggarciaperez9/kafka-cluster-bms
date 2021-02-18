package com.allcircuits.kafka.udf;

import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

@UdafDescription(
        name = "multiply",
        description = "Multiply a sequence of values. This is a support function for classify the panels",
        version = "0.1.0",
        author = "Gabriel Perez"
)
public class Multiplication {

    private static final String MUL = "MUL";

    @UdafFactory(description = "Compute average of column with type Integer.",
            aggregateSchema = "STRUCT<MUL integer>")
    public static TableUdaf<Integer, Struct, Double> mul() {

        final Schema STRUCT_INT = SchemaBuilder.struct().optional()
                .field(MUL, Schema.OPTIONAL_INT32_SCHEMA)
                .build();

        return new TableUdaf<Integer, Struct, Double>() {

            @Override
            public Struct initialize() {
                return new Struct(STRUCT_INT).put(MUL, 1);
            }

            @Override
            public Struct aggregate(final Integer newValue,
                                    final Struct aggregate) {

                if (newValue == null) {
                    return aggregate;
                }
                return new Struct(STRUCT_INT)
                        .put(MUL, aggregate.getInt32(MUL) * newValue);

            }

            @Override
            public Double map(final Struct aggregate) {

                return (double) aggregate.getInt32(MUL);
            }

            @Override
            public Struct merge(final Struct agg1,
                                final Struct agg2) {

                return new Struct(STRUCT_INT)
                        .put(MUL, agg1.getInt32(MUL) + agg2.getInt64(MUL));
            }

            @Override
            public Struct undo(final Integer valueToUndo,
                               final Struct aggregate) {

                if (valueToUndo != 0) {
                    return new Struct(STRUCT_INT)
                            .put(MUL, aggregate.getInt32(MUL) / valueToUndo);
                }
                return new Struct(STRUCT_INT)
                        .put(MUL, aggregate.getInt32(MUL));
            }
        };
    }
}