package com.allcircuits.kafka.udf;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;

import java.util.Arrays;
import java.util.OptionalDouble;

@UdfDescription(
        name = "multi",
        description = "Multiply 2 numbers",
        version = "0.1.0",
        author = "Gabriel Perez"
)
public class Multi {

    /*@Udf(description = "multiply two non-nullable INTs.")
    public long multi(
            @UdfParameter(value = "V1", description = "the first value") final int v1,
            @UdfParameter(value = "V2", description = "the second value") final int v2) {
        return v1 * v2;
    }

    @Udf(description = "multiply two non-nullable BIGINTs.")
    public long multi(
            @UdfParameter("V1") final long v1,
            @UdfParameter("V2") final long v2) {
        return v1 * v2;
    }

    @Udf(description = "multiply two nullable BIGINTs. If either param is null, null is returned.")
    public Long multi(final Long v1, final Long v2) {
        return v1 == null || v2 == null ? null : v1 * v2;
    }*/

    @Udf(description = "multiply two non-nullable DOUBLEs.")
    public double multi(final double v1, final double v2) {
        return v1 * v2;
    }

    @Udf(description = "multiply two non-nullable STRINGS.")
    public double multi(final String v1, final String v2) {
        return Double.parseDouble(v1) * Double.parseDouble(v2);
    }

    @Udf(description = "multiply an STRING by a DOUBLE.")
    public double multi(final String v1, final double v2) {
        return Double.parseDouble(v1) * v2;
    }

    @Udf(description = "multiply N non-nullable DOUBLEs.")
    public OptionalDouble multi(final double... values) {
        return Arrays.stream(values).reduce((a, b) -> a * b);
    }
}