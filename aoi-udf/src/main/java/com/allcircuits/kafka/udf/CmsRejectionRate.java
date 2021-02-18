package com.allcircuits.kafka.udf;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;

@UdfDescription(
        name = "reject_rate",
        description = "Calculate CMS Rejection Rate",
        version = "0.1.0",
        author = "Gabriel Perez"
)
public class CmsRejectionRate {

    @Udf(description = "Calculate the reject rate in percent")
    public double reject_rate(
            @UdfParameter(value = "C_COMPONENTABSENCEAFTERPICK_INT", description = "the value of C_COMPONENTABSENCEAFTERPICK_INT") final int C_COMPONENTABSENCEAFTERPICK_INT,
            @UdfParameter(value = "C_COMPONENTABSENCEBEFOREPLACE_INT", description = "the value of C_COMPONENTABSENCEBEFOREPLACE_INT") final int C_COMPONENTABSENCEBEFOREPLACE_INT,
            @UdfParameter(value = "C_COMPONENTMATERIALDEFECT_INT", description = "the value of C_COMPONENTMATERIALDEFECT_INT") final int C_COMPONENTMATERIALDEFECT_INT,
            @UdfParameter(value = "C_TREATMENTERROR_INT", description = "the value of C_TREATMENTERROR_INT") final int C_TREATMENTERROR_INT,
            @UdfParameter(value = "C_DROPPEDERROR_INT", description = "the value of C_DROPPEDERROR_INT") final int C_DROPPEDERROR_INT,
            @UdfParameter(value = "C_IDENTERROR_INT", description = "the value of C_IDENTERROR_INT") final int C_IDENTERROR_INT,
            @UdfParameter(value = "C_PICKUPRETRIES_INT", description = "the value of C_PICKUPRETRIES_INT") final int C_PICKUPRETRIES_INT,
            @UdfParameter(value = "C_REJECTIDENT_INT", description = "the value of C_REJECTIDENT_INT") final int C_REJECTIDENT_INT,
            @UdfParameter(value = "C_REJECTVACUUM_INT", description = "the value of C_REJECTVACUUM_INT") final int C_REJECTVACUUM_INT,
            @UdfParameter(value = "C_ACCESSTOTAL_INT", description = "the value of C_ACCESSTOTAL_INT") final int C_ACCESSTOTAL_INT){

        int rejected = C_REJECTIDENT_INT + C_REJECTVACUUM_INT;
        int bad_comp = C_COMPONENTABSENCEAFTERPICK_INT + C_COMPONENTABSENCEBEFOREPLACE_INT + C_COMPONENTMATERIALDEFECT_INT + C_TREATMENTERROR_INT + C_DROPPEDERROR_INT + C_IDENTERROR_INT + C_PICKUPRETRIES_INT;
        int errors = (bad_comp >= rejected) ? bad_comp : rejected;
        return (errors / C_ACCESSTOTAL_INT) * 100;
    }

    @Udf(description = "Calculate the cost of the rejected components")
    public double reject_rate(
            @UdfParameter(value = "PLACEDCOMPONENTS_INT", description = "the value of PLACEDCOMPONENTS_INT") final int PLACEDCOMPONENTS_INT,
            @UdfParameter(value = "ACCESSTOTAL_INT", description = "the value of ACCESSTOTAL_INT") final int ACCESSTOTAL_INT,
            @UdfParameter(value = "PRICE", description = "the value of PRICE") final double PRICE){

        Double price = new Double(PRICE);
        return (price == 0.0 || price == null) ? 0.0 : (ACCESSTOTAL_INT - PLACEDCOMPONENTS_INT) * price;
    }
}
