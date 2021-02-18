package com.allcircuits.kafka.udf;

import hex.genmodel.MojoModel;
import hex.genmodel.easy.EasyPredictModelWrapper;
import hex.genmodel.easy.RowData;
import hex.genmodel.easy.exception.PredictException;
import hex.genmodel.easy.prediction.BinomialModelPrediction;
import hex.genmodel.easy.prediction.MultinomialModelPrediction;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;

import java.io.IOException;

@UdfDescription(
        name = "emintec_bridge_classify",
        description = "classify Emintec windows using algo_leadsolder AOI's data",
        version = "0.1.0",
        author = "Gabriel Perez"
)
public class EmintecLeadsolderClassifier {

    @Udf(description = "Apply classification model to algo_leadsolder table of AOI")
    public int emintec_bridge_classify(
            @UdfParameter(value = "T_ToleranceBand3D", description = "the value of T_ToleranceBand3D") final double T_ToleranceBand3D,
            @UdfParameter(value = "R_BWPercent", description = "the value of R_BWPercent") final double R_BWPercent,
            @UdfParameter(value = "R_3DPercent", description = "the value of R_3DPercent") final double R_3DPercent) throws PredictException, IOException {

        EasyPredictModelWrapper model = new EasyPredictModelWrapper(MojoModel.load("/home/gabriel/Models/PEM_POST_GU74712796CMS/algo_leadsolder/GBM_model_python_1600930320161_307.zip"));

        RowData row = new RowData();
        row.put("T_ToleranceBand3D", T_ToleranceBand3D);
        row.put("R_BWPercent", R_BWPercent);
        row.put("R_3DPercent", R_3DPercent);

        BinomialModelPrediction p = model.predictBinomial(row);
        return p.label.equals("true") ? 1 : 0;
    }
}
