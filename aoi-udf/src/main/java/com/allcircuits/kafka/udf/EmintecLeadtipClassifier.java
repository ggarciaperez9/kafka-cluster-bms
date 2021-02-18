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
        name = "emintec_leadtip_classify",
        description = "classify Emintec windows using algo_leadtip AOI's data",
        version = "0.1.0",
        author = "Gabriel Perez"
)
public class EmintecLeadtipClassifier {

    @Udf(description = "Apply classification model to algo_leadtip table of AOI")
    public int emintec_leadtip_classify(
            @UdfParameter(value = "R_FindOK", description = "the value of R_FindOK") final double R_FindOK) throws PredictException, IOException {

        EasyPredictModelWrapper model = new EasyPredictModelWrapper(MojoModel.load("/home/gabriel/Models/PEM_POST_GU74712796CMS/algo_leadtip/GBM_model_python_1600930320161_433.zip"));

        RowData row = new RowData();
        row.put("R_FindOK", R_FindOK);

        BinomialModelPrediction p = model.predictBinomial(row);
        return p.label.equals("true") ? 1 : 0;
    }
}
