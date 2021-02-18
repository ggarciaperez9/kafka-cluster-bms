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
        name = "emitec_leadlift_classify",
        description = "classify Emitec windows using algo_leadlift AOI's table",
        version = "0.1.0",
        author = "Gabriel Perez"
)
public class EmitecLeadliftClassifier {

    @Udf(description = "Apply classification model to algo_leadlift table of AOI")
    public int emitec_leadlift_classify(
            @UdfParameter(value = "T_AvgHeight3D", description = "the value of T_AvgHeight3D") final double T_AvgHeight3D,
            @UdfParameter(value = "R_Height", description = "the value of R_Height") final double R_Height) throws PredictException, IOException {

        EasyPredictModelWrapper model = new EasyPredictModelWrapper(MojoModel.load("/home/gabriel/Models/PEM_POST_GU74712796CMS/algo_leadlift/GBM_model_python_1601015708874_1.zip"));

        RowData row = new RowData();
        row.put("T_AvgHeight3D", T_AvgHeight3D);
        row.put("R_Height", R_Height);

        BinomialModelPrediction p = model.predictBinomial(row);
        return p.label.equals("true") ? 1 : 0;
    }
}
