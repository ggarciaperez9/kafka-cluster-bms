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
        name = "emitec_heightdiff_classify",
        description = "classify Emitec windows using algo_heightdiff AOI's table",
        version = "0.1.0",
        author = "Gabriel Perez"
)
public class EmitecHeightdiffClassifier {

    @Udf(description = "Apply classification model to algo_heightdiff table of AOI")
    public int emitec_heightdiff_classify(
            @UdfParameter(value = "T_HeightDiff3D", description = "the value of T_HeightDiff3D") final double T_HeightDiff3D,
            @UdfParameter(value = "R_HeightDiff", description = "the value of R_HeightDiff") final double R_HeightDiff) throws PredictException, IOException {

        EasyPredictModelWrapper model = new EasyPredictModelWrapper(MojoModel.load("/home/gabriel/Models/PEM_POST_GU74712796CMS/algo_heightdiff/GBM_model_python_1601015708874_18.zip"));

        RowData row = new RowData();
        row.put("T_HeightDiff3D", T_HeightDiff3D);
        row.put("R_HeightDiff", R_HeightDiff);

        BinomialModelPrediction p = model.predictBinomial(row);
        return p.label.equals("true") ? 1 : 0;
    }
}
