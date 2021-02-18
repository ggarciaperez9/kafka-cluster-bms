package com.allcircuits.kafka.udf;

import hex.genmodel.MojoModel;
import hex.genmodel.easy.EasyPredictModelWrapper;
import hex.genmodel.easy.RowData;
import hex.genmodel.easy.exception.PredictException;
import hex.genmodel.easy.prediction.BinomialModelPrediction;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;

import java.io.IOException;

@UdfDescription(
        name = "emitec_blackwhite_classify",
        description = "classify Emitec windows using algo_blackwhite AOI's table",
        version = "0.1.0",
        author = "Gabriel Perez"
)
public class EmitecBlackwhiteClassifier {

    @Udf(description = "Apply classification model to algo_blackwhite table of AOI")
    public int emitec_blackwhite_classify(
            @UdfParameter(value = "R_Percent", description = "the value of R_Percent") final double R_Percent,
            @UdfParameter(value = "T_AreaMin", description = "the value of T_AreaMin") final double T_AreaMin,
            @UdfParameter(value = "R_Area", description = "the value of R_Area") final double R_Area,
            @UdfParameter(value = "T_HeightMean", description = "the value of T_HeightMean") final double T_HeightMean) throws PredictException, IOException {

        EasyPredictModelWrapper model = new EasyPredictModelWrapper(MojoModel.load("/home/gabriel/Models/PEM_POST_GU74712796CMS/algo_blackwhite/GBM_model_python_1601023240913_104.zip"));

        RowData row = new RowData();
        row.put("R_Percent", R_Percent);
        row.put("T_AreaMin", T_AreaMin);
        row.put("R_Area", R_Area);
        row.put("T_HeightMean", T_HeightMean);

        BinomialModelPrediction p = model.predictBinomial(row);
        return p.label.equals("true") ? 1 : 0;
    }
}
