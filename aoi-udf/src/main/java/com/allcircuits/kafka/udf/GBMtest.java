package com.allcircuits.kafka.udf;

import hex.genmodel.MojoModel;
import hex.genmodel.easy.EasyPredictModelWrapper;
import hex.genmodel.easy.RowData;
import hex.genmodel.easy.prediction.BinomialModelPrediction;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;

@UdfDescription(
        name = "gbm_test",
        description = "To test gbm classifier with tilt data",
        version = "0.1.0",
        author = "Gabriel Perez"
)
public class GBMtest {

    @Udf(description = "Apply classification model to algo_tilt table of AOI")
    public int coyote_tilt_classify(
            @UdfParameter(value = "R_HeightDiff", description = "the value of R_HeightDiff") final double R_HeightDiff,
            @UdfParameter(value = "R_ArrRectHeight1", description = "the value of R_ArrRectHeight1") final double R_ArrRectHeight1,
            @UdfParameter(value = "R_ArrRectHeight2", description = "the value of R_ArrRectHeight2") final double R_ArrRectHeight2,
            @UdfParameter(value = "R_ArrRectHeight3", description = "the value of R_ArrRectHeight3") final double R_ArrRectHeight3,
            @UdfParameter(value = "R_ArrRectHeight4", description = "the value of R_ArrRectHeight4") final double R_ArrRectHeight4,
            @UdfParameter(value = "R_HeightDiffMin", description = "the value of R_HeightDiffMin") final double R_HeightDiffMin,
            @UdfParameter(value = "R_HeightDiffMax", description = "the value of R_HeightDiffMax") final double R_HeightDiffMax) throws Exception {

        EasyPredictModelWrapper model = new EasyPredictModelWrapper(MojoModel.load("/home/gabriel/Models/PEM_POST_COY229564254/algo_tilt/GBM_model_python_1600342804708_290.zip"));

        RowData row = new RowData();
        row.put("R_HeightDiff", R_HeightDiff);
        row.put("R_ArrRectHeight1", R_ArrRectHeight1);
        row.put("R_ArrRectHeight2", R_ArrRectHeight2);
        row.put("R_ArrRectHeight3", R_ArrRectHeight3);
        row.put("R_ArrRectHeight4", R_ArrRectHeight4);
        row.put("R_HeightDiffMin", R_HeightDiffMin);
        row.put("R_HeightDiffMax", R_HeightDiffMax);

        BinomialModelPrediction p = model.predictBinomial(row);
        return p.label.equals("true") ? 1 : 0;
    }
}
