package pdzd.miron;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
public class ClassifyRiskGroup extends EvalFunc < String > {
    @Override
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0 || input.get(0) == null) {
            return null;
        }
        int totalInjuries = (int) input.get(0);
        try {
            if (totalInjuries == 0) {
                return "NO_HARM";
            } else if (totalInjuries > 0 && totalInjuries <= 2) {
                return "SAFE";
            } else if (totalInjuries > 2 && totalInjuries <= 10) {
                return "MEDIUM";
            } else if (totalInjuries > 10 && totalInjuries <= 29) {
                return "DANGEROUS";
            } else {
                return "EXTREME";
            }
        } catch (ClassCastException e) {
            throw new IOException("Can't convert to String", e);
        } catch (Exception e) {
            throw new IOException("Unknow error", e);
        }
    }
    @Override
    public List < FuncSpec > getArgToFuncMapping() throws FrontendException {
        Schema s = new Schema();
        s.add(new Schema.FieldSchema(null, DataType.INTEGER));
        return Arrays.asList(new FuncSpec(this.getClass().getName(), s));
    }
}