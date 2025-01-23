package pdzd.miron;
import me.xdrop.fuzzywuzzy.FuzzySearch;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
public class FuzzyMatchingToken extends EvalFunc < Integer > {

    private final static double MATCH_THRESHOLD = 0.9; // Adjust the threshold as needed
    @Override
    public Integer exec(Tuple input) throws IOException {
        if (input == null || input.size() < 2) {
            return null;
        }
        String s1 = ((String) input.get(0));
        String s2 = ((String) input.get(1));
        try {
            return FuzzySearch.tokenSortPartialRatio(s1, s2);
        } catch (ClassCastException e) {
            throw new IOException("Can't convert to String", e);
        } catch (Exception e) {
            throw new IOException("Unknow error", e);
        }
    }
    @Override
    public List < FuncSpec > getArgToFuncMapping() throws FrontendException {
        Schema s = new Schema();
        s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        return Arrays.asList(new FuncSpec(this.getClass().getName(), s));
    }
}