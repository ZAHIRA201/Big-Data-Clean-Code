package pdzd.miron;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
public class CompaniesExtractLocation extends EvalFunc < String > {
    public String extractCityFromLocality(String locality) {
        String[] tokens = locality.split(",\\s*");
        return tokens[0];
    }
    @Override
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0 || input.get(0) == null) {
            return null;
        }
        try {
            return extractCityFromLocality((String) input.get(0));
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
        return Arrays.asList(new FuncSpec(this.getClass().getName(), s));
    }
}