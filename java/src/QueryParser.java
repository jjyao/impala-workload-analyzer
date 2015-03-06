import com.cloudera.impala.analysis.SqlParser;
import com.cloudera.impala.analysis.SqlScanner;

import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Paths;

public class QueryParser {
    public static void main(String[] args) throws Exception {
        String sql = new String(Files.readAllBytes(Paths.get(args[0])));
        SqlScanner scanner = new SqlScanner(new StringReader(sql));
        SqlParser parser = new SqlParser(scanner);
        Object stmt = parser.parse().value;
    }
}
