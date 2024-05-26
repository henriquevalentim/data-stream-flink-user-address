package Converter;

import org.bson.Document;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class CountStateByUser implements Function<Map<String, Integer>, List<Document>>, Serializable {
    @Override
    public List<Document> apply(Map<String, Integer> stateCountMap) {
        List<Document> documents = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : stateCountMap.entrySet()) {
            Document document = new Document();
            document.append("state", entry.getKey());
            document.append("count", entry.getValue());
            documents.add(document);
        }
        return documents;
    }
}
