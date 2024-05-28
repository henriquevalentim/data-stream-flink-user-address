package Converter;

import org.bson.Document;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class CountCountryByUser implements Function<Map<String, Integer>, List<Document>>, Serializable {
    @Override
    public List<Document> apply(Map<String, Integer> countryCountMap) {
        List<Document> documents = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : countryCountMap.entrySet()) {
            Document document = new Document();
            document.append("country", entry.getKey());
            document.append("count", entry.getValue());
            documents.add(document);
        }
        return documents;
    }
}