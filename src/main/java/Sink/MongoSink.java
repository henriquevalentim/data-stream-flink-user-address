package Sink;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.bson.Document;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Filters;

import java.util.function.Function;

public class MongoSink<T> extends RichSinkFunction<T> {
    private final String uri;
    private final String database;
    private final String collection;
    private final Function<T, Document> converter;

    private transient MongoClient mongoClient;
    private transient MongoCollection<Document> col;

    public MongoSink(String uri, String database, String collection, Function<T, Document> converter) {
        this.uri = uri;
        this.database = database;
        this.collection = collection;
        this.converter = converter;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        mongoClient = MongoClients.create(uri);
        MongoDatabase db = mongoClient.getDatabase(database);
        col = db.getCollection(collection);
    }

    @Override
    public void invoke(T value, Context context) {
        Document document = converter.apply(value);
        String userId = document.getString("userId");
        col.updateOne(
            Filters.eq("userId", userId),
            new Document("$set", document),
            new UpdateOptions().upsert(true)
        );
    }

    @Override
    public void close() throws Exception {
        if (mongoClient != null) {
            mongoClient.close();
        }
        super.close();
    }
}
