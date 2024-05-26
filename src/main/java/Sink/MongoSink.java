package Sink;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.bson.Document;

import java.io.Serializable;
import java.util.List;
import java.util.function.Function;

public class MongoSink<T> extends RichSinkFunction<T> implements Serializable {
    private final String uri;
    private final String database;
    private final String collection;
    private final Function<T, List<Document>> listConverter;
    private final Function<T, Document> singleConverter;

    private transient MongoClient mongoClient;
    private transient MongoCollection<Document> col;

    public MongoSink(String uri, String database, String collection, Function<T, Document> singleConverter, Function<T, List<Document>> listConverter) {
        this.uri = uri;
        this.database = database;
        this.collection = collection;
        this.singleConverter = singleConverter;
        this.listConverter = listConverter;
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
        if (singleConverter != null) {
            Document document = singleConverter.apply(value);
            col.updateOne(
                new Document("userId", document.getString("userId")),
                new Document("$set", document),
                new UpdateOptions().upsert(true)
            );
        } else if (listConverter != null) {
            List<Document> documents = listConverter.apply(value);
            for (Document document : documents) {
                col.updateOne(
                    new Document("state", document.getString("state")),
                    new Document("$set", document),
                    new UpdateOptions().upsert(true)
                );
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (mongoClient != null) {
            mongoClient.close();
        }
        super.close();
    }
}
