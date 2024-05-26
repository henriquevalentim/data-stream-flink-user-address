package Converter;

import org.bson.Document;

import Dto.User;

import java.io.Serializable;
import java.util.function.Function;

public class UserToDocumentConverter implements Function<User, Document>, Serializable {
    @Override
    public Document apply(User user) {
        Document doc = new Document();
        doc.append("id", user.getId());
        doc.append("name", user.getName());
        doc.append("email", user.getEmail());
        doc.append("genre", user.getGenre());
        doc.append("register_date", user.getRegisterDate().toString());
        return doc;
    }
}
