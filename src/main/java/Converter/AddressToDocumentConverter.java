package Converter;

import org.bson.Document;
import Dto.Address;

import java.io.Serializable;
import java.util.function.Function;

public class AddressToDocumentConverter implements Function<Address, Document>, Serializable {
    @Override
    public Document apply(Address address) {
        Document doc = new Document();
        doc.append("userId", address.getUserId());
        doc.append("address", address.getAddress());
        doc.append("city", address.getCity());
        doc.append("state", address.getState());
        doc.append("zip_code", address.getZipCode());
        doc.append("country", address.getCountry());
        return doc;
    }
}
