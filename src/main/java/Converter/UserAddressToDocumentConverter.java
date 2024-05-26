package Converter;

import Dto.UserAddress;
import org.bson.Document;

import java.io.Serializable;
import java.util.function.Function;

public class UserAddressToDocumentConverter implements Function<UserAddress, Document>, Serializable {

    @Override
    public Document apply(UserAddress userAddress) {
        return userAddress.toDocument();
    }
}
