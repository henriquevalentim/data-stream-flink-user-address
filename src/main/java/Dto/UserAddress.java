package Dto;

import org.bson.Document;
import java.util.List;
import java.util.ArrayList;

public class UserAddress {
    private User user;
    private List<Address> addresses;

    public UserAddress(User user, List<Address> addresses) {
        this.user = user;
        this.addresses = addresses;
    }

    public UserAddress(User user) {
        this.user = user;
        this.addresses = new ArrayList<>();
    }

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }

    public List<Address> getAddresses() {
        return addresses;
    }

    public void setAddresses(List<Address> addresses) {
        this.addresses = addresses;
    }

    public void addAddress(Address address) {
        this.addresses.add(address);
    }

    public Document toDocument() {
        Document doc = new Document();
        doc.append("userId", user.getId());
        doc.append("userName", user.getName());
        doc.append("userEmail", user.getEmail());
        doc.append("genre", user.getGenre());
        doc.append("registerDate", user.getRegisterDate());

        List<Document> addressDocuments = new ArrayList<>();
        for (Address address : addresses) {
            Document addressDoc = new Document();
            addressDoc.append("address", address.getAddress());
            addressDoc.append("city", address.getCity());
            addressDoc.append("state", address.getState());
            addressDoc.append("zipCode", address.getZipCode());
            addressDoc.append("country", address.getCountry());
            addressDocuments.add(addressDoc);
        }
        doc.append("addresses", addressDocuments);
        return doc;
    }
}
