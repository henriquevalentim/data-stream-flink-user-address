package Dto;

import java.sql.Timestamp;
import java.util.Objects;

public class User {
    private String id;
    private String name;
    private String email;
    private String genre;
    private Timestamp registerDate;

    public User() {}

    public User(String id, String name, String email, String genre, String registerDate) {
        this.id = id;
        this.name = name;
        this.email = email;
        this.genre = genre;
        this.registerDate = Timestamp.valueOf(registerDate);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getGenre() {
        return genre;
    }

    public void setGenre(String genre) {
        this.genre = genre;
    }

    public Timestamp getRegisterDate() {
        return registerDate;
    }

    public void setRegisterDate(Timestamp registerDate) {
        this.registerDate = registerDate;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("User{");
        sb.append("id=").append(id).append('\'');
        sb.append(", name=").append(name).append('\'');
        sb.append(", email=").append(email).append('\'');
        sb.append(", genre=").append(genre).append('\'');
        sb.append(", registerDate=").append(registerDate).append('\'');
        sb.append('}');
        return sb.toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, email, genre, registerDate);
    }
}
