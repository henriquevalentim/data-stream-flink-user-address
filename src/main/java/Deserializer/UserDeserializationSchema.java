package Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import Dto.User;

import java.io.IOException;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

public class UserDeserializationSchema extends AbstractDeserializationSchema<User> {
    private static final long serialVersionUID = 1L;

    private transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) {
        objectMapper = JsonMapper.builder().build().registerModule(new JavaTimeModule());
    }

    @Override
    public User deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, User.class);
    }
}
