package Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import Dto.Address;

import java.io.IOException;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

public class AddressDeserializationSchema extends AbstractDeserializationSchema<Address> {
    private static final long serialVersionUID = 1L;

    private transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) {
        objectMapper = JsonMapper.builder().addModule(new JavaTimeModule()).build();
    }

    @Override
    public Address deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, Address.class);
    }
}
