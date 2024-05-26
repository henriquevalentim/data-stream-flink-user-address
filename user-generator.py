import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import Converter.UserAddressToDocumentConverter;
import Deserializer.AddressDeserializationSchema;
import Deserializer.UserDeserializationSchema;
import Dto.Address;
import Dto.User;
import Dto.UserAddress;
import Sink.MongoSink;

import java.util.ArrayList;
import java.util.List;

public class Main {

    private static final String BROKERS = "localhost:9092";
    private static final String MONGO_USERNAME = "root";
    private static final String MONGO_PASSWORD = "123456";
    private static final String MONGO_URI = "mongodb://" + MONGO_USERNAME + ":" + MONGO_PASSWORD + "@localhost:27017";
    private static final String MONGO_DATABASE_USERADDRESS = "userAddress";
    private static final String MONGO_COLLECTION_USERADDRESS = "userAddress";

    public static void main(String[] args) throws Exception {

        // Step 1: Create the StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Step 2: Define Kafka source
        KafkaSource<User> sourceUser = KafkaSource.<User>builder()
                .setBootstrapServers(BROKERS)
                .setProperty("partition.discovery.interval.ms", "1000")
                .setTopics("user")
                .setGroupId("groupId-919292")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new UserDeserializationSchema())
                .build();

        KafkaSource<Address> sourceAddress = KafkaSource.<Address>builder()
                .setBootstrapServers(BROKERS)
                .setProperty("partition.discovery.interval.ms", "1000")
                .setTopics("address")
                .setGroupId("groupId-919293")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new AddressDeserializationSchema())
                .build();

        // Step 3: Create a DataStreamSource from Kafka
        DataStreamSource<User> usersStream = env.fromSource(sourceUser, WatermarkStrategy.noWatermarks(), "Kafka Source user");
        DataStreamSource<Address> addressStream = env.fromSource(sourceAddress, WatermarkStrategy.noWatermarks(), "Kafka Source address");

        // Step 4: Key streams by userId
        DataStream<User> keyedUserStream = usersStream.keyBy((KeySelector<User, String>) User::getId);
        DataStream<Address> keyedAddressStream = addressStream.keyBy((KeySelector<Address, String>) Address::getUserId);

        // Step 5: Combine the streams using KeyedCoProcessFunction
        SingleOutputStreamOperator<UserAddress> userAddressStream = keyedUserStream
                .connect(keyedAddressStream)
                .keyBy(User::getId, Address::getUserId)
                .process(new KeyedCoProcessFunction<String, User, Address, UserAddress>() {
                    // State to store User and Addresses
                    private transient MapState<String, User> userState;
                    private transient MapState<String, List<Address>> addressState;
                    private transient MapState<String, Boolean> emittedState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor<String, User> userStateDescriptor = new MapStateDescriptor<>(
                                "userState",
                                String.class,
                                User.class
                        );
                        userState = getRuntimeContext().getMapState(userStateDescriptor);

                        MapStateDescriptor<String, List<Address>> addressStateDescriptor = new MapStateDescriptor<>(
                                "addressState",
                                String.class,
                                (Class<List<Address>>)(Class<?>)List.class
                        );
                        addressState = getRuntimeContext().getMapState(addressStateDescriptor);

                        MapStateDescriptor<String, Boolean> emittedStateDescriptor = new MapStateDescriptor<>(
                                "emittedState",
                                String.class,
                                Boolean.class
                        );
                        emittedState = getRuntimeContext().getMapState(emittedStateDescriptor);
                    }

                    @Override
                    public void processElement1(User user, Context ctx, Collector<UserAddress> out) throws Exception {
                        // Update user state
                        userState.put(user.getId(), user);

                        // Check if we've already emitted this user
                        if (Boolean.TRUE.equals(emittedState.get(user.getId()))) {
                            return;
                        }

                        // Emit combined user and addresses if addresses are available
                        List<Address> addresses = addressState.get(user.getId());
                        if (addresses != null) {
                            out.collect(new UserAddress(user, addresses));
                            emittedState.put(user.getId(), true);
                        }
                    }

                    @Override
                    public void processElement2(Address address, Context ctx, Collector<UserAddress> out) throws Exception {
                        // Update address state
                        List<Address> addresses = addressState.get(address.getUserId());
                        if (addresses == null) {
                            addresses = new ArrayList<>();
                        }
                        addresses.add(address);
                        addressState.put(address.getUserId(), addresses);

                        // Check if we've already emitted this user
                        if (Boolean.TRUE.equals(emittedState.get(address.getUserId()))) {
                            return;
                        }

                        // Emit combined user and addresses if user is available
                        User user = userState.get(address.getUserId());
                        if (user != null) {
                            out.collect(new UserAddress(user, addresses));
                            emittedState.put(address.getUserId(), true);
                        }
                    }
                });

        // Step 6: Sink the combined stream to MongoDB
        userAddressStream.addSink(new MongoSink<>(MONGO_URI, MONGO_DATABASE_USERADDRESS, MONGO_COLLECTION_USERADDRESS, new UserAddressToDocumentConverter()))
                .name("MongoDB Sink for UserAddress");

        // Step 7: Execute the Flink job
        env.execute("Kafka-flink-stream-mongo");
    }
}