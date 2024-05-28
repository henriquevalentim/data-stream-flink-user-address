import json
import random
import time

from faker import Faker
from confluent_kafka import SerializingProducer
from datetime import datetime, timezone

fake = Faker()

def generate_user():
    user = fake.simple_profile()
    genre = random.choice(['M', 'F', 'O'])
    first_name = fake.first_name_male() if genre == "M" else fake.first_name_female()
    last_name = fake.last_name()

    return {
        "id": fake.uuid4(),
        "name": f"{first_name} {last_name}",
        "email": f"{first_name.lower()}.{last_name.lower()}@{fake.domain_name()}",
        "genre": genre,
        'registerDate': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f%z')
    }

def generate_addresses(userId):
    addresses = []
    for _ in range(random.randint(3, 3)):
        address = {
            "userId": userId,
            "address": fake.address(),
            "city": fake.city(),
            "state": fake.state(),
            "zipCode": fake.zipcode(),
            "country": fake.country()
        }
        addresses.append(address)
    return addresses

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def main():
    user_topic = 'user'
    address_topic = 'address'
    producer = SerializingProducer({
        'bootstrap.servers': 'localhost:9092'
    })

    for _ in range(10):
        try:
            user = generate_user()
            addresses = generate_addresses(user['id'])

            # Produce user message
            producer.produce(user_topic,
                             key=user['id'],
                             value=json.dumps(user),
                             on_delivery=delivery_report)
            producer.poll(0)

            # Produce address messages
            for address in addresses:
                print(address)
                producer.produce(address_topic,
                                 key=address['userId'],
                                 value=json.dumps(address),
                                 on_delivery=delivery_report)
                producer.poll(0)
            
            # Sleep to simulate time taken to generate next user and addresses
            # time.sleep(2)
        except BufferError:
            print("Buffer full! Waiting...")
            time.sleep(1)
        except Exception as e:
            print(e)

    producer.flush()

if __name__ == "__main__":
    main()
