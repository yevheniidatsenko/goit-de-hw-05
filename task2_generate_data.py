import json
import uuid
import time
import random
from datetime import datetime

from confluent_kafka import Producer
from colorama import Fore, Style, init

from configs import kafka_config

# Initialize colorama
init(autoreset=True)

# Create Kafka Producer
producer = Producer(
    {
        "bootstrap.servers": kafka_config["bootstrap_servers"],
        "security.protocol": kafka_config["security_protocol"],
        "sasl.mechanism": kafka_config["sasl_mechanism"],
        "sasl.username": kafka_config["username"],
        "sasl.password": kafka_config["password"],
    }
)

# Function to generate and send sensor data
def send_sensor_data():
    print(f"{Fore.YELLOW}Starting data generation...")

    while True:
        # Generate a unique sensor_id for each message
        sensor_id = str(uuid.uuid4())
        # Generate data
        temperature = random.uniform(25, 45)
        humidity = random.uniform(15, 85)
        timestamp = datetime.utcnow().isoformat()

        data = {
            "sensor_id": sensor_id,
            "timestamp": timestamp,
            "temperature": temperature,
            "humidity": humidity,
        }

        # Output data with colored formatting
        print(f"{Fore.BLUE}----------------------------------------{Style.RESET_ALL}")
        print(f"{Fore.CYAN}Sensor ID: {sensor_id}{Style.RESET_ALL}")
        print(f"{Fore.GREEN}Temperature: {temperature:.2f}°C{Style.RESET_ALL}")
        print(f"{Fore.YELLOW}Humidity: {humidity:.2f}%{Style.RESET_ALL}")
        print(f"{Fore.MAGENTA}Timestamp: {timestamp}{Style.RESET_ALL}")
        print(f"{Fore.BLUE}----------------------------------------{Style.RESET_ALL}")

        # Send data to the topic
        topic_name = "building_sensors"
        producer.produce(
            topic_name,
            key=sensor_id,
            value=json.dumps(data),
            callback=lambda err, msg: print(
                f"{Fore.GREEN}Message sent: {msg.value()} to {msg.topic()}{Style.RESET_ALL}"
                if err is None else f"{Fore.RED}Error: {err}{Style.RESET_ALL}"
            ),
        )

        # Clear the Producer buffer
        producer.flush()

        print(f"Data sent to Kafka topic {topic_name}")

        # Delay before the next iteration
        time.sleep(2)

# Start the function
try:
    send_sensor_data()
except KeyboardInterrupt:
    print(f"{Fore.RED}\nData generation stopped by user.{Style.RESET_ALL}")