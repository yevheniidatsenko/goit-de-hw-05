import json

from confluent_kafka import Consumer, Producer
from configs import kafka_config
from colorama import Fore, Style, init

# Initialize colorama
init(autoreset=True)

# Create Kafka Consumer
consumer = Consumer(
    {
        "bootstrap.servers": kafka_config["bootstrap_servers"],
        "security.protocol": kafka_config["security_protocol"],
        "sasl.mechanism": kafka_config["sasl_mechanism"],
        "sasl.username": kafka_config["username"],
        "sasl.password": kafka_config["password"],
        "group.id": "sensor_group",
        "auto.offset.reset": "earliest",
    }
)

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

# Subscribe to the topic
consumer.subscribe(["building_sensors"])

# Function to process a message
def process_message(message):
    try:
        raw_data = message.value().decode("utf-8")
        print(Fore.BLUE + f"Raw data received: {raw_data}")

        data = json.loads(raw_data)
        if not isinstance(data, dict):
            print(Fore.RED + "Invalid data format.")
            return

        sensor_id = data["sensor_id"]
        temperature = data["temperature"]
        humidity = data["humidity"]

        print(Fore.GREEN + f"Processed data: {data}")
        print("-" * 50)

        # Generate alerts
        if temperature > 40:
            alert_message = {
                "sensor_id": sensor_id,
                "temperature": temperature,
                "timestamp": data["timestamp"],
                "message": "Temperature exceeds threshold!",
            }
            print(Fore.RED + f"ALERT: Temperature exceeds threshold! Sending alert...")
            # Send to the topic
            producer.produce("temperature_alerts", key=sensor_id, value=json.dumps(alert_message))
            print(Fore.MAGENTA + "Data sent to topic 'temperature_alerts'")
            print("-" * 50)

        if humidity > 80 or humidity < 20:
            alert_message = {
                "sensor_id": sensor_id,
                "humidity": humidity,
                "timestamp": data["timestamp"],
                "message": "Humidity exceeds threshold!",
            }
            print(Fore.CYAN + f"ALERT: Humidity exceeds threshold! Sending alert...")
            # Send to the topic
            producer.produce("humidity_alerts", key=sensor_id, value=json.dumps(alert_message))
            print(Fore.MAGENTA + "Data sent to topic 'humidity_alerts'")
            print("-" * 50)

        # Send messages to the appropriate topic
        producer.flush()
        print(Fore.WHITE + "Filtered data has been successfully sent to the appropriate topics.")
        print("=" * 50)

    except Exception as e:
        print(Fore.RED + f"Error processing message: {e}")

# Start the Consumer
try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            print(Fore.YELLOW + "No messages received.")
            continue
        if msg.error():
            print(Fore.RED + f"Consumer error: {msg.error()}")
            continue

        process_message(msg)
except KeyboardInterrupt:
    print(Fore.MAGENTA + "Consumer interrupted by user.")
finally:
    consumer.close()
    print(Fore.GREEN + "Consumer closed.")