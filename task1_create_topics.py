from confluent_kafka.admin import AdminClient, NewTopic
from configs import kafka_config
from colorama import Fore, Style, init

# Initialize colorama
init(autoreset=True)

# Kafka AdminClient
admin_client = AdminClient({
    'bootstrap.servers': kafka_config['bootstrap_servers'],
    'security.protocol': kafka_config['security_protocol'],
    'sasl.mechanism': kafka_config['sasl_mechanism'],
    'sasl.username': kafka_config['username'],
    'sasl.password': kafka_config['password'],
})

# Define topics
my_name = "zheyadatsenko"
topics = [
    NewTopic(f'{my_name}_building_sensors', num_partitions=1, replication_factor=1),
    NewTopic(f'{my_name}_temperature_alerts', num_partitions=1, replication_factor=1),
    NewTopic(f'{my_name}_humidity_alerts', num_partitions=1, replication_factor=1),
]

# Get existing topics
existing_topics = admin_client.list_topics(timeout=10).topics.keys()

# Filter topics to create
topics_to_create = [t for t in topics if t.topic not in existing_topics]

# Create topics
if topics_to_create:
    try:
        admin_client.create_topics(new_topics=topics_to_create)
        print(Fore.GREEN + "Topics created successfully:")
        for topic in topics_to_create:
            print(Fore.YELLOW + f"  - {topic.topic}")
    except Exception as e:
        print(Fore.RED + f"An error occurred during topic creation: {e}")
else:
    print(Fore.CYAN + "All topics already exist.")

# Display all available topics
print(Fore.MAGENTA + "\n==== Available topics ====")
for topic in existing_topics:
    print(Fore.WHITE + f"ℹ️  {topic}")