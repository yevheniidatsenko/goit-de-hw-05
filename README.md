# Apache Kafka

Ready to tackle the homework? Then let’s dive in! 🎢

Today, you’ll focus on the practical application of Apache Kafka using the Python API, honing your skills in creating topics, writing data to a topic, and reading it.

Scenario:
Imagine you work for a company developing monitoring systems for the Internet of Things (IoT). The primary goal of the system is to collect data from various sensors installed in several buildings and analyze it in real time to track parameters such as temperature and humidity.

To achieve this, Apache Kafka is used as a messaging platform that efficiently transmits and processes large volumes of data.

Your task is **to implement several components of this system using Python and Apache Kafka according to the provided instructions.**

Let this assignment broaden your perspective on the world of streaming data! 🧠

## Step-by-Step Instructions

### 1. Creating Topics in Kafka:

Create three topics in Kafka:

> [!TIP]
> Add your name or another identifier to topic names to ensure they are unique.

- **building_sensors** — stores data from all sensors.
- **temperature_alerts** — stores alerts for temperature exceeding permissible levels.
- **humidity_alerts** — stores alerts for humidity levels outside acceptable ranges.

### 2. Sending Data to Topics:

- Write a Python script that simulates a sensor's work and periodically sends randomly generated data (temperature and humidity) to the building_sensors topic.
- The data should include:

  - Sensor ID
  - Timestamp of data collection
  - Corresponding readings

> [!NOTE]
> Each script execution should simulate only one sensor. To simulate multiple sensors, the script must be run multiple times.
> The sensor ID can be a random constant for a single script run and may change with each new execution.

- Temperature values: random between 25 and 45°C.
- Humidity values: random between 15% and 85%.

### 3. Data Processing:

- Write a Python script that subscribes to the building_sensors topic, reads messages, and processes the data:
  - If the temperature exceeds 40°C, generate an alert and send it to the temperature_alerts topic.
  - If humidity exceeds 80% or drops below 20%, generate an alert and send it to the humidity_alerts topic.
- Alerts should include:
  - Sensor ID
  - Measurement values
  - Timestamp
  - Alert message indicating threshold exceeded.

### 4. Displaying Final Data:

- Write a Python script that subscribes to the temperature_alerts and humidity_alerts topics, reads alerts, and displays the messages on the screen.
