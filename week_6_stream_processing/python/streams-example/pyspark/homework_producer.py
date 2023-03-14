import csv
from time import sleep
from typing import Dict
from kafka import KafkaProducer

from settings import BOOTSTRAP_SERVERS, PRODUCE_TOPIC_RIDES_CSV


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for record {}: {}".format(msg.key(), err))
        return
    print('Record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


class RideCSVProducer:
    def __init__(self, props: Dict):
        self.producer = KafkaProducer(**props)
        # self.producer = Producer(producer_props)

    @staticmethod
    def read_records(resource_path: str, dataset: str):
        records, ride_keys = [], []
        i = 0
        with open(resource_path, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)  # skip the header
            for row in reader:
                # vendor_id, passenger_count, trip_distance, payment_type, total_amount
                if dataset == "fhv":
                    rows = f'{row[0]}, {row[1]}, {row[3]}'
                else:
                    rows = f'{row[0]}, {row[1]}, {row[5]}'
                records.append(rows)
                ride_keys.append(str(row[0]))
                i += 1
                if i == 50:
                    break                
        return zip(ride_keys, records)

    def publish(self, topic: str, records: [str, str]):
        for key_value in records:
            key, value = key_value
            try:
                self.producer.send(topic=topic, key=key, value=value)
                print(f"Producing record for <key: {key}, value:{value}>")
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Exception while producing record - {value}: {e}")

        self.producer.flush()
        sleep(1)


if __name__ == "__main__":
    config = {
        'bootstrap_servers': [BOOTSTRAP_SERVERS],
        'key_serializer': lambda x: x.encode('utf-8'),
        'value_serializer': lambda x: x.encode('utf-8')
    }
    producer = RideCSVProducer(props=config)

    ride_records = producer.read_records(resource_path='../../resources/green_tripdata_2019-01.csv', dataset="green")
    print(next(ride_records))
    producer.publish(topic="rides_green", records=ride_records)

    ride_records = producer.read_records(resource_path='../../resources/fhv_tripdata_2019-01.csv', dataset="fhv")
    print(next(ride_records))
    producer.publish(topic="rides_fhv", records=ride_records)

