import argparse
from datetime import datetime
from gcn_kafka import Consumer
import email
import matplotlib.pyplot as plt
import xml.etree.ElementTree as ET
import sys

# Simple connection as consumer
def connect_as_consumer():
    global consumer 
    consumer = Consumer(client_id="fill-me-in", 
                        client_secret="fill-me-in")

# Connect and recieve oldest alerts
def connect_and_recieve_old_alerts():
    global consumer 
    consumer = Consumer(client_id="fill-me-in", 
                        client_secret="fill-me-in",
                        domain="gcn.nasa.gov",
                        **{'auto.offset.reset':'earliest'})

def connect_with_consumer_args(client_id, client_secret):
    global consumer
    consumer = Consumer(client_id=client_id, 
                    client_secret=client_secret)


def subscribe_to_topics():
    consumer.subscribe([
                    'gcn.classic.voevent.SWIFT_BAT_GRB_POS_ACK',
                    'gcn.classic.voevent.FERMI_GBM_FIN_POS'
                    # 'gcn.classic.text.SWIFT_BAT_GRB_POS_ACK',
                    # 'gcn.classic.text.FERMI_GBM_FIN_POS'
                ])

def recieve_alerts():
    try:
        while True:
            for message in consumer.consume():
                print(message.value())
                save_voevent_alert(message.value())

    except KeyboardInterrupt:
        print('Interrupted')

def parse_text_alert_to_dict(alert):
    return email.message_from_bytes(alert)

def parse_voevent_alert_to_xml_root(alert):
    return ET.fromstring(alert)

# Plot w/ astropy ra/dec/error
def get_coordinates_from_parsed_text_alert(parsed_message):
    raw_ra = parsed_message['GRB_RA'].split(",")[0]
    raw_dec = parsed_message['GRB_DEC'].split(",")[0]
    return {
        'ra': float(raw_ra[0:raw_ra.index('d')]),
        'dec' : float(raw_dec[0:raw_dec.index('d')]),
        'radius' :parsed_message['GRB_ERROR']
    }

def get_coordinates_from_parsed_voevent_alert(parsed_message):
    pos2d = parsed_message.find('.//{*}Position2D')
    ra = float(pos2d.find('.//{*}C1').text)
    dec = float(pos2d.find('.//{*}C2').text)
    radius = float(pos2d.find('.//{*}Error2Radius').text)
    print('ra = {:g}, dec={:g}, radius={:g}'.format(ra, dec, radius))
    return {
        'ra':ra,
        'dec':dec,
        'radius':radius
    }

def plot_map(coordinateObj):
    plt.figure()
    plt.subplot(111, projection="aitoff")
    plt.title("Aitoff")
    plt.grid(True)
    plt.scatter(coordinateObj['ra'], coordinateObj['dec'])
    plt.show()

def save_text_alert(alert_data):
    with open("alerts/alert_log_{0}.txt".format(datetime.utcnow()), "wb") as binary_file:
        binary_file.write(alert_data)

def save_voevent_alert(alert_data):
    with open("alerts/alert_log_{0}.xml".format(datetime.utcnow()), "wb") as xml_file:
        xml_file.write(alert_data)

def parse_args():
    parser=argparse.ArgumentParser(description="A simple script with some example functionallity \
    for handling gcn alerts")
    parser.add_argument("client_id", help="Your client Id used to connect to the kafka client")
    parser.add_argument("client_secret", help="Your client secret used to connect to the kafka client")
    parser.add_argument("alert_type", help="Alert type to recieve [ text | voevent | binary ]")
    args=parser.parse_args()
    return args

def main():
    inputs = parse_args()
    print("Inputs: ", inputs)
    connect_with_consumer_args(inputs.client_id, inputs.client_secret)
    # connect_and_recieve_old_alerts()
    # #connect_as_consumer()
    subscribe_to_topics()
    recieve_alerts()

if __name__ == "__main__":
    main()