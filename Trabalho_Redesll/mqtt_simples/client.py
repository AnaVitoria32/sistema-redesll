import socket
import threading
from protocol import create_message, parse_message

class Client:
    def __init__(self, host="localhost", port=1883):
        self.sock= socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((host, port))

    def receive(self):
        while True:
            try:
                data= self.sock.recv(1024)
                if data:
                    msg = parse_message(data)

                    if msg["type"] == "message":
                        print(f"\n[{msg['topic']}] {msg['message']}")
                    elif msg["type"] == "response":
                        print(f"\n[SERVER] {msg['message']}")
            except:
                break

    def create_topic(self, topic):
        self.sock.send(create_message("create_topic", topic=topic))

    def subscribe(self, topic):
        self.sock.send(create_message("subscribe", topic=topic))

    def publish(self, topic, message):
        self.sock.send(create_message("publish", topic=topic, message=message))

def main():
    client = Client()

    threading.Thread(target=client.receive, daemon=True).start()

    while True:
        print("\n1 - Criar tópico")
        print("2 - Inscrever")
        print("3 - Publicar")

        choice = input("Escolha: ")

        if choice == "1":
            topic = input("Nome do tópico:").strip()
            client.create_topic(topic)

        if choice == "2":
            topic = input("Tópico:").strip()
            client.subscribe(topic)

        if choice == "3":
            topic = input("Tópico:")
            msg = input("Mensagem: ").strip()
            client.publish(topic, msg)

if __name__ == "__main__":
    main()