import socket
import threading
from protocol import create_message, parse_message

class Client:
    def __init__(self, username, host="localhost", port=1883):
        self.username = username
        self.sock= socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((host, port))

        self.sock.send(create_message(
            "connect",
            username=self.username
        ))

    def receive(self):
        while True:
            try:
                data= self.sock.recv(1024)
                if not data:
                    break

                msg = parse_message(data)

                if not msg:
                    continue

                if msg["type"] == "message":
                    print(
                        f"\n[{msg['topic']}]"
                        f"{msg['username']}: "
                        f"{msg['message']}"
                    )
                elif msg["type"] == "response":
                    print(
                        f"\n[SERVER] {msg['message']}"
                    )

            except Exception as e:
                print(f"[ERRO CLIENTE] {e}")
                

    def create_topic(self, topic):
        self.sock.send(create_message("create_topic", topic=topic, username=self.username))

    def subscribe(self, topic):
        self.sock.send(create_message("subscribe", topic=topic, username=self.username))

    def unsubscribe(self, topic):
        self.sock.send(create_message("unsubscribe", topic=topic, username=self.username))

    def publish(self, topic, message):
        self.sock.send(create_message(
            "publish", 
            topic=topic, 
            message=message,
            username=self.username
            ))

def main():
    username = input("Seu nome: ").strip()
    client = Client(username)

    threading.Thread(target=client.receive, daemon=True).start()

    while True:
        print("\n1 - Criar tópico")
        print("2 - Inscrever")
        print("3 - Publicar")
        print("4 - Sair do tópico")

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

        if choice == "4":
            topic = input("Tópico: ").strip()
            client.unsubscribe(topic)

if __name__ == "__main__":
    main()