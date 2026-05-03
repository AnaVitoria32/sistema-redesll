import socket
import threading
from protocol import parse_message, create_message

class Broker:
    def __init__(self, host="localhost", port= 1883):
        self.host = host
        self.port = port
        self.topics = {}
        self.clients = []

    def start(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((self.host, self.port))
        server.listen()

        print(f"[BROKER] Rodando em {self.host}:{self.port}")

        while True:
            conn, addr = server.accept()
            print(f"[CONEXÃO] {addr}")

            self.clients.append(conn)
            threading.Thread(target=self.handle_client, args=(conn,), daemon=True).start()

    def handle_client(self, conn):
        while True:
            try:
                data = conn.recv(1024)
                if not data:
                    break
                msg = parse_message(data)
                if msg:
                    self.process_message(conn, msg)
            except:
                break
        self.remove_client(conn)
        
    def process_message(self, conn, msg):
        msg_type = msg.get("type")

        if msg_type == "create_topic":
            topic = msg["topic"]

            if topic not in self.topics:
                self.topics[topic] = []
                print(f"[TOPIC] Criado: {topic}")

                conn.send(create_message("response", status= "ok", message= "Tópico criado"))
            else:
                conn.send(create_message("response", status= "error", message= "Tópico já foi criado!")) 
        elif msg_type == "subscribe":
            topic = msg["topic"]

            if topic not in self.topics:
                conn.send(create_message("response", status= "error", message= "Tópico não existe!"))
                return
            
            if conn not in self.topics[topic]:
                self.topics[topic].append(conn)
            print(f"[SUBSCRIBE] Cliente em {topic}")

            conn.send(create_message("response", status= "ok", message= "Inscrito com sucesso"))
        
        elif msg_type == "publish":
            topic = msg["topic"]
            message = msg["message"]

            print(f"[PUBLISH] {topic}: {message}")

            if topic in self.topics:
                for client in self.topics[topic]:
                    try:
                        client.send(create_message(
                            "message",
                            topic=topic,
                            message=message
                        ))
                    except:
                        pass
    
    def remove_client(self, conn):
        print("[DESCONECTADO] Cliente")

        for topic in self.topics:
            if conn in self.topics[topic]:
                self.topics[topic].remove(conn)
        
        if conn in self.clients:
            self.clients.remove(conn)
        conn.close()

if __name__ == "__main__":
    broker = Broker()
    broker.start()