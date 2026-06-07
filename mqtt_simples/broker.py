import socket
import threading
from protocol import parse_message, create_message

class Broker:
    def __init__(self, host="localhost", port= 1883):
        self.host = host
        self.port = port
        self.topics = {}
        self.users = {}
        self.pending_messages = {}

    def start(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((self.host, self.port))
        server.listen()

        print(f"[BROKER] Rodando em {self.host}:{self.port}")

        while True:
            conn, addr = server.accept()
            print(f"[CONEXÃO] {addr}")

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

        if msg_type == "connect":
            username = msg["username"]

            self.users[username] = conn
            
            if username not in self.pending_messages:
                self.pending_messages[username] = []

            print(f"[LOGIN] {username}")

            if self.pending_messages[username]:
                print(
                    f"[BUFFER] Tentando entregar"
                    f"{len(self.pending_messages[username])}"
                    f"mensagens para {username}"
                )

                delivered = []

                for pending in self.pending_messages[username]:
                    try:
                        conn.send(create_message(
                            "message",
                            topic=pending["topic"],
                            message=pending["message"],
                            username=pending["username"]
                        ))
                        delivered.append(pending)
                    except:
                        break
                
                for pending in delivered:
                    self.pending_messages[username].remove(pending)
                
                print(
                    f"[BUFFER] {len(delivered)} mensagens entregues para {username}"
                )

            conn.send(create_message(
                "response",
                status="ok",
                message=f"Bem-vindo {username}"
            ))


        elif msg_type == "create_topic":
            topic = msg["topic"]
            username = msg["username"]

            if topic not in self.topics:
                self.topics[topic] = [username]

                print(f"[TOPIC] Criado: {topic} (criador inscrito automaticamente)")

                conn.send(create_message("response", status= "ok", message= "Tópico criado"))
            else:
                conn.send(create_message("response", status= "error", message= "Tópico já foi criado!")) 
                
        elif msg_type == "subscribe":
            topic = msg["topic"]
            username = msg["username"]

            if topic not in self.topics:
                conn.send(create_message("response", status= "error", message= "Tópico não existe!"))
                return
            
            if username not in self.topics[topic]:
                self.topics[topic].append(username)
            print(f"[SUBSCRIBE] {username} entrou  em {topic}")

            conn.send(create_message("response", status= "ok", message= "Inscrito com sucesso"))

        elif msg_type == "unsubscribe":
            topic = msg["topic"]
            username = msg["username"]

            if topic in self.topics:
                if username in self.topics[topic]:
                    self.topics[topic].remove(username)

                    print(f"[UNSUBSCRIBE] Cliente saiu de {topic}")

                    conn.send(create_message(
                        "response",
                        status="ok",
                        message="Saiu do tópico"
                    ))

                    if len(self.topics[topic]) == 0:
                        del self.topics[topic]
                        
                        print(f"[TOPIC REMOVIDO] {topic}")
        
        elif msg_type == "publish":
            topic = msg["topic"]
            message = msg["message"]
            username = msg["username"]

            print(f"[PUBLISH] [{topic}] {username}: {message}")

            if topic in self.topics:
                for username_dest in self.topics[topic]:
                    if username_dest in self.users:
                        client_conn = self.users[username_dest]
                        try:
                          client_conn.send(create_message(
                            "message",
                            topic=topic,
                            message=message,
                            username=username
                        ))
                        except:
                          self.pending_messages[username_dest].append({
                              "topic":topic,
                              "message":message,
                              "username":username
                          })
                    else:
                        self.pending_messages[username_dest].append({
                            "topic":topic,
                            "message":message,
                            "username":username
                        })
                        print(
                          f"[BUFFER] Mensagem de {username} "
                          f"armazenada para {username_dest}"
                        )
    
    def remove_client(self, conn):
        username_to_remove = None

        for username, user_conn in self.users.items():
            if user_conn == conn:
                username_to_remove = username
                break

        if username_to_remove:
            del self.users[username_to_remove]

            print(f"[DESCONECTADO] {username_to_remove}")
        
        conn.close()

if __name__ == "__main__":
    broker = Broker()
    broker.start()