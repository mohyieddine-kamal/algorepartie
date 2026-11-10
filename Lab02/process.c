#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <pthread.h>
#include <fcntl.h>
#include <stdbool.h>
#include <signal.h>
#include <time.h>

#define MAX_N 100
#define MAX_REQS 1000
#define PORT      9999
#define BACKLOG   MAX_N
#define BUF_SIZE  256
#define Port 10000
#define MAX_reqs 1000
#define MAX_QUEUE 1000

#define IDLE_MS 3000
#define DRAIN_MS 2000

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

typedef enum { PROPOSE, ACK, COMMIT, ECHO, VOTE } req_type;

typedef struct {
    int msg;
    int process_number;
} cmd_t;

typedef struct {
    req_type type;
    int msg;
} message_t;

int NB_process;
size_t nb_commandes = 0;
int my_id;
int sock[MAX_N];

// Buffers pour le thread réseau
char recv_buf[MAX_N][sizeof(message_t)];
int  recv_offset[MAX_N];

// Files d'attente des messages complets
message_t msg_queue[MAX_N][MAX_QUEUE];
int q_head[MAX_N] = {0};
int q_tail[MAX_N] = {0};

pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t send_lock = PTHREAD_MUTEX_INITIALIZER;
volatile bool i_am_done = false;

bool peer_closed[MAX_N] = {false};

// Fonction pour obtenir le temps actuel en millisecondes
long long now_ms(void) {
    struct timespec ts; 
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000LL + ts.tv_nsec / 1000000;
}

// pair fermé ET plus aucun message non lu de sa part
bool peer_dead(int p) {
    pthread_mutex_lock(&lock);
    bool d = peer_closed[p] && (q_head[p] == q_tail[p]);
    pthread_mutex_unlock(&lock);
    return d;
}

bool all_peers_dead(int n) {
    for (int p = 0; p < n; p++) {
        if (p != my_id && !peer_dead(p)) return false;
    }
    return true;
}
// true quand on a reçu l'EOF (ou un reset) de tous les pairs connectés
bool all_peers_closed(int n) {
    pthread_mutex_lock(&lock);
    bool all = true;
    for (int p = 0; p < n; p++) {
        if (p != my_id && sock[p] >= 0 && !peer_closed[p]) { all = false; break; }
    }
    pthread_mutex_unlock(&lock);
    return all;
}

cmd_t* parse_file(const char *path) {
    FILE *fp = fopen(path, "r");
    if (!fp) {
        perror("fopen");
        exit(EXIT_FAILURE);
    }
    cmd_t* commandes = malloc(MAX_reqs * sizeof(cmd_t));
    if (!commandes) {
        perror("malloc");
        fclose(fp);
        exit(EXIT_FAILURE);
    }
    char *line = NULL;
    size_t len = 0;
    ssize_t read_bytes;
    bool is_first_line = true;
    int i = 0;

    while ((read_bytes = getline(&line, &len, fp)) != -1) {
        while (read_bytes > 0 && (line[read_bytes - 1] == '\n' || line[read_bytes - 1] == '\r')) {
            line[--read_bytes] = '\0';
        }

        char *token = strtok(line, " \t");
        if (token == NULL || token[0] == '#') continue;

        if (is_first_line) {
            is_first_line = false;
            NB_process = atoi(token);
            continue;
        }

        char* token1 = strtok(NULL, " \t");
        if (token1 == NULL) exit(EXIT_FAILURE);
        
        char* token2 = strtok(NULL, " \t");
        if (token2 == NULL) exit(EXIT_FAILURE);

        if (strcmp(token, "BCAST") == 0) {
            commandes[i].msg = atoi(token1);
            commandes[i].process_number = atoi(token2);
            i++;  
            nb_commandes++;          
        }
    }
    free(line);
    fclose(fp);
    return commandes;
}

static void die(const char *msg) {
    perror(msg);
    exit(EXIT_FAILURE);
}

static int send_all(int fd, const void *buf, size_t size) {
    const char *data = buf;
    size_t sent = 0;
    while (sent < size) {
        ssize_t n = send(fd, data + sent, size - sent, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) {
            errno = EPIPE;
            return -1;
        }
        sent += (size_t)n;
    }
    return 0;
}

int send_message(int fd, req_type type, int msg) {
    message_t message = { .type = type, .msg = msg };
    pthread_mutex_lock(&send_lock);
    int ret = send_all(fd, &message, sizeof(message));
    pthread_mutex_unlock(&send_lock);
    return ret;
}

void queue_push(int p, message_t m) {
    msg_queue[p][q_tail[p]] = m;
    q_tail[p] = (q_tail[p] + 1) % MAX_QUEUE;
}

// Fonction non bloquante pour récupérer un message traité
int fetch_message(int p, message_t *m) {
    pthread_mutex_lock(&lock);
    if (q_head[p] != q_tail[p]) {
        *m = msg_queue[p][q_head[p]];
        q_head[p] = (q_head[p] + 1) % MAX_QUEUE;
        pthread_mutex_unlock(&lock);
        return 1;
    }
    pthread_mutex_unlock(&lock);
    return 0;
}

void setup_network(int my_id, int n) {
    for (int i = 0; i < MAX_N; i++) {
        sock[i] = -1;
        recv_offset[i] = 0;
    }

    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) die("socket");

    int yes = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(Port + my_id);

    if (bind(listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) die("bind");
    if (listen(listen_fd, BACKLOG) < 0) die("listen");

    for (int j = 0; j < my_id; j++) {
        int conn_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (conn_fd < 0) die("socket");
        struct sockaddr_in server_addr;
        memset(&server_addr, 0, sizeof(server_addr));
        server_addr.sin_family = AF_INET;
        server_addr.sin_port   = htons(Port + j);
        if (inet_pton(AF_INET, "127.0.0.1", &server_addr.sin_addr) <= 0) die("inet_pton");

        while (connect(conn_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
            if (errno == ECONNREFUSED || errno == ENOENT || errno == EINTR) {
                usleep(10000);
                continue;
            }
            die("connect");
        }
        if (send_all(conn_fd, &my_id, sizeof(my_id)) < 0) die("send my_id");
        sock[j] = conn_fd;
    }

    for (int k = 0; k < n - 1 - my_id; k++) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        
        int conn_fd = accept(listen_fd, (struct sockaddr *)&client_addr, &client_len);
        if (conn_fd < 0) {
            if (errno == EINTR) { k--; continue; }
            die("accept");
        }

        int client_id = -1;
        size_t received = 0;
        while (received < sizeof(client_id)) {
            ssize_t bytes = recv(conn_fd, (char *)&client_id + received, sizeof(client_id) - received, 0);
            if (bytes <= 0) {
                if (errno == EINTR) continue;
                die("recv client_id");
            }
            received += (size_t)bytes;
        }
        sock[client_id] = conn_fd;
    }
    close(listen_fd);
}

// Thread dédié à la lecture asynchrone pour gérer la fragmentation
void* receiver_thread_func(void* arg) {
    (void)arg;
    while (1) {
        pthread_mutex_lock(&lock);
        bool should_exit = i_am_done;
        pthread_mutex_unlock(&lock);
        if (should_exit) break;

        bool activity = false;
        for (int j = 0; j < NB_process; j++) {
            if (j == my_id || sock[j] < 0) continue;
            
            ssize_t n = recv(sock[j], recv_buf[j] + recv_offset[j], sizeof(message_t) - recv_offset[j], MSG_DONTWAIT);
            if (n > 0) {
                activity = true;
                recv_offset[j] += (int)n;
                if (recv_offset[j] >= (int)sizeof(message_t)) {
                    message_t req;
                    memcpy(&req, recv_buf[j], sizeof(message_t));
                    recv_offset[j] = 0;

                    pthread_mutex_lock(&lock);
                    queue_push(j, req);
                    pthread_mutex_unlock(&lock);
                }
            } else if (n == 0 || (n < 0 && errno == ECONNRESET)) {
                close(sock[j]);
                sock[j] = -1;
                recv_offset[j] = 0;
                
                pthread_mutex_lock(&lock);
                peer_closed[j] = true;
                pthread_mutex_unlock(&lock);
            }
        }
        if (!activity) usleep(5000);
    }
    return NULL;
}

static void deliver_message(int id, int msg) {
    printf("[Process %d] DELIVERED %d\n", id, msg);
    fflush(stdout);
}

int raft_broadcast(int my_id, int n, const cmd_t *cmds, int nb) {
    int decision = n / 2 + 1;
    message_t m;

    if (my_id == 0) {
        int target[MAX_N], ack[MAX_reqs] = {0};
        bool sent[MAX_N] = {false};
        for (int p = 0; p < n; p++) target[p] = -1;
        
        int start = 0;
        for (int c = 0; c < nb; c++) {
            for (int p = start; p < start + cmds[c].process_number && p < n; p++) target[p] = c;
            start += cmds[c].process_number;
        }

        if (target[0] >= 0) ack[target[0]]++;
        
        for (int p = 1; p < n; p++) {
            if (target[p] >= 0) {
                sent[p] = (send_message(sock[p], PROPOSE, cmds[target[p]].msg) == 0);
            }
        }
        // on attend les ACKs
        int winner = -1;
        for (int c = 0; c < nb; c++) {
            if (ack[c] >= decision) winner = c; // on a recu la majorité des ACKs pour cette commande
        }

        long long last = now_ms();
        while (winner < 0) {
            bool activity = false;
            int pending = 0;
            
            for (int p = 1; p < n; p++) {
                if (!sent[p]) continue;
                if (!peer_dead(p)) pending++; // on attend encore un ACK de ce pair
                
                while (fetch_message(p, &m) == 1) {
                    activity = true;
                    if (m.type == ACK) {
                        int c = target[p];
                        if (c >= 0 && m.msg == cmds[c].msg) {
                            ack[c]++;
                            if (ack[c] >= decision) winner = c;
                        }
                    }
                }
            }
            if (winner >= 0) break;
            
            if (activity) last = now_ms();
            if (pending == 0 || now_ms() - last > IDLE_MS) return -1; // plus aucun ACK attendu ou timeout
            usleep(2000);
        }

        // on livre le message
        deliver_message(my_id, cmds[winner].msg);
        for (int p = 1; p < n; p++) send_message(sock[p], COMMIT, cmds[winner].msg);
        return 0;
    } 
    
    // Suiveurs
    bool committed = false;
    long long last = now_ms();
    
    while (!committed) {
        bool activity = false;
        while (fetch_message(0, &m) == 1) {
            activity = true;
            if (m.type == PROPOSE) {
                send_message(sock[0], ACK, m.msg);
            } else if (m.type == COMMIT) {
                deliver_message(my_id, m.msg);
                committed = true;
            }
        }
        
        if (activity) last = now_ms();
        if (peer_dead(0) || now_ms() - last > IDLE_MS) return -1; // peer mort ou timeout
        usleep(2000);        
    }
    return 0;
}

int Bracha_broadcast(int my_id, int n, const cmd_t *cmds, int nb) {
    int f = (n - 1) / 3;

    int echo_decision   = n-f;   // nombre d'échos pour voter
    int deliver_decision = n-f;         // nombre de votes pour livrer
    int my_value = -1;
    message_t m;

    if (my_id == 0) {
        int target[MAX_N];
        for (int p = 0; p < n; p++) target[p] = -1;
        int start = 0;
        for (int c = 0; c < nb; c++) {
            for (int p = start; p < start + cmds[c].process_number && p < n; p++) target[p] = c;
            start += cmds[c].process_number;
        }
        if (target[0] >= 0) my_value = cmds[target[0]].msg;

        for (int p = 1; p < n; p++) {
            int v = (target[p] >= 0) ? cmds[target[p]].msg : -1;
            send_message(sock[p], PROPOSE, v);
        }
    } else {
        long long last_propose = now_ms();
        while (my_value == -1) {
            if (fetch_message(0, &m) == 1 && m.type == PROPOSE) {
                my_value = m.msg;
            }
            if (peer_dead(0) || now_ms() - last_propose > IDLE_MS) return -1;
            usleep(2000);
        }
    }

    bool echo_seen[MAX_N] = {false}; // indique si on a déjà reçu un ECHO de chaque pair
    int echo_values[MAX_N]; // stocke la valeur reçue de chaque pair (-1 si pas encore reçu)
    for (int p = 0; p < n; p++) echo_values[p] = -1;
    bool vote_seen[MAX_N] = {false};
    for (int p = 0; p < n; p++) {
        if (p != my_id) send_message(sock[p], ECHO, my_value);
    }

    if (my_value != -1) {
        echo_seen[my_id] = true;
        echo_values[my_id] = my_value;
    }
    // indique si on a déjà reçu un VOTE de chaque pair
    int vote_vals[MAX_N];
    int nv = 0;
    int my_vote = -1;
    bool voted = false, amplified = false; // amplified = true si on a voté après avoir reçu f+1 votes identiques

    long long last = now_ms();
    while (1) {
        bool activity = false;
        
        // Collecte active et non-bloquante de tous les messages (ECHO ou VOTE)
        for (int p = 0; p < n; p++) {
            if (p == my_id) continue;
            while (fetch_message(p, &m) == 1) {
                activity = true;
                if (m.type == ECHO) {
                    if (!echo_seen[p]) {
                        echo_seen[p] = true;
                        echo_values[p] = m.msg;
                    }
                } else if (m.type == VOTE && !vote_seen[p]) {
                    vote_seen[p] = true;
                    if (m.msg != -1 && nv < MAX_N) {
                        vote_vals[nv++] = m.msg;
                    }
                }
            }
        }
        // on cherche un candidat à voter si on a reçu echo_decision échos identiques
        int echo_candidate = -1;
        for (int i = 0; i < n && echo_candidate == -1; i++) {
            if (!echo_seen[i] || echo_values[i] == -1) continue;

            int count = 0;
            for (int j = 0; j < n; j++) {
                if (echo_seen[j] && echo_values[j] == echo_values[i]) {
                    count++;
                }
            }
            if (count >= echo_decision) {
                echo_candidate = echo_values[i];
            }
        }
        
        // si on n'a pas encore voté , on vote si echo_nb >= n-f ou vote_nb >= f+1
        // 1er cas
        if (!voted && echo_candidate != -1) {
            my_vote = echo_candidate;
            voted = true;
            vote_seen[my_id] = true;
            if (nv < MAX_N) vote_vals[nv++] = my_vote;
            for (int p = 0; p < n; p++) {
                if (p != my_id) send_message(sock[p], VOTE, my_vote);
            }
        }
        
        // 2ème cas
        if (!voted && !amplified) {
            for (int i = 0; i < nv; i++) {
                int cnt = 0;
                for (int j = 0; j < nv; j++) if (vote_vals[j] == vote_vals[i]) cnt++;
                if (cnt >= f + 1) {
                    my_vote = vote_vals[i];
                    amplified = true;
                    voted = true;
                    vote_seen[my_id] = true;
                    if (nv < MAX_N) vote_vals[nv++] = my_vote;
                    for (int p = 0; p < n; p++) {
                        if (p != my_id) send_message(sock[p], VOTE, my_vote);
                    }
                    break;
                }
            }
        }
        
        // si vote_nb >= n-f on délivre
        for (int i = 0; i < nv; i++) {
            int count = 0;
            for (int j = 0; j < nv; j++) if (vote_vals[j] == vote_vals[i]) count++;
            if (count >= deliver_decision) {
                deliver_message(my_id, vote_vals[i]);
                return 0;
            }
        }
        
        if (activity) last = now_ms();
        // si tous les pairs sont morts ou si on a pas reçu de message depuis un certain temps
        if (all_peers_dead(n) || now_ms() - last > IDLE_MS) return -1;
        usleep(2000);
    }
    return -1;
}

int main(int argc, char **argv) {
    signal(SIGPIPE, SIG_IGN);
    const char *protocol = getenv("PROTOCOL");
    
    if (argc != 3 || !protocol) {
        fprintf(stderr, "Usage: PROTOCOL=... %s <id> <fichier>\n", argv[0]);
        return EXIT_FAILURE;
    }

    my_id = atoi(argv[1]);
    cmd_t *commandes = parse_file(argv[2]);
    setup_network(my_id, NB_process);

    pthread_t receiver_thread;
    if (pthread_create(&receiver_thread, NULL, receiver_thread_func, NULL) != 0) {
        die("pthread_create");
    }

    int ret = 0;
    if (strcmp(protocol, "RAFT") == 0) {
        ret = raft_broadcast(my_id, NB_process, commandes, (int)nb_commandes);
    } else if (strcmp(protocol, "BRACHA") == 0) {
        ret = Bracha_broadcast(my_id, NB_process, commandes, (int)nb_commandes);
    }

    for (int i = 0; i < NB_process; i++) {
        if (sock[i] >= 0) shutdown(sock[i], SHUT_WR);
    }
    long long t0 = now_ms();
    while (!all_peers_closed(NB_process) && now_ms() - t0 < DRAIN_MS) {
        usleep(2000);
    }

    // Terminaison propre du thread
    pthread_mutex_lock(&lock);
    i_am_done = true;
    pthread_mutex_unlock(&lock);
    pthread_join(receiver_thread, NULL);

    for (int i = 0; i < NB_process; i++) {
        if (sock[i] >= 0) close(sock[i]);
    }
    free(commandes);
    
    return (ret < 0) ? EXIT_FAILURE : EXIT_SUCCESS;
}