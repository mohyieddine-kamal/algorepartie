#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <pthread.h>
#include <poll.h>
#include <fcntl.h>
#include <stdbool.h>
#include <signal.h>

#define BACKLOG   128
#define BUF_SIZE  256
#define MAX_N 100
#define Port 10000

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

static void die(const char *msg) {
    perror(msg);
    exit(EXIT_FAILURE);
}

static int send_all(int fd, const void *buf, size_t len) {
    const char *p = (const char *)buf;
    size_t total = 0;
    while (total < len) {
        ssize_t n = send(fd, p + total, len - total, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) return -1;
        total += (size_t)n;
    }
    return 0;
}

static int recv_all(int fd, void *buf, size_t len) {
    char *p = (char *)buf;
    size_t total = 0;
    while (total < len) {
        ssize_t n = recv(fd, p + total, len - total, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) return -1; // Connection closed before reading len bytes
        total += (size_t)n;
    }
    return 0;
}

static void usage(const char *prog) {
    fprintf(stderr, "Usage: %s <id> <file>\n", prog);
}

typedef enum { CMD_LOCK, CMD_WAIT } cmd_type;

typedef struct {
    cmd_type type;
    int arg; // durée X si Lock, id j si Wait         
} cmd_t;

typedef struct {
    int n;              // N : nombre de processus
    cmd_t *cmds;        // mes commandes, dans l'ordre
    int n_cmds;         // combien j'en ai
} config_t;

typedef enum {
    REQ,
    ACK,
    REL,
    DONE
} msg_type;

typedef struct {
    msg_type type;
    int LCi; // timestamp du msg
    int id;  // id du processus
    int req_id; // id/numéro de la requête correspondante
} req_t;

// linked list of requests
typedef struct req_node {
    req_t req;
    struct req_node *next;
} req_node_t;

req_node_t *head = NULL; // head of the linked list
int my_LC = 0; // Lamport clock
int processNb = 0;
int received_rel[MAX_N]; // Array to track received REL messages for each process

void update_LC(int received_LC) {
    my_LC = (my_LC > received_LC ? my_LC : received_LC) + 1;
}

config_t parse_file(const char *path, int my_id) {
    FILE *fp = fopen(path, "r");
    if (!fp) {
        perror("fopen");
        exit(EXIT_FAILURE);
    }

    config_t config = {0, NULL, 0};

    char *line = NULL;
    size_t len = 0;
    ssize_t read_bytes;
    bool is_first_line = true;

    while ((read_bytes = getline(&line, &len, fp)) != -1) {
        while (read_bytes > 0 && (line[read_bytes - 1] == '\n' || line[read_bytes - 1] == '\r')) {
            line[--read_bytes] = '\0';
        }

        char *token = strtok(line, " \t");
        if (token == NULL || token[0] == '#') {
            continue;
        }

        if (is_first_line) {
            is_first_line = false;
            processNb = atoi(token);
            config.n = processNb;
            continue;
        }

        int id = atoi(token);

        token = strtok(NULL, " \t");
        if (token == NULL) {
            fprintf(stderr, "Commande invalide (attendait 'Lock' ou 'Wait')\n");
            exit(EXIT_FAILURE);
        }

        if (strcmp(token, "Lock") == 0) {
            token = strtok(NULL, " \t");
            if (token == NULL) {
                fprintf(stderr, "Expected duration after 'Lock', got nothing\n");
                exit(EXIT_FAILURE);
            }

            if (id == my_id) {
                cmd_t cmd = { .type = CMD_LOCK, .arg = atoi(token) };
                config.cmds = realloc(config.cmds, sizeof(cmd_t) * (config.n_cmds + 1));
                if (!config.cmds) { perror("realloc"); exit(EXIT_FAILURE); }
                config.cmds[config.n_cmds++] = cmd;
            }
        } else if (strcmp(token, "Wait") == 0) {
            token = strtok(NULL, " \t");
            if (token == NULL) {
                fprintf(stderr, "Expected process ID after 'Wait', got nothing\n");
                exit(EXIT_FAILURE);
            }

            if (id == my_id) {
                cmd_t cmd = { .type = CMD_WAIT, .arg = atoi(token) };
                config.cmds = realloc(config.cmds, sizeof(cmd_t) * (config.n_cmds + 1));
                if (!config.cmds) { perror("realloc"); exit(EXIT_FAILURE); }
                config.cmds[config.n_cmds++] = cmd;
            }
        } else {
            fprintf(stderr, "Expected 'Lock' or 'Wait', got '%s'\n", token);
            exit(EXIT_FAILURE);
        }
    }

    free(line);
    fclose(fp);
    return config;
}

int sock[MAX_N];   // sock[j] = socket vers le processus j
pthread_cond_t cond  = PTHREAD_COND_INITIALIZER;
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t send_lock = PTHREAD_MUTEX_INITIALIZER;

int my_req_counter = 0;
int current_active_req_id = -1;
int current_active_req_lc = 0;
bool acked[MAX_N];

int my_id;
volatile bool i_am_done = 0;
volatile int done_peers = 0;

char recv_buf[MAX_N][sizeof(req_t)];
int  recv_offset[MAX_N];

static bool has_all_acks(int n) {
    for (int j = 0; j < n; j++) {
        if (!acked[j]) return false;
    }
    return true;
}

void setup_network(int my_id, int n) {
    for (int i = 0; i < MAX_N; i++) {
        sock[i] = -1;
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

    if (bind(listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0)
        die("bind");

    if (listen(listen_fd, BACKLOG) < 0)
        die("listen");

    // Connect to processes with id < my_id
    for (int j = 0; j < my_id; j++) {
        int client_sock = socket(AF_INET, SOCK_STREAM, 0);
        if (client_sock < 0) die("socket");

        struct sockaddr_in server_addr;
        memset(&server_addr, 0, sizeof(server_addr));
        server_addr.sin_family = AF_INET;
        server_addr.sin_port   = htons(Port + j);
        if (inet_pton(AF_INET, "127.0.0.1", &server_addr.sin_addr) <= 0)
            die("inet_pton");

        while (connect(client_sock, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
            if (errno == ECONNREFUSED || errno == ENOENT || errno == EINTR) {
                usleep(1000);
                continue;
            }
            perror("connect");
            exit(EXIT_FAILURE);
        }

        if (send_all(client_sock, &my_id, sizeof(my_id)) < 0)
            die("send my_id");

        sock[j] = client_sock;
    }

    // Accept connections from processes with id > my_id
    for (int k = 0; k < n - 1 - my_id; k++) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        
        int conn_fd = accept(listen_fd, (struct sockaddr *)&client_addr, &client_len);
        if (conn_fd < 0) {
            if (errno == EINTR) {
                k--;
                continue;
            }
            die("accept");
        }

        int client_id = -1;
        if (recv_all(conn_fd, &client_id, sizeof(client_id)) < 0) {
            close(conn_fd);
            die("recv client_id");
        }

        if (client_id < 0 || client_id >= n || client_id == my_id) {
            close(conn_fd);
            die("invalid client_id");
        }

        sock[client_id] = conn_fd;
    }
    processNb = n;
}

void insert_request_sorted(req_node_t **head_ref, req_node_t *new_node) {
    if (*head_ref == NULL || new_node->req.LCi < (*head_ref)->req.LCi ||
        (new_node->req.LCi == (*head_ref)->req.LCi && new_node->req.id < (*head_ref)->req.id)) {
        new_node->next = *head_ref;
        *head_ref = new_node;
    } else {
        req_node_t *current = *head_ref;
        while (current->next != NULL &&
               (current->next->req.LCi < new_node->req.LCi ||
                (current->next->req.LCi == new_node->req.LCi && current->next->req.id < new_node->req.id))) {
            current = current->next;
        }
        new_node->next = current->next;
        current->next = new_node;
    }
}

void traiter_lock(cmd_t test, int i) {
    req_t req;
    req.type = REQ;
    req.id = i;

    pthread_mutex_lock(&lock);
    my_LC++;
    req.LCi = my_LC;
    current_active_req_id = my_req_counter++;
    req.req_id = current_active_req_id;
    current_active_req_lc = req.LCi;

    for (int j = 0; j < processNb; j++) {
        acked[j] = false;
    }
    acked[i] = true; // self-ack

    req_node_t *new_node = malloc(sizeof(req_node_t));
    new_node->req = req;
    new_node->next = NULL;
    insert_request_sorted(&head, new_node);
    pthread_mutex_unlock(&lock);

    // Broadcast REQ to all other processes
    for (int j = 0; j < processNb; j++) {
        if (j != i && sock[j] >= 0) {
            pthread_mutex_lock(&send_lock);
            if (send_all(sock[j], &req, sizeof(req)) < 0)
                die("send req");
            pthread_mutex_unlock(&send_lock);
        }
    }

    // Wait for ACK from all other processes with LC >= req.LCi and to be head of queue
    pthread_mutex_lock(&lock);
    while (!(head != NULL && head->req.id == i && head->req.LCi == current_active_req_lc && has_all_acks(processNb))) {
        pthread_cond_wait(&cond, &lock);
    }
    pthread_mutex_unlock(&lock);

    // Execute critical section
    char cmd[64];
    snprintf(cmd, sizeof(cmd), "./critical %d %d", i, test.arg);
    system(cmd);

    // Remove own node from queue and prepare release
    req_t release;
    release.type = REL;
    release.id = i;
    release.req_id = current_active_req_id;

    pthread_mutex_lock(&lock);
    req_node_t **curr = &head;
    while (*curr != NULL && !((*curr)->req.id == i && (*curr)->req.LCi == current_active_req_lc)) {
        curr = &(*curr)->next;
    }
    if (*curr != NULL) {
        req_node_t *old = *curr;
        *curr = (*curr)->next;
        free(old);
    }
    current_active_req_id = -1;

    my_LC++;
    release.LCi = my_LC;
    pthread_mutex_unlock(&lock);

    // Send release to all other processes
    for (int j = 0; j < processNb; j++) {
        if (j != i && sock[j] >= 0) {
            pthread_mutex_lock(&send_lock);
            if (send_all(sock[j], &release, sizeof(release)) < 0)
                die("send release");
            pthread_mutex_unlock(&send_lock);
        }
    }
}

void traiter_wait(cmd_t test) {
    int j = test.arg;
    pthread_mutex_lock(&lock);
    while (received_rel[j] <= 0) {
        pthread_cond_wait(&cond, &lock);
    }
    received_rel[j]--;
    pthread_mutex_unlock(&lock);
}

void* receiver_thread_func(void* arg) {
    (void)arg;
    // loop over sockets with non-blocking recv.
    while (1) {
        // Check for termination condition.
        pthread_mutex_lock(&lock);
        bool should_exit = (done_peers >= processNb - 1 && i_am_done);
        pthread_mutex_unlock(&lock);
        if (should_exit) {
            break;
        }

        bool activity = false;
        // Iterate over all peer sockets.
        for (int j = 0; j < processNb; j++) {
            if (j == my_id || sock[j] < 0) continue;
            ssize_t n = recv(sock[j], recv_buf[j] + recv_offset[j],
                            sizeof(req_t) - recv_offset[j], MSG_DONTWAIT);
            if (n < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
                    continue; // No data now.
                }
                if (errno == ECONNRESET) {
                    close(sock[j]);
                    sock[j] = -1;
                    recv_offset[j] = 0;
                    continue;
                }
                die("recv");
            } else if (n == 0) {
                close(sock[j]);
                sock[j] = -1;
                recv_offset[j] = 0;
                continue;
            }

            // Data received.
            activity = true;
            recv_offset[j] += n;
            if (recv_offset[j] < (int)sizeof(req_t)) {
                // Incomplete message, continue to next socket.
                continue;
            }

            // Full message ready.
            req_t req;
            memcpy(&req, recv_buf[j], sizeof(req_t));
            recv_offset[j] = 0;

            if (req.type == ACK) {
                pthread_mutex_lock(&lock);
                update_LC(req.LCi);
                if (req.req_id == current_active_req_id && req.LCi >= current_active_req_lc &&
                    req.id >= 0 && req.id < processNb) {
                    acked[req.id] = true;
                }
                pthread_cond_broadcast(&cond);
                pthread_mutex_unlock(&lock);
                continue;
            }

            if (req.type == DONE) {
                pthread_mutex_lock(&lock);
                update_LC(req.LCi);
                done_peers++;
                pthread_cond_broadcast(&cond);
                pthread_mutex_unlock(&lock);
                continue;
            }

            if (req.type == REL) {
                pthread_mutex_lock(&lock);
                update_LC(req.LCi);
                // Remove request from queue if present.
                req_node_t **curr = &head;
                while (*curr != NULL && (*curr)->req.id != req.id) {
                    curr = &(*curr)->next;
                }
                if (*curr != NULL) {
                    req_node_t *to_free = *curr;
                    *curr = (*curr)->next;
                    free(to_free);
                }
                received_rel[req.id]++;
                pthread_cond_broadcast(&cond);
                pthread_mutex_unlock(&lock);
                continue;
            }

            if (req.type == REQ) {
                // Prepare ACK.
                req_node_t *new_node = malloc(sizeof(req_node_t));
                new_node->req = req;
                new_node->next = NULL;

                req_t ack;
                ack.type = ACK;
                ack.id = my_id;
                ack.req_id = req.req_id;

                pthread_mutex_lock(&lock);
                update_LC(req.LCi);
                insert_request_sorted(&head, new_node);
                ack.LCi = my_LC;
                pthread_cond_broadcast(&cond);
                pthread_mutex_unlock(&lock);

                pthread_mutex_lock(&send_lock);
                if (sock[req.id] >= 0 && send_all(sock[req.id], &ack, sizeof(ack)) < 0)
                    die("send ack");
                pthread_mutex_unlock(&send_lock);
            }
        }
        if (!activity) {
            // No socket activity; sleep briefly to avoid CPU spin.
            usleep(5000);
        }
    }
    return NULL;

    
}

int main(int argc, char **argv) {
    signal(SIGPIPE, SIG_IGN);

    if (argc != 3) {
        usage(argv[0]);
        return EXIT_FAILURE;
    }

    config_t config = parse_file(argv[2], atoi(argv[1]));
    pthread_mutex_lock(&lock);
    for (int i = 0; i < MAX_N; i++) {
        received_rel[i] = 0;
        recv_offset[i] = 0;
        acked[i] = false;
    }
    pthread_mutex_unlock(&lock);

    my_id = atoi(argv[1]);
    setup_network(my_id, config.n);

    pthread_t receiver_thread;
    if (pthread_create(&receiver_thread, NULL, receiver_thread_func, NULL) != 0) {
        perror("pthread_create");
        exit(EXIT_FAILURE);
    }

    for (int i = 0; i < config.n_cmds; i++) {
        cmd_t test = config.cmds[i];
        if (test.type == CMD_LOCK) {
            traiter_lock(test, my_id);
        } else if (test.type == CMD_WAIT) {
            traiter_wait(test);
        }
    }

    pthread_mutex_lock(&lock);
    i_am_done = 1;
    my_LC++;
    req_t done;
    done.type = DONE;
    done.LCi = my_LC;
    done.id = my_id;
    done.req_id = -1;
    pthread_mutex_unlock(&lock);

    for (int i = 0; i < config.n; i++) {
        if (i != my_id && sock[i] >= 0) {
            pthread_mutex_lock(&send_lock);
            if (send_all(sock[i], &done, sizeof(done)) < 0)
                die("send done");
            pthread_mutex_unlock(&send_lock);
        }
    }

    pthread_join(receiver_thread, NULL);

    return EXIT_SUCCESS;
}
