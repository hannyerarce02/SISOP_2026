#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <sys/wait.h>
#include <semaphore.h>

#define FIFO_FILE "canal_fifo"
#define SHM_KEY 1234
#define BUFFER_SIZE 128
#define NUM_DATOS 5

// Estructura modificada según la sección 5.3 de los hints
typedef struct {
    int a;
    int b;
    int resultado_mul;
    int resultado_sum;
    int listo;
    int fin;          /* 1 = no habrá más pares (Mitigación P8) */
    sem_t vacio;      /* 1 = el buffer está libre */
    sem_t lleno;      /* 1 = hay un par disponible */
} DatosCompartidos;

int main() {
    int shm_id;
    DatosCompartidos *mem;
    pid_t pid_prod, pid_cons1, pid_cons2;

    // Crear y asociar memoria compartida
    shm_id = shmget(SHM_KEY, sizeof(DatosCompartidos), IPC_CREAT | 0666);
    if (shm_id < 0) {
        perror("Error creando memoria compartida");
        exit(1);
    }
    
    mem = (DatosCompartidos *) shmat(shm_id, NULL, 0);
    if (mem == (DatosCompartidos *) -1) {
        perror("Error asociando memoria compartida");
        exit(1);
    }

    // Inicializar memoria y semáforos POSIX compartidos (pshared = 1)
    memset(mem, 0, sizeof(*mem));
    sem_init(&mem->vacio, 1, 1); 
    sem_init(&mem->lleno, 1, 0);

    // Crear FIFO
    mkfifo(FIFO_FILE, 0666);

    pid_prod = fork();

    if (pid_prod == 0) {
        // --- PRODUCTOR ---
        int fd = open(FIFO_FILE, O_WRONLY);
        if (fd < 0) {
            perror("Error abriendo FIFO");
            exit(1);
        }

        for (int i = 1; i <= NUM_DATOS; i++) {
            int a = i, b = i + 2;
            char buffer[BUFFER_SIZE];
            snprintf(buffer, BUFFER_SIZE, "%d %d", a, b);
            write(fd, buffer, strlen(buffer) + 1);
            printf("[Productor] Enviado al FIFO: %s\n", buffer);
            sleep(1); 
        }

        close(fd);
        exit(0);
    } else {
        pid_cons1 = fork();
        if (pid_cons1 == 0) {
            // --- CONSUMIDOR 1 (Lee FIFO, publica en SHM y multiplica) ---
            int fd = open(FIFO_FILE, O_RDONLY);
            int *resultados_mul = (int *)malloc(NUM_DATOS * sizeof(int));
            int n = 0;
            char buffer[BUFFER_SIZE];
            ssize_t r;

            // Lectura robusta del FIFO (Mitigación P6)
            while ((r = read(fd, buffer, BUFFER_SIZE)) > 0) {
                for (char *p = buffer; p < buffer + r; p += strlen(p) + 1) {
                    int a, b;
                    if (sscanf(p, "%d %d", &a, &b) == 2) {
                        
                        sem_wait(&mem->vacio); // Espera a que Consumidor 2 haya tomado el anterior
                        mem->a = a;
                        mem->b = b;
                        sem_post(&mem->lleno); // Avisa: hay un par nuevo
                        
                        resultados_mul[n] = a * b;
                        printf("[Consumidor 1] Publicado en SHM: %d, %d | Resultado (a*b): %d\n", a, b, resultados_mul[n]);
                        n++;
                    }
                }
                if(n >= NUM_DATOS) break;
            }

            // Detectar fin del FIFO y avisar al Consumidor 2
            sem_wait(&mem->vacio);
            mem->fin = 1;
            sem_post(&mem->lleno); 

            printf("[Consumidor 1] Resultados finales (mul): ");
            for (int i = 0; i < NUM_DATOS; i++) printf("%d ", resultados_mul[i]);
            printf("\n");

            free(resultados_mul);
            close(fd);
            exit(0);
        } else {
            pid_cons2 = fork();
            if (pid_cons2 == 0) {
                // --- CONSUMIDOR 2 (Lee desde SHM y suma) ---
                int *resultados_sum = (int *)malloc(NUM_DATOS * sizeof(int));
                int n = 0;

                for (;;) {
                    sem_wait(&mem->lleno); // Bloquea sin gastar CPU hasta que haya dato
                    
                    if (mem->fin) break; // Evita quedarse esperando por siempre (Mitigación P8)
                    
                    int a = mem->a, b = mem->b;
                    sem_post(&mem->vacio); // Libera el espacio inmediatamente
                    
                    resultados_sum[n] = a + b;
                    printf("[Consumidor 2] Leído de SHM: %d, %d | Resultado (a+b): %d\n", a, b, resultados_sum[n]);
                    n++;
                }

                printf("[Consumidor 2] Resultados finales (sum): ");
                for (int i = 0; i < n; i++) printf("%d ", resultados_sum[i]);
                printf("\n");

                free(resultados_sum);
                exit(0);
            }
        }
    }

    // --- PADRE (Cierre ordenado y limpieza P9, P10, P11) ---
    for (int i = 0; i < 3; i++) wait(NULL); // Esperar a Productor, Consumidor 1 y Consumidor 2
    printf("[Padre] Limpieza de recursos completada.\n");

    sem_destroy(&mem->vacio);
    sem_destroy(&mem->lleno);
    shmdt(mem);
    shmctl(shm_id, IPC_RMID, NULL);
    unlink(FIFO_FILE);

    return 0;
}
