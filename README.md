# Guía: Cluster Airflow con CeleryExecutor — Master + 2 Workers en EC2

Objetivo: simular un cluster real de Airflow distribuido en 3 instancias EC2 separadas, con RabbitMQ como broker de tareas entre el scheduler (master) y los workers.

Esto es un paso más allá de la guía anterior (una sola instancia con todo el compose) — aquí RabbitMQ pasa de estar "de adorno" a cumplir su función real: transportar mensajes de tarea entre máquinas distintas.

---

## 0. Arquitectura objetivo

| Instancia | Servicios | Rol |
|---|---|---|
| **master** | postgres, rabbitmq, airflow-apiserver, airflow-scheduler, airflow-dag-processor, airflow-triggerer, flower | Cerebro del cluster: decide qué ejecutar, guarda estado, encola tareas |
| **worker-1** | airflow-worker | Ejecuta tareas que toma de la cola |
| **worker-2** | airflow-worker | Ejecuta tareas que toma de la cola |

Los 3 nodos comparten `/opt/airflow/dags` y `/opt/airflow/logs` vía **EFS** (sección 3).

---

## 1. Red: VPC, subnets y Security Groups

### VPC y subnets

Para este ejercicio no necesitas una VPC custom elaborada — puedes usar la VPC default de tu cuenta, siempre que las 3 instancias queden en **subnets de la misma VPC** (no necesariamente la misma subnet, pero si usas varias AZs, EFS necesita un mount target por AZ — ver sección 3).

Más simple para aprender: lanza las 3 instancias en la **misma subnet**. Menos resiliente, pero elimina una variable mientras entiendes el flujo.

### Security Groups — necesitas 2

**SG `airflow-master`** (asociado a la instancia master):

| Puerto | Origen | Para qué |
|---|---|---|
| 22 | Tu IP | SSH |
| 8080 | Tu IP | Airflow UI |
| 15672 | Tu IP | Panel RabbitMQ |
| 5555 | Tu IP | Panel Flower (ver qué worker toma cada tarea) |
| 5672 | SG `airflow-worker` (no 0.0.0.0/0) | AMQP — los workers publican/consumen aquí |
| 5432 | SG `airflow-worker` | Postgres — los workers leen/escriben metadatos |

**SG `airflow-worker`** (asociado a worker-1 y worker-2):

| Puerto | Origen | Para qué |
|---|---|---|
| 22 | Tu IP | SSH |
| 2049 (NFS) | — | Lo abre automáticamente el SG de EFS, ver sección 3 |

**Punto clave**: en los puertos 5672 y 5432 del SG de master, el origen no es una IP sino **el propio Security Group de los workers** (`airflow-worker`). Esto le dice a AWS "permite tráfico desde cualquier instancia que tenga este SG asociado", sin importar su IP — es el patrón correcto para que puedas apagar/encender workers sin reconfigurar reglas cada vez.

---

## 2. Lanzar las 3 instancias

Mismo tamaño para las 3 por simplicidad: `t3.medium` (igual que en la guía de una sola instancia — Airflow sigue siendo el proceso más pesado, ahora repartido en más máquinas pero cada una corriendo imágenes completas de Airflow).

- **master**: Amazon Linux 2023, SG `airflow-master`, 20 GB EBS
- **worker-1** y **worker-2**: Amazon Linux 2023, SG `airflow-worker`, 20 GB EBS

Anota las **IPs privadas** de las 3 (no las públicas) — son las que usarás para que se hablen entre sí dentro de la VPC. Las verás en la consola EC2, columna "Private IPv4 address".

---

## 3. EFS — el filesystem compartido para /dags y /logs

Esto resuelve el problema que discutimos: sin esto, cada worker tendría su propia copia (o ausencia) de los DAGs, y el scheduler no puede garantizar que el worker que ejecuta una tarea tenga el mismo código que él mismo parseó.

### Crear el EFS

1. Consola AWS → **EFS** → **Create file system**.
2. Selecciona la misma VPC donde están tus 3 instancias.
3. AWS crea automáticamente un **mount target** por cada subnet/AZ que elijas — si pusiste las 3 instancias en la misma subnet, con un mount target basta.
4. En la configuración de red del mount target, AWS te pide un Security Group — crea uno nuevo (`airflow-efs`) que permita **puerto 2049 (NFS)** con origen el SG `airflow-master` y el SG `airflow-worker` (ambos, porque las 3 instancias van a montar el mismo EFS).

### Montarlo en las 3 instancias

En **cada una** de las 3 instancias (master, worker-1, worker-2):

```bash
# Instalar el cliente EFS
sudo dnf install -y amazon-efs-utils

# Crear el punto de montaje
sudo mkdir -p /mnt/efs/airflow

# Montar (reemplaza fs-XXXXXXXX por el ID real de tu EFS)
sudo mount -t efs fs-XXXXXXXX:/ /mnt/efs/airflow

# Verificar
df -h | grep efs
```

Para que sobreviva un reinicio, agrega a `/etc/fstab`:

```
fs-XXXXXXXX:/ /mnt/efs/airflow efs _netdev,tls 0 0
```

Dentro de `/mnt/efs/airflow`, crea las carpetas que luego montarás en los contenedores:

```bash
sudo mkdir -p /mnt/efs/airflow/{dags,logs,plugins,config}
sudo chown -R 50000:0 /mnt/efs/airflow
```

(`50000` es el UID por defecto del usuario `airflow` dentro de la imagen oficial — coincide con `AIRFLOW_UID` que ya usabas en tu compose original.)

---

## 4. Preparar la instancia master

```bash
# Instalar Docker (igual que en la guía anterior)
sudo dnf update -y
sudo dnf install -y docker
sudo systemctl enable --now docker
sudo usermod -aG docker ec2-user
# cierra sesión y vuelve a entrar

sudo mkdir -p /usr/local/lib/docker/cli-plugins
sudo curl -SL https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 \
  -o /usr/local/lib/docker/cli-plugins/docker-compose
sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
```

Sube `docker-compose.master.yaml` a `~/airflow-master/docker-compose.yaml` (scp o git clone).

Edita el archivo y **reemplaza `<MASTER_PRIVATE_IP>`** por la IP privada real de esta instancia (aparece dos veces, en `AIRFLOW__CORE__EXECUTION_API_SERVER_URL`).

Crea el `.env`:

```bash
cd ~/airflow-master
cat > .env <<EOF
AIRFLOW_UID=50000
AIRFLOW_PROJ_DIR=/mnt/efs/airflow
RABBITMQ_DEFAULT_USER=rabbit
RABBITMQ_DEFAULT_PASS=rabbit
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
EOF
```

Nota que `AIRFLOW_PROJ_DIR` ahora apunta a `/mnt/efs/airflow`, no a `.` como en tu compose original de un solo nodo — así los volúmenes del compose quedan en el filesystem compartido.

Levanta la master:

```bash
docker compose up airflow-init
docker compose up -d
docker compose ps
```

---

## 5. Preparar cada worker

En **worker-1** y **worker-2** (mismos pasos de instalación de Docker que en la master).

Sube `docker-compose.worker.yaml` a `~/airflow-worker/docker-compose.yaml` en cada instancia.

Edita el archivo y reemplaza **las 4 ocurrencias** de `<MASTER_PRIVATE_IP>` por la IP privada real de la instancia master.

`.env` en **worker-1**:

```bash
cd ~/airflow-worker
cat > .env <<EOF
AIRFLOW_UID=50000
AIRFLOW_PROJ_DIR=/mnt/efs/airflow
WORKER_NAME=worker-1
RABBITMQ_DEFAULT_USER=rabbit
RABBITMQ_DEFAULT_PASS=rabbit
EOF
```

`.env` en **worker-2** — idéntico salvo `WORKER_NAME=worker-2`.

Levanta cada worker:

```bash
docker compose up -d
docker compose ps
docker compose logs -f airflow-worker
```

En los logs deberías ver al worker conectarse exitosamente a RabbitMQ y Postgres (busca líneas de Celery confirmando conexión al broker).

---

## 6. Verificar que el cluster funciona de verdad

Esta es la parte que responde tu pregunta original — ver el flujo real, no solo que los contenedores están "up":

1. Entra a `http://<MASTER_PUBLIC_IP>:8080` (Airflow UI).
2. Entra a `http://<MASTER_PUBLIC_IP>:5555` (Flower) — en la pestaña **Workers** deberías ver `worker-1` y `worker-2` listados como activos, cada uno reportando su hostname.
3. Dispara un DAG de ejemplo desde la UI.
4. En Flower, pestaña **Tasks**, observa en tiempo real cómo cada tarea se asigna a uno u otro worker — esto es literalmente ver el broker (RabbitMQ) haciendo su trabajo: el scheduler publica la tarea, un worker la toma.
5. Entra a `http://<MASTER_PUBLIC_IP>:15672` (panel RabbitMQ) y mira la pestaña **Queues** mientras el DAG corre — verás mensajes entrando y saliendo de la cola `celery` en tiempo real.
6. Revisa los logs de un worker específico (`docker compose logs airflow-worker` en esa instancia) y confirma que ahí aparece la ejecución de la tarea que viste asignada en Flower.

Si ves tareas repartiéndose entre los dos workers (no siempre en el mismo), confirmaste que el cluster distribuido funciona como se espera.

---

## 7. Problemas comunes

- **Worker no aparece en Flower**: normalmente es el Security Group — revisa que el SG de master permita 5672 y 5432 con origen el SG de los workers, no una IP.
- **Tarea falla con "DAG not found" en el worker**: el mount de EFS no quedó igual en las 3 instancias, o el DAG se subió después de montar y el worker no lo ve — revisa `ls /mnt/efs/airflow/dags` en el worker.
- **`airflow-init` falla en la master**: normalmente permisos — confirma `sudo chown -R 50000:0 /mnt/efs/airflow` se ejecutó correctamente.

---

## 8. Costos y limpieza

- Ahora son **3 instancias `t3.medium`** corriendo en paralelo, más EFS (cobra por GB almacenado + algo de throughput). El costo sube proporcionalmente respecto a la guía de una sola instancia.
- **Detén las 3 instancias** cuando no las uses. EFS puedes dejarlo (es barato en reposo) o eliminarlo si terminaste del todo el ejercicio.
- Si terminas definitivamente: termina las 3 instancias, elimina el EFS, y revisa que no quede ninguna Elastic IP huérfana.

---

## 9. Siguiente paso natural

Con el cluster funcionando, el ejercicio de fase 2 que ya tenías en mente (RDS + Amazon MQ) se vuelve aún más interesante aquí: en vez de que la master cargue con Postgres y RabbitMQ como contenedores, los reemplazarías por servicios gestionados — y la master quedaría solo con los procesos de Airflow (api-server, scheduler, dag-processor, triggerer), exactamente como ya tienes diseñados los workers.
