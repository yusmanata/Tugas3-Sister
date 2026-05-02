# 🔄 Distributed Sync System

Sistem sinkronisasi terdistribusi yang mengimplementasikan berbagai algoritma konsensus dan sinkronisasi data untuk mensimulasikan skenario *real-world distributed systems*.

---

## 📋 Daftar Isi
1. [Fitur Sistem](#fitur-sistem)
2. [Prasyarat](#prasyarat)
3. [Instalasi](#instalasi)
4. [Konfigurasi](#konfigurasi)
5. [Menjalankan Pengujian (Pytest)](#menjalankan-pengujian-pytest)
6. [Menjalankan dengan Docker](#menjalankan-dengan-docker)
7. [Menjalankan Node Secara Manual](#menjalankan-node-secara-manual)
8. [Load Testing dengan Locust](#load-testing-dengan-locust)
9. [Dokumentasi Tambahan](#dokumentasi-tambahan)
10. [Struktur Proyek](#struktur-proyek)

---

## Fitur Sistem

| Bagian | Fitur | Teknologi |
|--------|-------|-----------|
| **A** | Distributed Lock Manager | Raft Consensus + Deadlock Detection |
| **B** | Distributed Queue System | Consistent Hashing + AOF Persistence |
| **C** | Distributed Cache Coherence | MESI Protocol + LRU + Redis |
| **D** | Containerization | Docker + Docker Compose |
| **Bonus** | PBFT Byzantine Fault Tolerance | 3-Phase Commit |
| **Bonus** | Security & Encryption | Fernet E2E + RBAC + Tamper-Proof Logs |

---

## Prasyarat

Pastikan perangkat lunak berikut sudah terinstal sebelum memulai:

- **Python 3.8+** → [Download Python](https://www.python.org/downloads/)
- **pip** (biasanya sudah terinstal bersama Python)
- **Docker Desktop** *(opsional, untuk containerization)* → [Download Docker](https://www.docker.com/products/docker-desktop/)

Verifikasi instalasi:
```bash
python --version   # Python 3.8+
pip --version
docker --version   # Docker 20.x (opsional)
```

---

## Instalasi

### 1. Clone / Masuk ke Direktori Proyek
```bash
cd d:\Tugas3-Sister\distributed-sync-system
```

### 2. Install Dependensi Python
```bash
pip install -r requirements.txt
```

Daftar paket yang akan terinstal:
```
cryptography==41.0.3   # E2E Encryption
redis==5.0.1           # Distributed State (Cache)
pytest==7.4.2          # Testing framework
pytest-asyncio==0.21.1 # Async test support
locust==2.16.1         # Load testing
fakeredis              # Redis simulator (testing lokal)
```

---

## Konfigurasi

### 1. Salin File Environment
```bash
# Windows
copy .env.example .env

# Linux/Mac
cp .env.example .env
```

### 2. Edit File `.env` (jika diperlukan)

Buka file `.env` dan sesuaikan:
```env
# Konfigurasi kluster (format: node_id:host:port,...)
CLUSTER_NODES=node_1:node_1:8001,node_2:node_2:8002,node_3:node_3:8003

# Konfigurasi Redis
# Gunakan 'localhost' untuk pengujian lokal
# Gunakan 'redis' saat berjalan di Docker
REDIS_HOST=localhost
REDIS_PORT=6379
```

> **Catatan:** Untuk menjalankan tes lokal (pytest), konfigurasi default di `.env` sudah cukup. Tidak perlu Redis sungguhan karena tes menggunakan `fakeredis`.

---

## Menjalankan Pengujian (Pytest)

Ini adalah cara paling mudah untuk memvalidasi **semua fitur** sekaligus tanpa perlu menjalankan node secara terpisah.

### Jalankan Semua Tes Sekaligus
```bash
pytest tests/integration/ -v
```

### Menjalankan Tes Per Fitur

```bash
# Bagian A: Distributed Lock Manager (Raft + Deadlock Detection)
pytest tests/integration/test_lock_manager.py -v

# Bagian B: Distributed Queue System (Consistent Hashing + AOF)
pytest tests/integration/test_queue_system.py -v

# Bagian C: Distributed Cache Coherence (MESI + LRU + Redis)
pytest tests/integration/test_cache_system.py -v

# Bonus: PBFT Byzantine Fault Tolerance
pytest tests/integration/test_pbft.py -v

# Bonus: Security & Encryption (E2E + RBAC + Audit)
pytest tests/integration/test_security.py -v
```

### Output yang Diharapkan
```
============================= test session starts =============================
platform win32 -- Python 3.13.1, pytest-7.4.2, pluggy-1.6.0
plugins: asyncio-0.21.1
collected 5 items

tests\integration\test_cache_system.py  .                            [ 20%]
tests\integration\test_lock_manager.py  .                            [ 40%]
tests\integration\test_pbft.py          .                            [ 60%]
tests\integration\test_queue_system.py  .                            [ 80%]
tests\integration\test_security.py      .                            [100%]

============================= 5 passed in ~37.00s ============================
```

---

## Menjalankan dengan Docker

Cara ini menjalankan sistem secara penuh di dalam container yang terisolasi, termasuk Redis.

### 1. Pastikan Docker Desktop Berjalan

Buka **Docker Desktop** dan tunggu hingga statusnya *Running*.

### 2. Sesuaikan `.env` untuk Docker

Ubah `REDIS_HOST` dari `localhost` menjadi `redis` (nama service di docker-compose):
```env
REDIS_HOST=redis
REDIS_PORT=6379
CLUSTER_NODES=node_1:node_1:8001,node_2:node_2:8002,node_3:node_3:8003
```

### 3. Build dan Jalankan Semua Container

```bash
docker-compose up --build -d
```

Perintah ini akan:
- Build *image* Python dari `Dockerfile`
- Menjalankan **3 Queue Node** (`node_1`, `node_2`, `node_3`)
- Menjalankan **1 Redis container** sebagai distributed state
- Menghubungkan semua container dalam jaringan `sync_network`

### 4. Lihat Status Container

```bash
docker-compose ps
```

Output yang diharapkan:
```
NAME              IMAGE                     STATUS    PORTS
queue_node_1      distributed-sync-...      Up
queue_node_2      distributed-sync-...      Up
queue_node_3      distributed-sync-...      Up
redis_store       redis:alpine              Up        0.0.0.0:6379->6379/tcp
```

### 5. Pantau Log Secara Real-Time

```bash
# Semua container
docker-compose logs -f

# Hanya node tertentu
docker-compose logs -f node_1
```

### 6. Hentikan Semua Container

```bash
docker-compose down
```

### Scaling Node Secara Dinamis

Untuk menambah node menjadi **5 node** tanpa mengubah kode Python:

**Step 1:** Edit `.env`:
```env
CLUSTER_NODES=node_1:node_1:8001,node_2:node_2:8002,node_3:node_3:8003,node_4:node_4:8004,node_5:node_5:8005
```

**Step 2:** Tambahkan service di `docker-compose.yml`:
```yaml
  node_4:
    build: .
    container_name: queue_node_4
    command: ["--node-id", "node_4", "--service", "queue"]
    env_file: [.env]
    networks: [sync_network]
    volumes: [./data:/app/data]

  node_5:
    build: .
    container_name: queue_node_5
    command: ["--node-id", "node_5", "--service", "queue"]
    env_file: [.env]
    networks: [sync_network]
    volumes: [./data:/app/data]
```

**Step 3:** Jalankan ulang:
```bash
docker-compose up --build -d
```

---

## Menjalankan Node Secara Manual

Jika ingin menjalankan node individual (tanpa Docker), buka **4 terminal terpisah**.

### Terminal 1 — Node 1

```bash
cd d:\Tugas3-Sister\distributed-sync-system
python main.py --node-id node_1 --service queue
```

### Terminal 2 — Node 2

```bash
cd d:\Tugas3-Sister\distributed-sync-system
python main.py --node-id node_2 --service queue
```

### Terminal 3 — Node 3

```bash
cd d:\Tugas3-Sister\distributed-sync-system
python main.py --node-id node_3 --service queue
```

### Mengganti Service

Ganti argumen `--service` untuk menjalankan jenis node yang berbeda:

```bash
# Distributed Lock Manager
python main.py --node-id node_1 --service lock

# Distributed Cache
python main.py --node-id node_1 --service cache

# Distributed Queue
python main.py --node-id node_1 --service queue
```

---

## Load Testing dengan Locust

Locust digunakan untuk menguji ketahanan sistem di bawah beban tinggi (*stress test*).

### Prasyarat

Pastikan minimal 1 node Queue sedang berjalan di port 8001:
```bash
# Terminal terpisah
python main.py --node-id node_1 --service queue
```

### Jalankan Locust (Mode GUI)

```bash
locust -f locustfile.py
```

Kemudian buka browser di **http://localhost:8089**

Di halaman web Locust:
- **Number of users**: Jumlah pengguna simultan (contoh: `50`)
- **Spawn rate**: Jumlah user yang muncul per detik (contoh: `10`)
- **Host**: Kosongkan (sudah diatur di `locustfile.py`)

Klik **Start Swarming** untuk memulai pengujian.

### Jalankan Locust (Mode CLI / Headless)

```bash
# 100 user, spawn 10/detik, selama 60 detik
locust -f locustfile.py --headless -u 100 -r 10 -t 60s
```

### Output Locust

Locust akan menampilkan metrik real-time:
- **RPS** (Requests per Second)
- **Average/Min/Max response time**
- **Failure rate**

---

## Fitur Keamanan (Security Bonus)

Fitur keamanan aktif secara otomatis saat sistem berjalan. Berikut cara memverifikasinya:

### Verifikasi End-to-End Encryption

Tes enkripsi otomatis berjalan bersama `test_security.py`:
```bash
pytest tests/integration/test_security.py -v -s
```

Anda akan melihat payload terenkripsi di output:
```
Intercepted Network Payload:
{
  "encrypted_payload": "Z0FBQUFBQn..."  ← Ciphertext tidak terbaca
}
Encryption Verified: The secret payload was not visible in plain text.
```

### Verifikasi RBAC (Role-Based Access Control)

RBAC diuji otomatis. Output yang diharapkan:
```
RBAC Enforcement working: Access Denied: User 'client_C' with role 'guest'
cannot perform action 'write'
```

### Verifikasi Audit Log (Tamper-Proof)

Log audit tersimpan di:
```
data/audit_logs/<node_id>_audit.log
```

Format setiap baris (hash-chain blockchain-style):
```json
{
  "event": {"timestamp": ..., "event_type": "USER_LOGIN", "user_id": "client_A"},
  "prev_hash": "4b22c572...",
  "hash": "ae2ec2cc..."
}
```

---

## PBFT Byzantine Fault Tolerance (Bonus)

Demonstrasi PBFT dengan 4 node (1 node curang):

```bash
pytest tests/integration/test_pbft.py -v -s
```

Anda akan melihat log:
```
[PBFT-node_4] - WARNING - Malicious node sending FAKE digest in PREPARE
...
[PBFT-node_1] - INFO - Consensus reached. EXECUTING: {'action': 'transfer', 'amount': 100}
[PBFT-node_2] - INFO - Consensus reached. EXECUTING: {'action': 'transfer', 'amount': 100}
[PBFT-node_3] - INFO - Consensus reached. EXECUTING: {'action': 'transfer', 'amount': 100}
```

Node 1, 2, 3 berhasil mencapai konsensus meskipun Node 4 mengirim data palsu.

---

## Dokumentasi Tambahan

| File | Isi |
|------|-----|
| [`TECHNICAL_DOCUMENTATION.md`](./TECHNICAL_DOCUMENTATION.md) | Arsitektur sistem, penjelasan algoritma, API spec, deployment guide |
| [`PERFORMANCE_REPORT.md`](./PERFORMANCE_REPORT.md) | Hasil benchmark, analisis throughput/latency, comparison single vs distributed |
| [`scripts/benchmark.py`](./scripts/benchmark.py) | Skrip benchmark untuk mengukur performa sistem |
| [`locustfile.py`](./locustfile.py) | Konfigurasi load testing Locust |

---

## Struktur Proyek

```
distributed-sync-system/
├── src/
│   ├── communication/
│   │   ├── message_passing.py    # TCP I/O + E2E Encryption
│   │   └── failure_detector.py
│   ├── consensus/
│   │   ├── raft.py               # Raft Consensus (Bagian A)
│   │   └── pbft.py               # PBFT Byzantine (Bonus)
│   ├── nodes/
│   │   ├── base_node.py
│   │   ├── lock_manager.py       # Distributed Lock (Bagian A)
│   │   ├── queue_node.py         # Distributed Queue (Bagian B)
│   │   └── cache_node.py         # Cache Coherence MESI (Bagian C)
│   ├── security/
│   │   ├── crypto.py             # Fernet E2E Encryption
│   │   ├── rbac.py               # Role-Based Access Control
│   │   └── audit.py              # Tamper-Proof Audit Logging
│   └── utils/
│       ├── config.py             # Cluster & environment config
│       ├── consistent_hashing.py # Hash Ring (Bagian B)
│       └── metrics.py            # System performance counters
├── tests/
│   └── integration/
│       ├── test_lock_manager.py  # Test Bagian A
│       ├── test_queue_system.py  # Test Bagian B
│       ├── test_cache_system.py  # Test Bagian C
│       ├── test_pbft.py          # Test PBFT Bonus
│       └── test_security.py      # Test Security Bonus
├── data/
│   ├── queue_logs/               # AOF persistence files (Bagian B)
│   └── audit_logs/               # Tamper-proof audit logs (Bonus)
├── scripts/
│   └── benchmark.py              # Performance benchmark
├── main.py                       # Unified node entrypoint (Bagian D)
├── locustfile.py                 # Load testing (Mandatory Stack)
├── Dockerfile                    # Container image (Bagian D)
├── docker-compose.yml            # Multi-container orchestration (Bagian D)
├── requirements.txt              # Python dependencies
├── .env                          # Runtime configuration (aktif)
├── .env.example                  # Template konfigurasi
├── TECHNICAL_DOCUMENTATION.md   # Dokumentasi teknis (Laporan A)
└── PERFORMANCE_REPORT.md         # Laporan performa (Laporan B)
```

---

## Troubleshooting

| Masalah | Solusi |
|---------|--------|
| `KeyError: 'node_1'` | Pastikan `node_id` yang dipakai ada di `CLUSTER_NODES` di `.env` |
| `ConnectionRefusedError` | Node tujuan belum berjalan; pastikan urutan startup benar |
| `redis.ConnectionError` | Jalankan Redis terlebih dahulu: `docker-compose up redis -d` |
| `pytest` tidak ditemukan | Jalankan `pip install pytest pytest-asyncio` |
| `asyncio.TimeoutError` | Node peer tidak merespons; pastikan semua node sudah `start()` |
| `Audit Chain Broken` | File log dimodifikasi manual; hapus dan buat log baru |
| Docker `pipe not found` | Pastikan **Docker Desktop** sudah dibuka dan dalam status *Running* |

Link Youtube :
https://youtu.be/7rvkRylZ4_4