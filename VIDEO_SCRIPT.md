# 🎬 Skrip Video YouTube: Distributed Sync System
**Durasi Target:** 12–14 menit | **Bahasa:** Indonesia

---

## PERSIAPAN SEBELUM REKAM

### Setup Layar
- Resolusi: **1920×1080** (Full HD)
- Terminal: pakai **Windows Terminal** atau **PowerShell** dengan font besar (14–16pt)
- VS Code: buka folder proyek, tampilkan Explorer di sisi kiri
- Matikan notifikasi Windows sebelum rekam

### Buka Terlebih Dahulu (Jangan Rekam Dulu)
1. VS Code dengan folder `distributed-sync-system` terbuka
2. 3 tab terminal disiapkan (tapi belum dijalankan)
3. Browser buka tab:
   - `TECHNICAL_DOCUMENTATION.md` preview (GitHub atau VS Code Preview)
   - `PERFORMANCE_REPORT.md` preview
4. File Explorer di folder proyek

### Command yang Disiapkan di Notepad (Copy-Paste Saat Rekam)
```
pytest tests/integration/ -v
pytest tests/integration/test_lock_manager.py -v -s
pytest tests/integration/test_security.py -v -s
```

---

## BAGIAN 1 — PENDAHULUAN (1:30 menit)
**[Tampilan: Layar VS Code dengan folder proyek terbuka]**

### Yang Diucapkan:
> "Halo semuanya, selamat datang di presentasi proyek Tugas 3 Sistem Terdistribusi."

> "Pada video ini saya akan mendemonstrasikan **Distributed Sync System** — sebuah sistem sinkronisasi terdistribusi yang saya bangun dari nol menggunakan Python."

> "Sistem ini mengimplementasikan **empat komponen utama** yang merupakan inti dari distributed systems modern:"

*[Tunjuk ke tree folder di VS Code Explorer]*

> "Pertama, **Distributed Lock Manager** — menggunakan algoritma Raft untuk mencapai konsensus dalam pengelolaan kunci terdistribusi."

> "Kedua, **Distributed Queue System** — menggunakan Consistent Hashing untuk mendistribusikan antrian pesan secara merata."

> "Ketiga, **Distributed Cache Coherence** — mengimplementasikan protokol MESI seperti yang ada pada arsitektur CPU modern, namun diterapkan pada level jaringan."

> "Keempat, sistem ini dilengkapi **Containerization** menggunakan Docker dan Docker Compose."

> "Selain empat komponen wajib, saya juga mengimplementasikan dua fitur bonus: **PBFT Byzantine Fault Tolerance** dan **Security & Encryption** end-to-end."

> "Tanpa basa-basi lagi, mari kita mulai dari arsitektur sistem."

---

## BAGIAN 2 — PENJELASAN ARSITEKTUR (2:30 menit)
**[Tampilan: Buka TECHNICAL_DOCUMENTATION.md di browser/preview]**

### Yang Diucapkan:

*[Scroll ke diagram Mermaid pertama — High-Level Architecture]*

> "Ini adalah gambaran arsitektur sistem secara keseluruhan."

> "Sistem ini berbasis **Peer-to-Peer**. Tidak ada server pusat — setiap node setara dan dapat berkomunikasi langsung satu sama lain."

> "Semua komunikasi antar-node melewati **Security Layer** terlebih dahulu. Di sinilah enkripsi Fernet AES bekerja — setiap pesan dienkripsi sebelum dikirim melalui jaringan."

> "Di bawahnya ada tiga Sync Node: Lock Manager, Queue System, dan Cache System. Masing-masing berjalan secara independen namun bisa diintegrasikan."

> "Untuk penyimpanan state, kita menggunakan tiga mekanisme berbeda: Raft Log di memory untuk lock, AOF file di disk untuk queue, dan Redis untuk cache."

*[Scroll ke diagram Raft State Machine]*

> "Untuk Distributed Lock, saya menggunakan algoritma **Raft Consensus**. Di sini Anda bisa melihat state machine-nya — setiap node mulai sebagai Follower, kemudian bisa menjadi Candidate saat election timeout terjadi, dan akhirnya menjadi Leader jika mendapat suara mayoritas."

> "Yang menarik adalah sistem ini juga memiliki **Deadlock Detection** berbasis Wait-For Graph — jika terjadi siklus dependensi antar klien, sistem otomatis menolak permintaan yang berpotensi deadlock."

*[Scroll ke diagram Consistent Hash Ring]*

> "Untuk Queue System, saya menggunakan **Consistent Hashing** dengan 100 virtual node per physical node. Ini memastikan distribusi beban yang merata dan meminimalisir perpindahan data saat ada node yang masuk atau keluar dari kluster."

*[Scroll ke diagram MESI State Machine]*

> "Dan ini adalah diagram transisi state **protokol MESI** untuk Cache System — Modified, Exclusive, Shared, Invalid. Protokol ini menentukan bagaimana setiap node mengelola salinan data di cache lokalnya."

---

## BAGIAN 3 — LIVE DEMO SEMUA FITUR (6 menit)

### Demo 3A: Jalankan Semua Tes Sekaligus (1 menit)
**[Tampilan: Buka terminal baru]**

### Yang Diucapkan:
> "Sekarang mari kita jalankan sistem secara langsung. Saya akan mulai dengan menjalankan seluruh integration test menggunakan **pytest**."

*[Ketik di terminal:]*
```bash
pytest tests/integration/ -v
```

> "Framework pytest akan menjalankan 5 kelompok tes secara otomatis — untuk Lock Manager, Queue System, Cache System, PBFT, dan Security."

*[Tunggu output berjalan — sambil mengomentari]*

> "Terlihat test berjalan satu per satu... Cache System lulus... Lock Manager lulus... PBFT lulus..."

*[Saat muncul "5 passed"]*

> "Dan hasilnya: **lima dari lima tes lulus!** Semua komponen sistem berjalan dengan benar."

---

### Demo 3B: Distributed Lock Manager — Raft + Deadlock (1:30 menit)
**[Tampilan: Jalankan dengan -s agar log terlihat]**

*[Ketik:]*
```bash
pytest tests/integration/test_lock_manager.py -v -s
```

### Yang Diucapkan:
> "Sekarang kita lihat lebih detail pada Lock Manager. Saya jalankan dengan flag `-s` agar kita bisa melihat output log-nya secara real-time."

*[Saat log leader election muncul]*

> "Lihat di sini — tiga node mulai sebagai **Follower**. Setelah election timeout, salah satu mengajukan diri menjadi **Candidate** dan kemudian terpilih sebagai **Leader**. Ini adalah proses Raft Leader Election."

*[Saat log 'Deadlock' atau 'Rejected' muncul]*

> "Dan di bagian ini, sistem mendeteksi **Deadlock**. ClientX menunggu resource yang dipegang ClientY, sementara ClientY juga menunggu resource yang dipegang ClientX. Sistem mendeteksi siklus ini dan **menolak** permintaan ClientY, mencegah deadlock terjadi."

---

### Demo 3C: Fitur Keamanan — E2E Encryption, RBAC, Audit Log (2 menit)
**[Tampilan: Terminal baru]**

*[Ketik:]*
```bash
pytest tests/integration/test_security.py -v -s
```

### Yang Diucapkan:
> "Sekarang demo untuk fitur bonus Security. Ada tiga sub-fitur yang akan kita lihat."

*[Saat output RBAC muncul]*

> "Pertama, **RBAC — Role-Based Access Control**. Terlihat bahwa `client_C` dengan role **guest** mencoba melakukan operasi `write`, dan sistem langsung menolaknya dengan pesan: *Access Denied*. Sementara `client_A` dengan role admin bisa melakukan semua operasi."

*[Saat output Encryption muncul]*

> "Kedua, **End-to-End Encryption**. Di sini kita bisa melihat payload yang diintersep di jaringan — isinya bukan teks biasa, melainkan ciphertext yang dienkripsi dengan Fernet AES. Kata 'SECRET_TRANSFER' tidak terlihat sama sekali di paket jaringan."

> "Dan setelah didekripsi di tujuan, pesan aslinya muncul kembali dengan benar."

*[Buka file `data/audit_logs/test_node_audit.log` di VS Code]*

> "Ketiga, **Tamper-Proof Audit Log**. Ini adalah file log audit yang dihasilkan sistem. Setiap entri memiliki `prev_hash` dan `hash` — ini membentuk struktur **blockchain** sederhana. Jika ada yang memodifikasi baris manapun secara manual, hash chain akan rusak dan sistem akan langsung mendeteksinya."

---

### Demo 3D: PBFT — Byzantine Fault Tolerance (1 menit)
**[Tampilan: Terminal]**

*[Ketik:]*
```bash
pytest tests/integration/test_pbft.py -v -s
```

### Yang Diucapkan:
> "Ini adalah demo fitur bonus PBFT atau **Practical Byzantine Fault Tolerance**."

*[Saat log malicious node muncul]*

> "Kita punya 4 node — node 1, 2, 3 adalah node jujur, dan node 4 adalah **node curang** yang sengaja mengirimkan data palsu. Lihat di log ini — node 4 mengirimkan `FAKE_DIGEST` pada fase PREPARE dan COMMIT."

*[Saat log 'Consensus Reached' muncul]*

> "Namun hasilnya? Konsensus **tetap tercapai** di tiga node jujur. Mereka berhasil mengeksekusi perintah yang benar meskipun ada satu node yang berkhianat. Ini adalah implementasi toleransi Byzantine dengan rumus f = (N-1)/3."

---

## BAGIAN 4 — CONTAINERIZATION DENGAN DOCKER (2 menit)
**[Tampilan: Buka terminal baru, tampilkan file `docker-compose.yml` di VS Code sebelah kiri]**

### Yang Diucapkan:

> "Sekarang kita beralih ke bagian **Containerization** menggunakan Docker. Ini adalah komponen wajib Bagian D dari tugas ini."

*[Tampilkan file `docker-compose.yml` di VS Code]*

> "Di sini saya memiliki file `docker-compose.yml` yang mendefinisikan seluruh infrastruktur sistem secara lengkap. Ada **sepuluh container** yang akan berjalan sekaligus: tiga **Lock Node** untuk kluster Raft, tiga **Queue Node** untuk kluster Consistent Hashing, tiga **Cache Node** untuk kluster MESI, dan satu **Redis** sebagai distributed state backend."

> "Yang penting untuk diperhatikan — semua node ini menggunakan **satu Dockerfile yang sama**. Tidak ada Dockerfile terpisah per komponen. Perbedaannya hanya pada argumen `--service` dan `--node-id` yang diberikan di bagian `command`. Inilah prinsip *build once, run anywhere* dalam Docker."

*[Scroll ke bagian `networks` dan `volumes`]*

> "Semua container terhubung dalam satu jaringan virtual bernama `sync_network` sehingga bisa saling berkomunikasi secara langsung. Volume `redis_data` memastikan data Redis tidak hilang meskipun container di-restart."

*[Tampilkan file `.env` di VS Code]*

> "Seluruh konfigurasi — termasuk daftar node per kluster, host Redis, dan port — dipisahkan ke dalam file `.env`. Ini memenuhi syarat *environment configuration* dan mendukung **dynamic scaling**: untuk menambah node, cukup tambahkan entri di `.env` dan service baru di compose, tanpa menyentuh kode Python sama sekali."

*[Pindah ke terminal, ketik:]*
```bash
docker-compose up --build -d
```

> "Sekarang saya jalankan `docker-compose up --build -d`. Flag `--build` membangun image dari Dockerfile kita, dan `-d` artinya berjalan di *background*."

*[Tunggu proses build 30–60 detik — komentari selama menunggu]*

> "Proses build ini menginstal semua dependensi dari `requirements.txt` ke dalam image. Image yang sama ini akan dipakai oleh semua sepuluh container..."

*[Saat selesai, ketik:]*
```bash
docker-compose ps
```

*[Tunjuk ke output — 10 container STATUS: Up]*

> "Terlihat **sepuluh container** berjalan sekaligus — tiga lock node, tiga queue node, tiga cache node, dan satu Redis. Semua dalam status **Up** dan terhubung dalam jaringan `sync_network`."

*[Ketik:]*
```bash
docker-compose logs --tail=5 lock_node_1 queue_node_1 cache_node_1
```

> "Kita bisa memonitor log dari node representatif masing-masing kluster sekaligus. Lihat — lock node sedang melakukan Raft election, queue node menginisialisasi hash ring, dan cache node berhasil terhubung ke Redis."

*[Tampilkan `Dockerfile` di VS Code]*

> "Satu hal yang ingin saya tekankan: meskipun ada sepuluh container, kita hanya punya **satu Dockerfile**. Setiap container mendapat argumen berbeda via `command` di docker-compose — inilah yang disebut *universal entrypoint pattern*."

*[Ketik untuk menghentikan:]*
```bash
docker-compose down
```

> "Dan cukup satu perintah `docker-compose down` untuk menghentikan semua sepuluh container sekaligus. Inilah kekuatan orchestration — mengelola puluhan container dengan perintah yang sederhana."


---

## BAGIAN 5 — PERFORMANCE TESTING (2 menit)
**[Tampilan: Buka PERFORMANCE_REPORT.md di browser]**

### Yang Diucapkan:

> "Sekarang kita lihat hasil **Performance Analysis** dari sistem ini."

*[Scroll ke tabel Throughput]*

> "Hasil benchmark menunjukkan perbandingan antara mode Single-Node dan 3-Node Distributed. Untuk produce throughput, single-node mencapai sekitar **1.95 operasi per detik**, sementara mode terdistribusi sedikit lebih lambat di **1.82 operasi per detik**."

> "Overhead dari distribusi hanya **6.7%** — nilai yang sangat kecil dibandingkan manfaat fault tolerance yang kita dapatkan."

*[Scroll ke bagian Cache Hit vs Miss]*

> "Yang paling menarik adalah perbandingan Cache HIT vs MISS. Cache hit — artinya data sudah ada di cache lokal — hanya membutuhkan **0.004 milidetik**. Sangat cepat karena akses langsung ke memori."

> "Sedangkan cache miss — di mana node harus meminta data dari node lain via bus MESI — membutuhkan **83 milidetik**. Perbedaannya hampir **20.000 kali lipat!** Ini membuktikan betapa pentingnya hit rate yang tinggi dalam sistem cache terdistribusi."

*[Scroll ke bagian Scalability]*

> "Dan untuk skalabilitas payload, sistem tetap stabil meskipun ukuran pesan meningkat dari 64 byte hingga 4 kilobyte. Throughput hanya turun sekitar 15% — bottleneck utamanya bukan di ukuran pesan, melainkan di operasi disk write AOF yang dilakukan setiap produce."

---

## BAGIAN 6 — KESIMPULAN DAN TANTANGAN (1:30 menit)
**[Tampilan: Kembali ke VS Code — tampilkan struktur folder proyek]**

### Yang Diucapkan:

> "Baiklah, mari kita rangkum apa yang telah kita demonstrasikan hari ini."

> "Sistem ini berhasil mengimplementasikan **empat komponen wajib** — Lock Manager dengan Raft dan Deadlock Detection, Queue System dengan Consistent Hashing dan AOF persistence, Cache Coherence dengan protokol MESI dan Redis, serta Containerization dengan Docker."

> "Ditambah **dua fitur bonus** — PBFT untuk Byzantine Fault Tolerance, dan sistem Security lengkap dengan E2E encryption, RBAC, dan audit log berbasis hash chain."

> "Seluruh pengujian menggunakan **pytest** dan **locust** sebagai mandatory testing stack."

*[Jeda sebentar]*

> "Sekarang saya ingin cerita tentang **tantangan** yang saya hadapi selama pengembangan."

> "Tantangan pertama adalah implementasi **Bus Snooping MESI**. Tidak seperti protokol MESI pada CPU yang berjalan di shared memory, di sini saya harus mensimulasikannya melalui TCP sockets asinkron. Setiap operasi miss harus mengirim pesan broadcast ke semua peer dan menunggu respons — ini yang menyebabkan miss latency mencapai 80 milidetik."

> "Tantangan kedua adalah membuat **pengujian asinkron** bekerja dengan pytest. Karena semua node menggunakan asyncio, saya perlu mengintegrasikan `pytest-asyncio` dan memastikan setiap test function didekorasi dengan benar."

> "Tantangan ketiga adalah **integrasi Redis** — terutama saat pengujian lokal tanpa server Redis sungguhan. Solusinya adalah menggunakan `fakeredis` sebagai shim, sehingga tes tetap bisa berjalan di environment manapun."

*[Penutup]*

> "Secara keseluruhan, proyek ini memberikan pemahaman mendalam tentang trade-off dalam distributed systems — antara **konsistensi, ketersediaan, dan performa**. Tidak ada solusi sempurna; setiap keputusan arsitektur membawa kompromis."

> "Terima kasih telah menonton. Seluruh source code tersedia dan dokumentasi lengkap ada di dalam repositori. Sampai jumpa!"

---

## CHECKLIST REKAMAN

### Sebelum Mulai Rekam
- [ ] Resolusi layar 1920×1080
- [ ] Font terminal ukuran 14+
- [ ] Notifikasi Windows dimatikan
- [ ] Mode Do Not Disturb aktif
- [ ] VS Code: Explorer terbuka, font size 14
- [ ] Browser: preview TECHNICAL_DOCUMENTATION.md & PERFORMANCE_REPORT.md siap
- [ ] Terminal tab terbuka (belum dijalankan)
- [ ] Command siap di Notepad untuk copy-paste
- [ ] **Docker Desktop sudah dibuka dan dalam status Running**
- [ ] Jalankan `docker-compose down` dulu untuk pastikan kondisi bersih
- [ ] Test `pytest tests/integration/ -v` sudah dijalankan sekali (untuk warm cache)
- [ ] Rekam audio terpisah jika ada microphone eksternal

### Urutan Rekaman
1. ✅ Pendahuluan — tunjukkan folder proyek
2. ✅ Arsitektur — scroll TECHNICAL_DOCUMENTATION.md
3. ✅ Demo pytest all → Lock Manager → Security → PBFT
4. ✅ **Docker: `docker-compose up --build -d` → ps → logs → down**
5. ✅ Performance — scroll PERFORMANCE_REPORT.md
6. ✅ Kesimpulan & Tantangan

### Tips Rekaman
- Bicara **pelan dan jelas** — tidak perlu terburu-buru
- Saat terminal berjalan, **beri komentar** sambil menunggu (jangan diam)
- Saat `docker-compose up --build` berjalan (~30-60 detik), ceritakan tentang Dockerfile sambil menunggu
- Gunakan **zoom in** di bagian penting (Windows: Win + "+")
- Highlight teks penting dengan mouse hover
- Jika salah ucap, diam 3 detik lalu lanjut — bisa dipotong saat editing
- **Khusus Docker**: pastikan Docker Desktop sudah Running SEBELUM rekam dimulai

---

## STRUKTUR WAKTU (Total: ~14-15 menit)

| Bagian | Durasi | Konten |
|--------|--------|--------|
| Intro & Pendahuluan | 1:30 | Perkenalan diri + overview fitur |
| Arsitektur Sistem | 2:30 | Scroll docs + jelaskan diagram Mermaid |
| Demo: pytest all | 1:00 | 5 tes lulus sekaligus |
| Demo: Lock + Deadlock | 1:30 | Log raft election + deadlock reject |
| Demo: Security | 2:00 | RBAC + Encryption + Audit Log |
| Demo: PBFT | 1:00 | Node curang gagal ganggu konsensus |
| **Demo: Docker** | **2:00** | **build → ps → logs → Dockerfile → down** |
| Performance Report | 2:00 | Scroll laporan + jelaskan angka |
| Kesimpulan | 1:30 | Ringkasan + tantangan + penutup |
| **TOTAL** | **~15 menit** | |
