# Backup restore test matrix

Bu doküman, `pg_recyclebin` + `pgBackRest` ile iddia edilen senaryoları **elle kanıtlamak** için adımları özetler.

## Ne test ediliyor?

| Senaryo | Açıklama |
|--------|-----------|
| Büyük tablo | `fb_matrix_big` içine yaklaşık **10GB** ham veri (satır başı ~800 byte) |
| Diff zinciri | `full` → veri değişikliği → `diff` backup → DROP → restore (referans zinciri) |
| Şifreli repo | `repo1-cipher-type` + `repo1-cipher-pass`; extension’da `flashback.pgbackrest_cipher_pass` |
| Çoklu tablo | Ardışık `flashback_restore_from_backup` (FK sırası: parent → child) |
| FK / partition / inheritance | `scripts/setup_backup_matrix_schema.sql` |

## Hızlı smoke (dakikalar)

```bash
cd /path/to/pg_recyclebin
cp scripts/backup_restore_matrix.env.example scripts/backup_restore_matrix.env
# .env içinde PGHOST/PGPORT/PGDATABASE düzenle
source scripts/backup_restore_matrix.env
export PGFB_TARGET_GB=0.05
./scripts/backup_restore_test_matrix.sh all
```

## 10GB yük (uzun sürebilir)

```bash
export PGFB_TARGET_GB=10
./scripts/backup_restore_test_matrix.sh load-only   # sadece veri yükle
./scripts/backup_restore_test_matrix.sh backup-chain
# ... DROP + restore adımları script çıktısını takip et
./scripts/backup_restore_test_matrix.sh all
```

Disk: geçici restore için hedef DB boyutu + repo kadar alan gerekir (`--db-include` kullanılıyor).

## Şifreli repo

1. **Yeni** bir repo dizini kullanmanız önerilir (mevcut plain repo’yu bozmamak için).
2. `pgbackrest.conf` içine (örnek):

```ini
repo1-cipher-type=aes-256-cbc
repo1-cipher-pass=my-long-test-passphrase
```

3. `PGFB_ENCRYPTED_REPO=1` ve `PGFB_CIPHER_PASS` ile script ortamını ayarla.
4. İlk backup’tan önce `stanza-create` / `backup` şifreli repo ile alınır.
5. Restore öncesi SQL’de:

```sql
SET flashback.pgbackrest_cipher_pass = 'my-long-test-passphrase';
```

Üretimde şifreyi `pg_settings`’te tutmamak için GUC tasarımı zaten superuser-only; testte kısa süreli SET kabul edilebilir.

## Script fazları

| Komut | Ne yapar |
|--------|-----------|
| `schema` | `setup_backup_matrix_schema.sql` + küçük seed |
| `load-only` | `PGFB_TARGET_GB` kadar `fb_matrix_big` doldurur |
| `backup-full` | `pgbackrest backup --type=full` |
| `mutate` | Diff’i anlamlı kılmak için küçük değişiklik |
| `backup-diff` | `pgbackrest backup --type=diff` |
| `drop-matrix` | Tabloları DROP (metadata operations’a düşer) |
| `purge-recycle` | Recycle’ı temizle (backup yolunu zorla) |
| `restore-all` | GUC set + sırayla restore |
| `all` | schema → load → full → mutate → diff → drop → purge → restore |

## Beklenen sonuç

- `restore-all` sonunda `fb_matrix_big`, `fb_matrix_orders`, `fb_matrix_order_lines`, `fb_matrix_part`, `fb_matrix_parent`/`fb_matrix_child` satır sayıları kontrol sorgularıyla doğrulanır.
- Diff senaryosunda restore, zincirdeki **son** backup set’e karşı yapılır (script `latest` etiketini kullanır; gerekirse `PGFB_BACKUP_SET` ile sabitleyin).

## Sınırlar

- **100GB+** bu dokümanda yok; mantık 10GB ile aynı, süre ve disk lineer büyür.
- Cloud RDS snapshot / Barman ayrı çalışma gerekir (bu matrix sadece pgBackRest disk repo).
