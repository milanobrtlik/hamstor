# Blokový layout pro velké soubory — návrh (stav k 2026-07-21)

Rozhodovací dokument. **Neobsahuje produkční kód a implementace z něj nevyplývá
automaticky** — každý krok ze sekce 5 je samostatné vlákno.

## Proč

Hamstor ukládá soubor jako jeden celý S3 objekt. Tři důsledky:

- **Zápis do existujícího souboru je read-modify-write celého souboru.** Otevření pro
  zápis stáhne celý objekt (`node.go:345` v write preloadu, `handle.go:184`
  v `ensureLoaded`), `Flush` nahraje celý zpátky (`handle.go:1135`). Připsání jednoho
  řádku do 3GB logu stojí 6 GB provozu.
- **Soubory nad 2 GB nejdou znovu otevřít pro zápis vůbec** — `s3store.MaxDownloadSize`
  (`s3.go:23`) je tvrdý strop na `Download`.
- **Soubory nad ~5 MiB jdou nahoru jako multipart**, protože `manager.NewUploader(client)`
  (`s3.go:59`) běží s defaultním `PartSize` 5 MiB. A právě multipart objekty dnes
  `Store.Download` neumí stáhnout celé: SDK neprojde validací CRC32
  (`checksum did not match: algorithm CRC32`), reprodukováno proti lokálnímu Garage.
  Range GETy téhož objektu jsou v pořádku. Prakticky to znamená, že velký soubor je po
  vypadnutí z cache nečitelný pro zápis vždy a pro čtení tehdy, když je šifrovaný
  (šifrování vypíná range cestu).

> **Ten třetí bod je dnes Garage-only, ne produkční.** Ověřeno 2026-07-21 v logu
> produkčního démona (B2, 60 dní): **nula** výskytů `checksum did not match`, zato
> opakovaně `SDK WARN Skipped validation of multipart checksum` — B2 hlásí composite
> checksum tak, že ho SDK rozpozná a validaci **přeskočí**. Garage ho hlásí jako holou
> hodnotu, SDK ji zvaliduje a neprojde.
>
> Expozice tedy je: **rozbité lokální testování** whole-object `Download` nad 5 MiB
> (přesně cesty, které bloková práce potřebuje testovat nejvíc), a **latentní produkční
> riziko** pro případ, že se backend hne od B2 nebo B2 změní hlášení. Ne aktivní ztráta
> dostupnosti dat v produkci. Řeší to **Krok 0** v sekci 5.

Cíl: soubor = N objektů pevné velikosti, mapování `(inode, block_index) -> klíč`.

**Model S3QL, ne log-structured slices JuiceFS.** Slices existují proto, aby zápis
nepotřeboval koordinaci mezi současnými klienty na více strojích — což hamstor záměrně
zakazuje `flock`em na `<dbPath>.lock` (`main.go:128`). Za tu volnost se platí tím, že
chyba v řešení překryvů slices selhává **tiše**: vrátí starou verzi bloku a nikdo se to
nedozví. Chyba v indexu bloků selhává hlučně — klíč buď existuje, nebo GET vrátí 404.
Při jednom zapisovateli je to čistá ztráta bez protihodnoty.

**Migrace není potřeba.** Produkční data neexistují, `purge-s3` + reinit je přijatelný.
Dokument proto nenavrhuje dvojí layout ani migrační nástroj — a to je zásadní úleva,
protože právě ta by byla nejdražší část.

Mimo rozsah: odložený (writeback) upload. Je to samostatné rozhodnutí *až po* blocích,
kdy se z něj stane optimalizace místo přepisu durability modelu. Stejně tak rozdělení
`dentries` z `inodes` — druhá schema změna, kterou tenhle purge+reinit taky umožňuje
zdarma, ale míchat dvě osy rizika do jedné změny se nevyplatí.

---

## 1. Rozhodnutí

### D1 — velikost bloku vs `PartSize` uploaderu

**Verdikt: `blockSize = 8 << 20` (8 MiB), `PartSize = 16 << 20` (16 MiB).**

Velikost bloku a `PartSize` nejde volit nezávisle, protože multipart je pro nás rozbitá
třída objektů. Místo záplaty (vypnout validaci checksumu, stahovat po rangích) se
`PartSize` nastaví tak, aby **každý blok byl z definice jeden PUT**. Pak žádný objekt
v systému multipart nikdy nevytvoří a celá třída problémů odpadá strukturálně.

Že to platí, je ověřené ve zdroji SDK, ne odhadnuté:

- `manager@v1.20.18/upload.go:391` — `upload()` udělá jeden `nextReader()`; když vrátí
  `io.EOF`, jde se do `singlePart()`. `nextReader()` (`upload.go:472`) vrací `io.EOF`
  právě když `bytesLeft <= PartSize`. Tedy **`size <= PartSize` ⇒ jeden `PutObject`**,
  garantovaně, ne heuristicky.
- Zvýšení `PartSize` **nestojí žádnou RAM** — pro tělo typu `readerAtSeeker` jede
  `nextReader()` větev s `io.NewSectionReader` bez alokace, a bufferový pool
  z `newByteSlicePool(PartSize)` (`upload.go:427`) alokuje slice až v `Get()`, který se
  na téhle větvi nikdy nezavolá. `Upload` posílá `bytes.NewReader`, `UploadReader` posílá
  `*os.File` — obojí splňuje `ReaderAt+Seeker`. **Tohle je podmínka, ne náhoda:** kdyby
  někdo protlačil do `Upload` obyčejný `io.Reader`, začne se pro každý souběžný upload
  alokovat 16 MiB, a `UploadSem` má kapacitu 32. Pod `debug.SetMemoryLimit(150<<20)`
  (`main.go:33`) to mount okamžitě zabije.
- `MinUploadPartSize` = 5 MiB je vynucené (`upload.go:386`), pod to `PartSize` nejde.

Proč zrovna 8 MiB, proti třem osám ze zadání:

| | 4 MiB | **8 MiB** | 16 MiB |
|---|---|---|---|
| řádků v `blocks` pro 4 TB | 1 048 576 (~90 MB) | **524 288 (~45 MB)** | 262 144 (~23 MB) |
| write amplification (zápis 1 B) | 4 MiB PUT | **8 MiB PUT** | 16 MiB PUT |
| Class B GETů na 4 TB čtení | 1M ($0.42) | **524k ($0.21)** | 262k ($0.10) |
| GETů na 100MB soubor | 25 | **13** | 7 |
| špinavý blok v RAM | 4 MiB | **8 MiB** | 16 MiB |

- **Počet řádků nerozhoduje.** 45 MB SQLite je stejný řád jako dnešní DB s 35k inody
  (viz stress test s ~35 000 soubory) a Litestream to snapshotuje jednou za 6 h. Ani
  90 MB by nebyla katastrofa; není to důvod jít nahoru.
- **Cena GETů nerozhoduje.** Na B2 je Class B $0.40/M, takže rozdíl mezi 4 a 16 MiB je
  na celém 4TB datasetu 30 centů. Co rozhoduje, je **latence**: 13 sériových GETů proti
  7 je znát, ale ty se paralelizují, zatímco write amplification ne.
- **Write amplification rozhoduje**, dokud neexistuje writeback cache. Do té doby každý
  `close()` na špinavém bloku pošle celý blok. 16 MiB je dvojnásobek toho i dvojnásobek
  paměti drženého bloku, a 150MB limit je reálný strop, ne teoretický.

Bonus, který rozhodl mezi 4 a 8: **8 MiB = `volume.TargetVolumeSize`.** Celý systém pak
má jednu velikost objektu — sealed volume i plný blok jsou stejně velké, takže LRU
v disk cache pracuje s uniformními jednotkami místo směsi 8 MiB volumes a 4 MiB bloků.

**Bloky se nepadují.** `blockSize` je krok, ne délka. Poslední blok je krátký, blok
souboru o 300 KB je objekt o 300 KB. Tím je pravidlo „blok ≤ PartSize" splněné triviálně
i pro šifrování (blok + 29 B režie GCM ≪ 16 MiB).

**Zavrženo:**
- *Nechat `PartSize` na 5 MiB a zvolit blok 4 MiB.* Funguje (blok < PartSize ⇒ single PUT),
  ale nechává 5MiB hranici jako neviditelnou past: kdokoli později zvedne blok na 8 MiB
  a nesáhne na `PartSize`, tiše obnoví multipart. Explicitní `PartSize = 2 × blockSize`
  je invariant, který jde napsat do jednoho řádku a otestovat.
- *Spolehnout se na opravu CRC32 bugu a nechat multipart být.* Ta oprava se udělat **má
  a musí** (Krok 0, je urgentní sama o sobě), ale nesmí být *předpokladem* tohohle
  návrhu: znamená vypnout validaci, kterou SDK dělá správně, a nechat blokovou cestu
  záviset na tom, že ji nikdo nezapne zpátky. `PartSize ≥ blockSize` drží i s validací
  zapnutou. Obojí, ne jedno místo druhého.

### D2 — tvar schématu

**Verdikt: samostatná tabulka `blocks` s `PRIMARY KEY (inode_id, block_index) WITHOUT ROWID`.
Ne manifest blob v řádku inodu.**

Rozhodující je, že DB replikuje Litestream a ten posílá **WAL stránky**:

- **Manifest blob se přepisuje celý.** Manifest 4GB souboru je 512 položek × ~40 B ≈
  20 KB, tedy blob přes overflow stránky. Změna jednoho bloku přepíše celý blob — při
  4 KB stránce ~6 stránek do WAL. Připisování do logu (nejčastější případ zápisu do
  existujícího souboru) tak generuje 6× víc replikačního provozu, než je potřeba.
- **Tabulka řádků se dotkne jen změněných řádků** — jedna leaf stránka.
- `WITHOUT ROWID` navíc dělá z primárního klíče samotnou tabulku: žádný druhý B-strom
  a žádný implicitní `rowid`, takže lookup `(inode, block_index)` je jedna sonda a
  zápisová stopa je poloviční proti rowid tabulce s `UNIQUE` indexem.

Druhý, nezávislý důvod proti blobu: **`AllS3KeySet` musí umět `SELECT s3_key FROM blocks`
v SQL.** S manifestem by GC musel načíst a deserializovat každý manifest v Go, což je
přesně ten druh kódu, jehož selhání smaže bucket (viz sekce 3, položka Č1). Dotaz do
tabulky selhat nemůže jinak než hlučně.

**Bez indexu na `s3_key`.** Reverzní lookup klíč → inode nikdo nepotřebuje: GC skenuje
celou množinu, `fsck` taky. Index by stál zápis navíc při každém commitu bloku.

**Zavrženo:** *manifest blob + `blocks` jen jako cache.* Dva zdroje pravdy o tomtéž, což
je přesně ta konfigurace, kde se rozejdou a nikdo si toho nevšimne.

### D3 — kolik tvarů uložení zůstane

**Verdikt: dva, ne tři. Nad `volume.MaxNeedleSize` (256 KB) je soubor *vždycky* bloky,
i kdyby to byl jeden blok. Sloupec `inodes.s3_key` jako datový ukazatel zmizí úplně.**

Dnes jsou dva tvary: needle ve volume a samostatný objekt. Bloky by udělaly třetí, a
větvení v read path (`Read`, `readChunked`, `readStreaming`, `ensureLoaded`,
`loadFromVolume`) by muselo pokrýt 3 × (cache hit / cache miss / dirty / šifrovaný).
Sloučení to vrací na dvě.

Co tím **zmizí z kódu**:

- **Celá 2MB chunk sub-cache** — `cache.chunkPath` / `GetChunk` / `PutChunk` / `HasChunk`
  (`cache.go:281-337`). Existuje jen proto, aby šlo range-číst uvnitř jednoho velkého
  objektu. Blok *je* chunk a cachuje se pod svým vlastním klíčem jako každý jiný objekt.
- **S ní obě pasti „chunk directory není soubor"** — `Has` (`cache.go:87`) a `Open`
  (`cache.go:104`). Obě existují výhradně kvůli tomu, že `PutChunk` položí adresář na
  cestu, kde jiní čekají soubor; `os.Open` na adresáři uspěje a write preload v
  `node.go:339` návratovou hodnotu `ReadAt` (tedy `EISDIR`) zahazuje. Ta konstrukce po
  zrušení chunků nemá důvod.
- **`readChunked` / `getOrFetchChunk` / `prefetchChunks`** se scvrknou na „načti blok N".
- **`Read`ova podmínka `!Cache.Has(h.s3Key)`** (`handle.go:338-339`) — dnes brání tomu,
  aby se range-četl objekt, který držíme celý lokálně. Pod bloky je jednotka cache a
  jednotka čtení tatáž věc, takže rozpor, který tu podmínku vyžádal, neexistuje.
- **`MaxDownloadSize`** přestane být stropem na velikost souboru a stane se sanity
  checkem na jeden blok.
- **`internal/ops/migrate.go` celý** — migruje `inodes.s3_key`, který přestane existovat.

A hlavně: **žádný objekt v systému už nepřeleze `PartSize`**, takže CRC32 bug (i strop
2 GB) mizí strukturálně, ne opravou.

Cena: soubor o 300 KB dostane místo hodnoty ve sloupci jeden řádek v `blocks`. Zanedbatelné.

**Zavrženo:** *ponechat samostatný objekt pro pásmo 256 KB – 8 MiB.* Ušetří jeden řádek
u běžné velikosti, ale drží `s3_key` naživu — a tím i celé větvení v read path, v GC,
v `Unlink`/`Rename`/`deleteTree` a v `fsck`. To je přesně ta cena, kterou tahle změna
platí za to, aby zmizela.

**Technika, kterou tenhle verdikt umožňuje: smazat `InodeMeta.S3Key`, ať inventuru udělá
compiler.** 170+ výskytů `S3Key`/`s3_key` v 9 souborech se změní v build errory. Ruční
inventura (sekce 3) je nutná k pochopení *co* na každém místě udělat, ale seznam míst,
který nesmí nikdo přehlédnout, vygeneruje `go build`. U změny, kde jedno přehlédnutí
maže bucket, to není pohodlí, ale bezpečnostní opatření.

### D4 — sledování špinavých bloků

**Verdikt: `inodeWrite` dostane dvě množiny indexů. Souvislý backing store zůstává jeden.
Špinavé bloky se při `Flush` kopírují do snapshot souboru.**

```go
type inodeWrite struct {
    // ...
    dirtyBlocks   map[int64]struct{} // zapsané od posledního flushe
    presentBlocks map[int64]struct{} // materializované lokálně (buf/spill)
}
```

Dnešní `loaded bool` se rozpadne na `presentBlocks`: „načteno" už není stav souboru, ale
stav jednotlivého bloku.

**Backing store zůstává jeden souvislý.** `Write`, `readLoaded`, `truncateWriteState`,
`logicalSize` i větev `O_APPEND` pracují s offsety do jedné lineární věci; rozdělení na
per-blok buffery přepisuje všechny čtyři a znovu otevírá souběžnostní invarianty bez
protihodnoty. Spill file se místo toho stane **řídkou lokální materializací** souboru:
`WriteAt`/`ReadAt` na offsetu už tam jsou, sparse soubory umí filesystem sám, a
`presentBlocks` říká, které rozsahy jsou skutečná data a které díry.

**Rozlišuj dvě různé nepřítomnosti**, jejich záměna je bug:

- **blok není v `blocks`** = v souboru je díra (sparse) ⇒ čtení vrací nuly, nefetchuje se;
- **blok není v `presentBlocks`** = není lokálně ⇒ musí se fetchnout, pokud řádek existuje.

Sparse soubory tím vycházejí zadarmo: zápis na offset 1 GB do prázdného souboru vytvoří
jeden řádek s `block_index = 127`, ne 128 objektů nul.

#### Proti pravidlům sdíleného write state z CLAUDE.md

**`writeMu` je leaf lock — beze změny.** Obě nové mapy žijí uvnitř `inodeWrite` a jsou
kryté `st.mu`, ne `writeMu`. `writeMu` dál drží jen mapu stavů a refcounty.

**Upload goroutina nikdy nebere `st.mu` — a právě proto se mění předání dat.** Dnes
`Flush` předá goroutině *vlastnictví* spill filu (`h.st.spillFile = nil`, `handle.go:846`)
a tím je hotovo. Pod bloky je spill file lokální materializace, ze které se dál čte a
píše, takže ho stav nemůže odevzdat. Kdyby ho goroutina četla souběžně, přečte směs dvou
verzí bloku a commitne ji — tichá koruze přesně toho druhu, kvůli kterému padly slices.

Řešení: **`Flush` pod `st.mu` zkopíruje špinavé bloky do snapshot souboru** v `SpillDir`,
vyprázdní `dirtyBlocks` a předá goroutině ten snapshot. Goroutina pak sahá výhradně na
soubor, který nikdo jiný nezná — pravidlo „nikdy nebere `st.mu`" platí beze změny
významu. Dnešní chování je degenerovaný případ (špinavé = celý soubor, předání přesunem
místo kopií), takže je to zobecnění, ne nový mechanismus.

Cena: jedna lokální kopie špinavých bajtů navíc na flush. Volitelná optimalizace: pokud
`dirtyBlocks` pokrývá celý soubor a `handleRefs == 1`, snapshot se dá udělat přesunem
jako dnes. Není povinná a v prvním kole se dělat nemá.

**Jeden `uploadAttempt` na flush, ne na blok.** Attempt se dál publikuje do `st.cur`
před tím, než `Flush` poprvé pustí `st.mu`. Kdyby byl attempt na blok, `awaitUpload` musí
čekat na množinu, otrava stavu se rozdrobí na „některé bloky selhaly" a `Fsync` už nemá
jednu věc, na kterou počkat. `att.err` = první chyba kteréhokoli bloku; ostatní se logují.

### D5 — atomicita commitu

**Verdikt: N uploadů, které musí *všechny* uspět → jedna transakce, která vymění sadu
bloků → teprve pak mazání starých klíčů.**

Dnešní pravidlo „jeden flush = jeden upload = jeden `CommitInode`, starý klíč se maže až
po commitu" se přeformuluje na N klíčů. Pět pravidel, která to drží:

1. **Všech N uploadů musí být hotových, než se otevře transakce.** Commitnutý řádek
   bloku, jehož objekt neexistuje, je nečitelný soubor bez chybové cesty — GET vrátí 404
   v okamžiku čtení, tedy třeba za měsíc.

2. **Staré klíče se čtou uvnitř transakce**, ne ze snapshotu pořízeného před
   `awaitUpload`. To je doslova dnešní pravidlo pro `oldKey` (`handle.go:1033-1049`)
   a samoodvozené účetnictví volumes v `CommitInode` (`db.go:396-414`), obojí zobecněné
   na N klíčů. Snapshot zvenčí znamená, že poražený flush smaže objekt, který vítěz právě
   commitnul — dnes jednou, pod bloky N-krát.

3. **Transakce dělá čtyři věci najednou:** upsert nových bloků,
   `DELETE FROM blocks WHERE inode_id=? AND block_index > ?` (zmenšení souboru),
   `UPDATE inodes SET size/status/mtime_ns`, a **dekrement volume, který inode dosud
   referencoval**. To poslední se nesmí vynechat: přechod needle → bloky je běžný (soubor
   přeroste 256 KB) a je to přesně situace, kterou dnešní `CommitInode` řeší tím, že si
   `vol_s3_key` přečte uvnitř své vlastní transakce.

4. **Mazání až po commitu, nikdy před.** Pád mezi commitem a mazáním nechá osiřelé
   objekty — to je práce pro GC a jeho 10minutová grace pokrývá in-flight okno.

5. **Částečné selhání uploadu necommituje nic.** Alternativa (commitnout bloky, které
   prošly) dá soubor napůl starý a napůl nový. POSIX to na reálném disku po výpadku
   napájení připouští, ale hamstor flushuje na `close()` a uživatel od toho čeká
   všechno-nebo-nic. Hlavně ale: je to tiché poškození, tedy přesně to, čemu se volba
   „bloky, ne slices" vyhýbala. **Zavrženo.**

Podpisy (implementováno v Kroku 2, `internal/db/db.go`):

```go
func (d *DB) CommitBlocks(id int64, blocks []BlockCommit, size int64) (
    committed bool, orphaned []string, err error)
func (d *DB) BlocksForInode(id int64) ([]BlockCommit, error)   // ORDER BY block_index
func (d *DB) DeleteBlocksForInode(id int64) ([]string, error)  // klíče, pak řádky
```

`orphaned` jsou klíče přečtené uvnitř transakce, které volající smaže **až po** návratu.
`committed` má stejný význam jako dnes: `false` znamená, že inode mezitím zmizel, a
volající musí uklidit, co nahrál (`handle.go:1191-1201`).

**`orphaned` není „všechny staré klíče".** Ta zkratka je v červené třídě: `blocks` obsahuje
jen **špinavé** bloky (D4), takže nedotčený blok uvnitř souboru v tom seznamu být nesmí —
vrátit celou předchozí sadu znamená smazat živá data souboru, který se právě korektně
zapsal. Množina je přesně:

- klíč na indexu, který je přepisován **jiným** klíčem (přepis stejným klíčem není osiření),
- klíče bloků, které uřízl `DELETE ... WHERE block_index > ?` (zmenšení souboru),
- starý `inodes.s3_key`, viz níže.

A naopak **`vol_s3_key` v seznamu nikdy být nesmí**: volume objekt sdílí víc needles a
smazat ho smí jedině GC fáze 3 podle `live_count`. Dekrementuje se, nemaže.

**Právě proto, že `CommitBlocks` zahodí předchozí úložiště vcelku, musí flush, který inode
na bloky teprve převádí, nahrát celý soubor** — ne jen špinavé bloky. To je nález Kroku 3
a je to jediná věc v celé řadě, kde se z „nahraj jen změněné" stane tichá ztráta zbytku
souboru. Detail a podmínka jsou u Kroku 3 v sekci 5.

**Přechodné období Kroků 2–3: `CommitBlocks` musí vynulovat `inodes.s3_key`** (a vrátit ho
v `orphaned`). Dokument to jinde neuvádí, protože od Kroku 4 sloupec neexistuje — jenže do
té doby existuje a čtecí cesta z něj čte staré soubory. Kdyby ho commit nechal být, přepis
starého celofilového souboru na bloky by dál servíroval **předchozí verzi dat**, tiše,
a `AllS3KeySet` by ten mrtvý objekt chránil navždy. Je to stejná třída chyby jako Č1, jen
o jeden sloupec vedle.

Blok s indexem **za** novým koncem souboru je chyba volajícího a `CommitBlocks` ho odmítá
errorem: upsertnout řádek, který tatáž transakce vzápětí uřízne, nechá v bucketu objekt,
který nikdy nic nereferencovalo. Krok 3 na to narazí legitimně — `truncate` dolů mezi
snapshotem špinavých bloků a `Flush`em — a musí snapshot proti nové velikosti ořezat sám,
ne to poslat do DB a doufat.

**Co se pádem zlepší:** pád mezi uploady a commitem nechá N osiřelých objektů, DB beze
změny a soubor v předchozí verzi. Dnes pád uprostřed flushe ztratí celý zápis. Bloková
verze je tedy odolnější, ne křehčí — což je proti intuici a stojí za to říct nahlas.

### D6 — šifrování per blok

**Verdikt: v prvním kole, protože to není volba.**

Jakmile je soubor N objektů, každý objekt **musí** být samostatně dešifrovatelný — jinak
je soubor nečitelný. Neexistuje varianta „odložit". A práce je nulová: `crypto.Encrypt`
už dnes emituje `[version][nonce][ct+tag]` na každé volání (`crypto.go:57-67`), takže
volat ho per blok *je* per-blok nonce. Žádná změna formátu.

Důsledky:

- **Objekt bloku = `blockSize + 29 B`** (1 B verze + 12 B nonce + 16 B GCM tag). To je
  důvod, proč `PartSize` = 2 × blok a ne blok + 1.
- **`blocks.size` ukládá plaintext délku**, ne délku objektu. Délka objektu je
  odvoditelná a nikdo ji nepotřebuje.
- **Dokumentované omezení „no range reads under encryption" padá na granularitu bloku.**
  Streaming (`--stream-rate`) půjde zapnout i pod šifrováním — dnes je vypnutý
  (`node.go:428`), protože byte-range řez celofilovým GCM blobem je nedešifrovatelný
  ciphertext. Blok je jednotka dešifrování, takže 4KB čtení šifrovaného souboru stáhne
  a dešifruje jeden 8MiB blok — stejná granularita, jakou dnes dostává nešifrovaná cesta.

**Co se odkládá, ale formát na to nechává místo: HKDF per-block subkey.** Dnešní jeden
AES-256-GCM klíč s náhodnými 96bitovými nonce má birthday bound kolem 2^32 zpráv
(deferovaná položka z auditu 2026-06-04). Pod bloky roste počet zpráv řádově — 4 TB =
524k bloků, plus nová sada při každém přepisu. Pořád hluboko pod hranicí, ale trend je
špatný. Čisté řešení je odvodit klíč bloku jako `HKDF(master, blockS3Key)`: klíč bloku je
čerstvé UUID pro každou verzi, takže opakování nonce napříč bloky je strukturálně nemožné.

Dodělat to jde **bez druhé změny formátu**, protože verze je už dnes první bajt a
`crypto.IsEncrypted` ji čte. Rezervuje se **version byte `0x02`** pro „klíč odvozený
HKDF z klíče objektu"; `Decrypt` větví na verzi, `Encrypt` píše to, co je nakonfigurované.
Pokud se to nakonec udělá hned v prvním kole, tím líp — ale není to podmínka.

---

## 2. Schéma a formát klíče

```sql
CREATE TABLE blocks (
    inode_id     INTEGER NOT NULL REFERENCES inodes(id) ON DELETE CASCADE,
    block_index  INTEGER NOT NULL,
    s3_key       TEXT    NOT NULL,
    size         INTEGER NOT NULL,   -- plaintext délka ULOŽENÉHO objektu
    PRIMARY KEY (inode_id, block_index)
) WITHOUT ROWID;
```

**Ruší se:** sloupec `inodes.s3_key`, `db.UpdateS3Key`, `db.AllS3Keys`, celý
`internal/ops/migrate.go`. Sloupce `vol_s3_key` / `vol_offset` / `vol_size` a tabulka
`volumes` zůstávají beze změny — needles se nemění.

### Aritmetika bloků

`blockSize` je od Kroku 2 konstanta **`db.BlockSize`** (`internal/db/db.go`). Bydlí v `db`,
protože `CommitBlocks` dostává podle D5 jen novou velikost souboru a index posledního
živého bloku si musí odvodit sám; `hfuse` `db` stejně importuje. `s3store` ji naopak
importovat nesmí (obrácený směr závislosti), takže `partsize_test.go` má vlastní literál
s odkazem na D1 — jediná úmyslná duplikace té hodnoty.

- `blockIndex(off) = off / blockSize`
- blok `b` pokrývá plaintext `[b*blockSize, min((b+1)*blockSize, inodes.size))`
- chybějící řádek = díra = nuly, nefetchuje se

### `blocks.size` není délka souboru

`blocks.size` je **kolik plaintextových bajtů bloku patří do souboru**. Za tou hranicí
blok čte nuly a uložený objekt klidně může být delší: zmenšení, které řízne doprostřed
bloku, objekt nepřepisuje, jen zkrátí tohle číslo. **Odvozovat délku souboru ze
`SUM(blocks.size)` je nová verze staré chyby.** `inodes.size` (a za běhu `inodeWrite.size`)
je jediná autorita.

> **Opraveno v Kroku 5.** Původně tu stálo, že `blocks.size` je *délka uloženého objektu*
> a že rozdíl proti živému rozsahu řeší clamp v `readLoaded`. To je pravda jen dokud soubor
> zůstane krátký. Jakmile se zvětší zpátky, clamp na `inodes.size` ty bajty zase pustí
> dovnitř a servíruje se starý ocas — a je to **přesně případ, který dokument sám naměřil**
> (1 048 560 B na 1MiB souboru). Mazání bloků za koncem, které Krok 5 předepisuje, na to
> nesahá: 1MiB soubor má jediný řádek, takže za koncem není co smazat. Řeší to
> `clampLastBlockSize` v `db`, volaná z `CommitBlocks` i `SetAttr`, a dvojitý clamp ve
> `fetchBlock`/`faultBlock`.

**Nezaměňovat se dvěma věcmi, které se sem podobají:**

- *Poslední* blok, jehož uložený objekt je delší než jeho živý rozsah, je v pořádku a
  řeší ho clamp. To je tenhle odstavec.
- Blok **celý za** koncem souboru v pořádku není. Ten se musí smazat — clamp ho sice
  nezobrazí, ale příští zvětšení souboru by ho vzkřísilo se starým obsahem místo nul.
  `CommitBlocks` to dělá (`DELETE ... WHERE block_index > ?`); bezhandlová cesta
  `truncate(2)` → `SetAttr` ne, a je to jediná díra, kterou po Kroku 3 zbývá zalepit —
  viz Krok 5.

Naopak `ftruncate` **nahoru** nesmí nic materializovat: zvětšení na 5 TB je jen
`UPDATE inodes SET size`, žádné bloky nul.

### Formát klíče

`s3store.NewKey()` beze změny: `{2hex}/{uuid}`. Blok tedy v bucketu není odlišitelný od
volume ani od čehokoli jiného — **a to je záměr.** Disk cache, `evictLRU` (skenuje přesně
2 úrovně, `cache.go:360-371`) i GC pak zacházejí se všemi objekty jednotně a nepotřebují
znát typ.

*Protiargument:* při ruční inspekci bucketu nejde poznat blok od volume, což ztěžuje
debug. *Proč prohrál:* tu informaci má DB a `hamstor fsck` ji umí vypsat; zaplatit za
pohodlí při debugu tím, že každá komponenta parsuje typ z klíče, je špatný obchod —
a prefixované klíče už jednou migraci stály (`ops/migrate.go` existuje přesně proto).

### `ON DELETE CASCADE`

`foreign_keys(1)` je zapnuté (`db.go:104`) a `xattrs` už cascade používá. Pro `blocks` je
to správně, ale **jen ve směru „přečti klíče → smaž řádek inodu → smaž objekty".** Opačné
pořadí (smazat inode a pak hledat, co mazat v S3) klíče nenávratně ztratí a objekty
zůstanou v bucketu jako osiřelé navždy — GC je sice uklidí, ale až podle listingu, a jen
pokud jeho množina klíčů je správná. Viz sekce 3.

---

## 3. Inventura call sites

### 3.1 ZTRÁTA DAT, POKUD SE PŘEHLÉDNE

Deset míst. Osm z nich má strukturu „porovnej DB s realitou a smaž rozdíl", což je přesně
ta třída, kterou zadání nechalo označit červeně.

---

**Č1 — `ops.GC` fáze 1, `internal/ops/gc.go:27`. Nejnebezpečnější řádek celé změny.**

```go
knownKeys, err := database.AllS3KeySet()   // gc.go:27
// ...
if _, ok := knownKeys[obj.Key]; ok { continue }   // gc.go:58
// ...
deleted, err := store.DeleteBatch(ctx, orphanKeys)   // gc.go:75
```

`AllS3KeySet` (`db.go:525-558`) dnes sjednocuje `inodes.s3_key` a `volumes.s3_key`.
**Když v té množině nebudou klíče bloků, první `hamstor gc` smaže každý blokový objekt
v bucketu jako osiřelý.** Totální ztráta všech velkých souborů, dávkově, jedním
`DeleteObjects`. Grace period 10 minut nepomůže — chrání jen čerstvě nahrané objekty.

Oprava je jeden `SELECT s3_key FROM blocks` navíc. Riziko není v obtížnosti, ale v tom,
že to je jediné místo, kde selhání není hlučné.

> **Tohle místo nemělo test.** `internal/ops/gc_test.go` mělo dvě funkce
> (`TestGCOrphanedInodes`, `TestGCOrphanedInodesDryRun`) a obě testují **fázi 2**
> (osiřelé inody v DB). Porovnání množiny klíčů proti listingu bucketu — fáze 1 —
> netestovalo nic.
>
> ✅ Vyřešeno Krokem 2 (`b24ef8e`), dřív než vznikl první blok:
> `TestGCPhase1KeepsBlockObjects` (`internal/ops`, skutečný `GC` proti bucketu) plus
> `TestAllS3KeySetIncludesBlocks` (`internal/db`, bez S3, tedy neskipnutelný). Proč
> naivní verze toho testu projde i s rozbitým `AllS3KeySet` — a co s tím — je
> v sekci 5, Krok 2.

---

**Č2 — `hfuse.RecoverPending`, `internal/hfuse/cleanup.go:91-175`. Druhé nejnebezpečnější.**

Formát zadržených bajtů je `<inodeID>.<logicalSize>` — **jeden soubor = jeden objekt =
celý soubor** (`fs.go:120`, `cleanup.go:111-123`). Beze změny by recovery vzala zadržené
bajty, nahrála je jako **jeden** objekt (`cleanup.go:152`) a commitla
(`cleanup.go:160`) — tedy vyrobila inode, jehož data leží v celofilovém objektu, který
bloková read path neumí najít. Soubor je nečitelný a původní bajty jsou smazané
(`cleanup.go:166`). Tiše.

Návrh formátu: adresář místo souboru.

```
<db-dir>/pending/<inodeID>/
    meta          # fileSize, mtime_ns, seznam indexů
    <blockIndex>  # bajty přesně tak, jak měly jít do objektu
```

`RecoverPending` pak musí nahrát **celou sadu a commitnout ji jednou transakcí**
(`CommitBlocks`), jinak vyrobí přesně ten napůl starý soubor, který D5 zakazuje. Selhání
kteréhokoli bloku = nechat celý adresář na místě a zkusit příští boot.

Detail, na který se snadno zapomene: `hasRetainedData` (`cleanup.go:63-77`) globuje
`<id>.*` a přeskakuje `.tmp`. Adresář `<id>` tomu vzoru **nevyhoví** — glob musí pokrýt
i holé `<id>`, jinak `Cleanup` (`cleanup.go:34`) přestane pending inody chránit a smaže
je i se zadrženými daty. To je vazba, kterou CLAUDE.md popisuje jako jedno ze tří
pravidel, jejichž porušení vrací tichou ztrátu.

> Zjednodušující invariant, který tu platí a stojí za to pojmenovat: **zadržená sada se
> vždycky týká inodu, který nikdy nebyl commitnutý.** `RecoverPending` na `cleanup.go:131`
> zahazuje zadržená data inodu se stavem `committed`. Přepis existujícího souboru, jehož
> upload selže, tedy nekončí obnovou — končí návratem k předchozí verzi. Merge částečné
> sady na existující bloky proto nikdy nenastane, což je přesně to, co drží recovery mimo
> nebezpečí z bodu 5 v D5.

---

**Č3 — `ops.GC` fáze 2, `internal/ops/gc.go:90-106`.** Zrcadlový bug k Č1: sbírá se jen
`meta.S3Key`, takže blokové objekty osiřelého inodu nikdo nesmaže. Není to ztráta, je to
trvalý leak — a protože `DeleteInodeWithVolume` (`gc.go:111`) přes cascade smaže i řádky
v `blocks`, jejich klíče už potom nikdo nezná. Objekty pak uklidí až fáze 1 při dalším
běhu, což je v pořádku **jen když je fáze 1 opravená podle Č1**. Dva bugy, které se
navzájem maskují.

---

**Č4 — `db.DeleteInode` (`db.go:430`) a `db.DeleteInodeWithVolume` (`db.go:643`).**
Ani jedna nezná bloky. S cascade řádky zmizí, objekty ne. Obě potřebují buď vracet
smazané klíče, nebo volajícího donutit je přečíst předem. Doporučení: **vracet je** —
volající, kteří na to zapomenou, jsou pak build error, ne runtime leak.

---

**Č5 — `node.Unlink` (`node.go:495-505`), `deleteTree` (`node.go:545-566`),
`Rename` přes existující cíl (`node.go:684-696`).** Všechny tři mají identickou strukturu:

```go
if meta.S3Key != "" {
    n.hfs.Cache.Evict(meta.S3Key)
    n.hfs.Store.Delete(ctx, meta.S3Key)
}
```

Všechny tři musí mazat sadu bloků, a všechny tři musí evictovat sadu z cache. Zapomenutý
`Evict` není leak v S3, ale zabraná disk cache, kterou uklidí až LRU.

> `internal/hfuse/rmdir_test.go:44` sahá přímo na `c.S3Key` a asertuje, že objekt zmizel.
> Po smazání pole spadne kompilace — **to je dobře**, test se přepíše na sadu bloků
> a zůstane strážcem. Totéž `rmdir_test.go:106`.

---

**Č6 — `hfuse.Cleanup`, `internal/hfuse/cleanup.go:49-53`.** Maže `meta.S3Key` u pending
inodů, které nemají zadržená data. Musí mazat bloky. Leak, ne ztráta — ale je to poslední
místo, kde ty klíče někdo zná.

---

**Č7 — `db.Fsck` (`db.go:928-931`) a `hfuse.CheckStagedData` (`cleanup.go:190`).**

```sql
WHERE status = 'committed' AND (s3_key IS NULL OR s3_key = '')
  AND (vol_s3_key IS NULL OR vol_s3_key = '') AND ...
```

Tenhle predikát znamená „commitnutý soubor, jehož data nejsou v S3". Pod bloky mu vyhoví
**každý velký soubor** — jeho `s3_key` neexistuje a `vol_s3_key` je prázdný, data jsou
v `blocks`. Není to ztráta dat, ale:

- `main.go:242` vypíše při **každém bootu** `WARNING: N committed file(s) have no data
  in S3` a vyjmenuje prvních deset;
- `hamstor fsck` skončí exit code 1 s hlášením „unreadable files";
- a tím je zabitý signál pro případ, kdy je to pravda — což je scénář „DB obnovená
  z Litestreamu na stroj bez původního staging disku", tedy skutečná tichá ztráta, kvůli
  které ta kontrola vznikla.

Predikát musí být „nemá needle **a** nemá žádný řádek v `blocks`".

---

**Č8 — `hfuse.readStaged`, `internal/hfuse/writestate.go:144`. Skutečný bug na horké cestě.**

```go
if meta.VolS3Key != "" || meta.S3Key != "" {
    return nil, 0, 0 // no longer staged; caller re-reads and reloads
}
```

Test „už není staged" se pod bloky rozbije: přepis, který staged soubor přeroste přes
`MaxNeedleSize`, commitne **bloky**, takže `VolS3Key` i (neexistující) `S3Key` zůstanou
prázdné. `readStaged` tedy usoudí, že soubor je pořád staged, nenajde staging soubor
(přepis ho odstranil), protočí všech 5 pokusů (`stagedReadAttempts`) a vrátí `EIO` pro
data, která jsou v pořádku uložená v blocích.

Je to na horké cestě: `ensureLoaded` (`handle.go:140`) i write preload v `Open`
(`node.go:450`) sdílejí tuhle funkci. Stejný predikát je v `CleanupStagingDir`
(`cleanup.go:282`, `:305`) a ve `volume.Builder.FlushInode` (`builder.go:203`, `:213`,
`:271`) — tam je dnes neškodný, protože staged soubor je z definice ≤ 256 KB, ale je to
stejná chyba čekající na posun prahu.

---

**Č9 — `ops.Migrate`, `internal/ops/migrate.go:13`. Smazat celý soubor, ne opravit.**

Migruje nestandardní klíče v `inodes.s3_key` na prefixovaný tvar. Po zrušení sloupce
buď tiše neudělá nic, nebo — kdyby někdo „pro pořádek" rozšířil `AllS3Keys` o bloky —
zavolá `Copy` + `UpdateS3Key` (`migrate.go:37`), tedy funkci, která umí updatovat jen
`inodes`. Výsledek: objekt zkopírovaný na nový klíč, `blocks` ukazující na starý, starý
smazaný (`migrate.go:42`). Ztráta bloku.

Legacy neprefixované klíče v tomhle bucketu po purge+reinit nebudou existovat. Příkaz
`migrate` se má smazat i z `main.go` a z README.

---

**Č10 — `ops.Compact`: prověřeno, NEDOTČENO.** Uvádím explicitně, protože patří do stejné
třídy „porovnává DB s realitou a maže". `compactVolume` pracuje výhradně nad `volumes`
a `vol_s3_key` (`compact.go:60` `NeedlesInVolume`, `compact.go:115`
`CommitNeedlesToVolume` s `expectedVolKey`), bloky nikdy nevidí.

**Bloky se nekompaktují a nepotřebují to.** Tabulka `volumes` a její `live_count`/
`live_size` existují jen proto, že mnoho needles sdílí jeden objekt a vzniká v něm mrtvé
místo. Jeden blok = jeden objekt = jeden vlastník; při přepsání se starý objekt smaže
celý. **`blocks` proto nemá žádnou obdobu `live_count` a nesmí ji dostat** — bylo by to
účetnictví bez čeho účtovat, tedy jen další věc, co se může rozejít.

---

### 3.2 Zbytek inventury

Sloupec „bloky rozbijí" rozlišuje: **ANO** = po změně nefunguje nebo je nebezpečné,
**PŘEPSAT** = funguje, ale je to mrtvý/zbytečný kód, **NE** = beze změny.

#### `internal/db/db.go`

| místo | co předpokládá | rozbije |
|---|---|---|
| `:30` `s3_key TEXT` ve schématu | soubor má jeden klíč | ANO — sloupec se ruší |
| `:68` `inodeCols` | `s3_key` je součást každého čtení inodu | ANO |
| `:76` `InodeMeta.S3Key` | totéž na úrovni typu | ANO — **smazat, ať compiler najde zbytek** |
| `:246-265` `scanInode` | skenuje `s3key sql.NullString` | ANO |
| `:372-428` `CommitInode` | jeden klíč, jedna velikost | ANO — nahradit `CommitBlocks` (D5) |
| `:430` `DeleteInode` | inode nemá vlastní objekty | ANO — Č4 |
| `:457-469` `GetStagedInodes` | „bez klíče ⇒ staged" | ANO — Č7 |
| `:507` `AllS3Keys` | jeden klíč na inode | ANO — ruší se s `migrate` |
| `:525-558` `AllS3KeySet` | inodes ∪ volumes = vše | **ANO — Č1**; ✅ hotovo v Kroku 2 |
| `:643` `DeleteInodeWithVolume` | maže jen needle účetnictví | ANO — Č4 |
| `:758` `UpdateS3Key` | klíč je updatovatelné pole inodu | ANO — ruší se |
| `:780-803` `SetAttr` | zmenšení = jen změna čísla | ANO — musí mazat bloky za koncem; **Krok 5**, měřeno níž |
| `:912-963` `Fsck` | predikát „nemá data" | ANO — Č7 |
| `:690-748` `CommitNeedlesToVolume` | needles | NE |
| `:591` `GetEmptyVolumes`, `:600` `GetVolumesForCompaction` | volumes | NE |

#### `internal/hfuse/handle.go`

| místo | co předpokládá | rozbije |
|---|---|---|
| `:44` `HamstorHandle.s3Key` | otevírací snapshot = jeden klíč | ANO — viz „reader coherence" v sekci 4 |
| `:46-54` `fileSize` | páruje se s `s3Key` | NE (zůstává jako mez), ale rozpojí se od klíče |
| `:96-213` `ensureLoaded` | načte **celý** soubor | ANO — jádro změny, krok 5 |
| `:163-169` `S3Key == ""` ⇒ prázdný | absence klíče = prázdný soubor | ANO — Č7/Č8 tvarem |
| `:184` `Store.Download(meta.S3Key)` | celý soubor jedním GETem | ANO — a je to místo, kde bije CRC32 bug |
| `:226-247` `loadFromVolume` | needle | NE |
| `:252-317` `readNeedle`/`fetchVolume` | volobj cache | NE |
| `:319-354` `Read` | větvení podle `s3Key` + `Cache.Has` | ANO — podmínka `:338` odpadá (D3) |
| `:357-396` `readChunked` | 2MB chunky uvnitř objektu | PŘEPSAT na bloky |
| `:400-427` `getOrFetchChunk` | `DownloadRange` do jednoho objektu | PŘEPSAT |
| `:431-489` `prefetchChunks` | `readAheadChunks = 3` × 2 MB = 6 MB | PŘEPSAT + **přeladit** (3 bloky = 24 MiB) |
| `:491-565` `readLoaded` | čte ze souvislého bufferu | ANO — musí faultovat chybějící blok |
| `:570-635` `readStreaming` | ring buffer 2MB chunků | PŘEPSAT + **přeladit** (viz níže) |
| `:638-655` `fetchStreamChunk` | `DownloadRange(h.s3Key)` | PŘEPSAT |
| `:682-701` `spillToDisk` | spill = celý soubor | ANO — spill je nově řídký |
| `:703-800` `Write` | souvislý buffer, `logicalSize` | částečně — přidat značení `dirtyBlocks` |
| `:802-900` `Flush` | jeden upload, předání spill filu | ANO — D4 snapshot, D5 commit |
| `:906-1019` `flushStaged` | needle cesta | NE (jen `:914` mazání starého klíče → Č5) |
| `:1027-1241` `flushAsync` | jeden objekt, jeden `oldKey` | ANO — jádro D5 |
| `:1049` `oldKey := meta.S3Key` | jeden předchůdce | ANO — → `orphaned` z transakce |
| `:1159` `retainPendingUpload` | jedny bajty = celý soubor | ANO — Č2 |
| `:1227` `cacheUploaded` | jeden objekt do cache | ANO — per blok, viz níže |
| `:1243-1307` `Fsync` | čeká na jeden attempt | NE (jeden attempt na flush, D4) |

#### `internal/hfuse/node.go`

| místo | co předpokládá | rozbije |
|---|---|---|
| `:256`, `:298` `handle.s3Key = meta.S3Key` | otevírací snapshot | ANO |
| `:303-304` `hasData` | „má klíč nebo volume nebo je staged" | ANO |
| `:309-330` větev `O_TRUNC` | vyprázdnit buffer = vyprázdnit soubor | ANO — musí smazat i sadu bloků při flushi |
| `:332-361` write preload z `S3Key` | stáhne celý objekt | ANO — krok 5 to ruší (a s ním CRC32 bug) |
| `:363-377` spill velkého preloadu | preload = celý soubor | ANO |
| `:379-393` preload z volume | needle | NE |
| `:409-420` clamp na `meta.Size` | souvislý buffer | ANO — viz sekce 4 |
| `:428-437` streaming jen bez šifrování | celofilový GCM blob | ANO — **omezení padá** (D6) |
| `:433` `streamChunksCap` | jednotka = `cache.ChunkSize` | ANO — přeladit |
| `:449-477` `openPreloadStaged` | needle/staging | NE (kromě Č8) |
| `:495-511` `Unlink` | jeden klíč | **ANO — Č5** |
| `:533-571` `deleteTree` | jeden klíč na dítě | **ANO — Č5** |
| `:604-705` `Rename` (cíl existuje) | jeden klíč | **ANO — Č5** |
| `:96-151` `Setattr` | truncate = změna čísla + bufferu | ANO — mazání bloků za koncem |

#### `internal/hfuse/{fs,writestate,cleanup}.go`

| místo | co předpokládá | rozbije |
|---|---|---|
| `fs.go:116-143` `retainPendingUpload` | `<id>.<size>`, jedny bajty | **ANO — Č2** |
| `fs.go:145-203` `cacheUploaded` + `maxCacheShare` | jeden objekt na flush | ANO — viz níže |
| `writestate.go:56-116` `inodeWrite` | `loaded bool`, jeden buffer | ANO — D4 |
| `writestate.go:136-184` `readStaged` | predikát „už není staged" | **ANO — Č8** |
| `writestate.go:189-229` `truncateWriteState` | zkrátit buffer stačí | ANO — musí značit bloky |
| `writestate.go:241-267` `awaitUpload` | jeden attempt | NE |
| `cleanup.go:23-59` `Cleanup` | jeden klíč u pending | ANO — Č6 |
| `cleanup.go:63-77` `hasRetainedData` | glob `<id>.*` | **ANO — Č2** |
| `cleanup.go:91-175` `RecoverPending` | jeden soubor = celý objekt | **ANO — Č2** |
| `cleanup.go:190-203` `CheckStagedData` | predikát z `GetStagedInodes` | ANO — Č7 |
| `cleanup.go:207-241` `CleanupVolumes` | volumes | NE |
| `cleanup.go:246-315` `CleanupStagingDir` | `S3Key != "" \|\| VolS3Key != ""` | ANO — Č8 tvarem |

#### `internal/cache/cache.go`, `internal/s3store/s3.go`, `internal/volume/builder.go`

| místo | co předpokládá | rozbije |
|---|---|---|
| `cache.go:87-92` `Has` (guard na adresář) | existují chunk directories | PŘEPSAT — guard odpadá s chunky |
| `cache.go:104-119` `Open` (týž guard) | totéž | PŘEPSAT |
| `cache.go:281-337` chunk API | 2MB chunky uvnitř objektu | PŘEPSAT — celé se ruší (D3) |
| `cache.go:350-435` `evictLRU` | klíč = 2 úrovně `prefix/uuid` | NE pro bloky — ale viz nález níže |
| `s3.go:23` `MaxDownloadSize` | strop na velikost souboru | PŘEPSAT — nově sanity check na blok |
| `s3.go:59` `manager.NewUploader(client)` | default `PartSize` 5 MiB | **ANO — D1** |
| `s3.go:128-151` `Download` | stahuje celý objekt | NE — nově jen bloky/volumes |
| `s3.go:158-185` `DownloadRange` | needles | NE |
| `builder.go:203/213/271` predikáty „už má úložiště" | `S3Key \|\| VolS3Key` | ANO — Č8 tvarem |
| `builder.go:177-184` `cacheVolume` | volume do cache | NE |

### 3.3 Incidenční nálezy

Dvě věci, které jsem našel při inventuře a které tenhle návrh nezpůsobil — ale zhorší.

**`cache.evictLRU` eviktuje `volobj/` po celých 1/256 najednou** (`cache.go:360-405`).
Skener předpokládá klíč o dvou úrovních (`prefix/uuid`): přečte `c.dir`, každou položku
bere jako prefix a její obsah jako klíče. Pro `volobj/a1/uuid` tedy vezme `volobj` jako
prefix, `a1` jako **jeden záznam**, uvidí adresář, sečte celý podstrom a při evikci ho
smaže celý — tedy ~1/256 všech cachovaných volumes najednou. Existující chyba (od commitu
`2d5bbde`), ale bloky cache vytíží podstatně víc, takže začne být vidět.

**`readStreaming` ring buffer nesnese blokovou jednotku bez přeladění.** Dnes:
`streamChunksCap = StreamBuffer*MiB / cache.ChunkSize` (`node.go:433`), s podlahou 4
(`node.go:434-436`). Při defaultu `--stream-buffer 16` to je 8 chunků = 16 MiB. Když se
jednotka změní z 2 MiB na 8 MiB, výpočet dá 2, podlaha to zvedne na 4, a **ring drží
32 MiB v RAM na otevřený handle** — proti `debug.SetMemoryLimit(150<<20)`. Stejně tak
`readAheadChunks = 3` (`handle.go:25`) je dnes 6 MB prefetche a stalo by se z něj 24 MiB.

Doporučení: **in-memory ring při přechodu na bloky zrušit.** Existuje proto, že chunky
ve streaming módu se schválně necachovaly na disk; blok se na disk cachuje jako každý
jiný objekt, takže ring řeší problém, který přestane existovat. Podlahu i `readAheadChunks`
pak nahradí jedna hodnota „kolik bloků dopředu", laděná v blocích.

---

## 4. Přeformulované invarianty

Sekce „Key Patterns" v CLAUDE.md, položka po položce, jak zní pod vícebjektovým zápisem.
Nic z toho se neruší — všechno se zobecňuje.

### Sdílený `inodeWrite` a jeho čtyři pravidla

**1. `writeMu` je leaf lock.** *Beze změny.* Nové mapy (`dirtyBlocks`, `presentBlocks`)
žijí uvnitř `inodeWrite` pod `st.mu`. `writeMu` dál kryje jen mapu stavů a refcounty a
`acquireWrite`/`tryAcquireWrite` se vracejí s ním uvolněným. `Flush` dál bumpne
`uploadRefs` pod `st.mu` bez inverze.

**2. Upload goroutina nikdy nebere `st.mu`.** *Beze změny významu, změna mechanismu.*
Dnes to drží tím, že goroutina dostane spill file do vlastnictví. Pod bloky je spill file
živá lokální materializace, ze které se dál čte a píše, takže se odevzdat nedá — a číst
ji souběžně by znamenalo nahrát směs dvou verzí bloku. Nově tedy `Flush` **pod `st.mu`
zkopíruje špinavé bloky do snapshot souboru** a goroutině předá ten. Goroutina zase sahá
výhradně na data, která nikdo jiný nezná. `uploadAttempt` zůstává neměnný po publikaci
(`err` se píše jen před `close(done)`) — a zůstává **jeden na flush**, ne na blok.

> **Stav po Kroku 3:** kopie zavedená není a zatím být nemusí. Dokud je čtení hloupé
> (`loaded = false` po flushi, příští přístup načítá znovu z bloků), spill file živá
> materializace *není* a odevzdání vlastnictví drží pravidlo stejně jako dnes. Goroutina
> tedy sahá na soubor, který stav pustil z ruky.
>
> **Rozhodnutí Kroku 5: kopie se nezavádí, protože se nepřebírá premisa.** Požadavek D4 je
> podmíněný — platí *„jakmile spill file přežívá flush jako řídká materializace"*. Krok 5
> tu podmínku nesplnil schválně: `Flush` dál odevzdá (řídký) spill file goroutině a stav se
> resetuje (`loaded=false`, `presentBlocks=nil`, `blockBacked=false`), takže goroutina zase
> sahá na data, ke kterým se nikdo jiný nedostane, a pravidlo drží **týmž ověřeným
> mechanismem**, ne novým. Důvod je robustnost: kopie by přidala dva invarianty, jejichž
> porušení je tiché — „kopie špinavých bloků je úplná" (nahraje se blok s nulami) a
> „`presentBlocks` po commitu pořád popisuje pravdu o bajtech, jejichž klíče se právě
> vyměnily". Doporučená optimalizace „přesun místo kopie" by navíc znamenala dvě cesty
> místo jedné.
>
> Cena: po `fsync` uprostřed zápisu se materializace zahodí a příští částečný zápis blok
> refaultuje — z disk cache, kterou `cacheBlock` právě naplnil. `Flush` přichází skoro vždy
> na `close()`, kdy handle stejně zaniká, takže dopad je omezený na fsync-heavy workloady.
> Kdyby se to někdy stalo měřitelným problémem, kopie z D4 je ta správná odpověď — ale až
> potom, a s testem, který ten rozdíl ukáže.
>
> Praktický důsledek pro Krok 5: uploadovaný blok se nešifrovaného mountu posílá jako
> `io.NewSectionReader(snapshot, start, extent)`, ne jako `[]byte`. Drží to dvě věci
> najednou — nulovou stopu na haldě (`UploadSem` pouští 32 uploadů proti limitu 150 MB)
> a bezalokační větev SDK z D1, která chce `ReaderAt+Seeker`. Šifrovaný blok přes haldu
> jít musí, ale je to jeden blok, ne celý soubor jako dřív.

**3. Attempt je publikován před prvním uvolněním `st.mu`; `oldKey` se čte až po čekání.**
*Platí, s N klíči.* Publikace se nemění. `oldKey` se mění na `orphaned []string` — a
zpřísňuje se: klíče se čtou **uvnitř commit transakce**, ne po `awaitUpload` mimo ni. Je
to sloučení dvou dnešních pravidel (`oldKey` až po čekání + samoodvozené účetnictví
volumes) do jednoho, protože obě řeší totéž: nedůvěřovat snapshotu, který mohl mezitím
zastarat. Rozdíl je v ceně chyby — dnes smaže poražený flush jeden živý objekt, pod bloky
až N.

**4. Selhaný attempt otráví stav.** *Platí, s upřesněním, co je „jediná kopie".*

### Otrava stavu a retence v `<db-dir>/pending/`

Zadání se ptá: co znamená „ty bajty jsou jediná kopie", když je jich N?

Znamená to **ta sada je jediná kopie, a je nedělitelná**. Tři pravidla z CLAUDE.md platí
beze změny formulace, ale musí se vztahovat na adresář místo souboru:

- `<db-dir>/pending/` se nikdy nesmí mazat při startu tak, jak se maže `spill/`;
- `Cleanup` musí přeskočit pending inody se zadrženými daty a běžet **po** `RecoverPending`;
- pokus o obnovu, který se nedostane do S3, nechává data na místě.

Nové čtvrté pravidlo, specifické pro bloky: **`RecoverPending` commituje sadu jedinou
transakcí, nebo necommituje nic.** Nahrát 7 z 8 bloků a commitnout je znamená vyrobit
soubor napůl starý a napůl nový — přesně to, co D5 zakazuje na horké cestě, jen o boot
později a bez svědka.

Co to *usnadňuje*: zadržená sada se vždycky týká inodu, který nikdy nebyl commitnutý
(`cleanup.go:131` zahazuje zadržená data u `status = 'committed'`). Sada je tedy vždy
kompletní obsah souboru, nikdy delta na existující bloky. Merge částečné sady na živý
soubor tak nemůže nastat — a nesmí se to rozbít tím, že by někdo `cleanup.go:131`
„opravil" na obnovu přepisů.

Otrava sama se nemění: pořád platí, že sourozený handle, který tiše commitne přes
zadržená data, překlopí inode na `'committed'` a `RecoverPending` je pak smaže jako
zastaralá.

### „`inodeWrite.size` je jediná autorita na délku souboru"

*Platí, a je to důležitější než dřív.* Přibyl nový, lákavý falešný zdroj:
`SUM(blocks.size)`. Ten je špatně z téhož důvodu, z jakého je špatný `HamstorHandle.fileSize`
— popisuje uložená data, ne logický soubor. Konkrétně: `truncate()` na cestě bez
otevřeného write handle zmenší `inodes.size` a poslední blok se nepřepíše, takže
`blocks.size` toho bloku zůstane větší než jeho živý rozsah. Clamp v `readLoaded`
(`handle.go:503-515`) proto nezmizí; jen se aplikuje na výsledek složený z bloků.

Rozdělení `fileSize` (per-handle snapshot) a `st.size` (sdílená autorita) zůstává. Nesmí
se slučovat, a nově se k nim nesmí přidat třetí.

### Write preload v `Open` a jeho clamp na `meta.Size`

*Platí, se změněným obsahem preloadu.* Pravidlo je, že preload se usadí pod `st.mu`:
počká na in-flight upload a **až pak** znovu přečte metadata, protože preload zároveň
načítá z úložiště *a* ořezává sdílený buffer na `meta.Size`, a rozejít ty dvě věci znamená
uříznout sourozencův zápis a příštím appendem přepsat zbytek.

Pod bloky se mění, co preload dělá: z „stáhni celý objekt" (`node.go:345`) se stane
„nefaultuj nic" — první `Write` si vyžádá blok, kterého se dotkne. Read-only otevření dál
záměrně nečeká.

Clamp zůstává, ale získává druhou polovinu: kromě ořezání bufferu na `meta.Size` platí,
že **bloky za koncem souboru se při flushi mažou, ne ořezávají** — jinak by je příští
zvětšení souboru vzkřísilo se starým obsahem místo nul.

### Samoodvozené účetnictví volumes v `CommitInode` / `DeleteInodeWithVolume`

*Platí beze změny a `CommitBlocks` ho musí převzít celé.* Pravidlo zní: přečti si
`vol_s3_key`/`vol_size` inodu **uvnitř své vlastní transakce** a dekrementuj volume tamtéž,
místo abys věřil snapshotu volajícího. Drží to dvě věci: crash nemůže nechat pořád
referencovaný volume na `live_count = 0` pro GC k smazání, a souběžný přepis, který needle
už přesunul, nezpůsobí dvojí dekrement.

Pod bloky je to **častější, ne vzácnější**: přechod needle → bloky nastane pokaždé, když
soubor přeroste 256 KB, tedy při každém růstu malého souboru. `CommitBlocks` proto musí
ve stejné transakci dekrementovat volume a vynulovat `vol_*` sloupce, jinak každý takový
růst nechá volume nafouklý o mrtvý needle, který nikdy nikdo neodečte.

Opačným směrem žádná obdoba nevzniká: `blocks` účetnictví nemá a mít nesmí (viz Č10).

### Reader coherence — jediné pravidlo, které se skutečně mění

CLAUDE.md dnes dokumentuje: *„A read-only handle that opened before a writer keeps reading
its own snapshot of the file; it does not see the writer's changes until it reopens."*
Implementačně to drží `h.s3Key` — otevírací snapshot klíče.

Pod bloky by ekvivalentní snapshot znamenal načíst při otevření celou mapu bloků: pro 4TB
soubor 524 288 řádků. To je nepřijatelné, takže **lookup `(inode, block_index) → klíč`
bude líný, jeden řádek na fault** — a read-only handle tím **začne vidět zápisy, jak
commitují**.

Je to změna směrem k POSIXu (tam se čtení a zápis na sdíleném souboru vidí), takže není
špatná. Ale je to změna dokumentovaného chování a musí se přepsat v CLAUDE.md, ne
propašovat. Zbytek téže položky (*„Streaming/chunked reads deliberately bypass the shared
buffer"*) zůstává v platnosti — nově proto, že blok je jednotka obojího.

---

## 5. Pořadí implementačních kroků

Krok 0 plus sedm. Každý samostatně buildí, projde `go vet ./...` i `source .env.test && go test ./...`,
a **nenechává filesystem v nekonzistentním stavu** — tzn. po kterémkoli z nich lze mount
zastavit, nasadit a zase spustit.

Kroky 1–2 jsou záměrně před vším ostatním: **připravují obranu dřív, než vznikne, co
bránit.**

---

**Krok 0 — zneškodnit CRC32 minu. NEČEKÁ na nic z tohohle dokumentu.**
> ✅ **HOTOVO 2026-07-21, commit `535686a`.** Regresní test
> `internal/s3store/checksum_test.go` (ověřeno, že na staré hodnotě padá).
> Nasazení: `make build && sudo make install` — produkce běží na `9988457-dirty`.

`s3store.New` dostane `config.WithResponseChecksumValidation(aws.ResponseChecksumValidationWhenRequired)`.
Default SDK je `WhenSupported` (`aws/config.go:186-191`), tedy „validuj vždycky, když
server checksum ohlásí" — a u multipart objektu je ohlášený CRC32 checksumem *zřetězených
checksumů částí*, ne dat. Validace tedy nemůže projít nikdy.

Změřeno 2026-07-21 proti lokálnímu Garage, čtyři varianty:

| konfigurace | objekt | výsledek |
|---|---|---|
| dnešní produkce | 9 MB (multipart) | **`Download` selže**, 3× CRC32; range GET téhož objektu vrátí správné bajty |
| `ValidationWhenRequired` | 9 MB (multipart) | **projde**, 9 437 184 B, bajty sedí |
| `PartSize` 16 MiB | 9 MB (single PUT) | projde |
| `PartSize` 16 MiB | 20 MB (pořád multipart) | **selže**, CRC32 |

Poslední řádek je pointa: **Krok 1 minu nezneškodňuje, jen ji zužuje z „nad 5 MiB" na
„nad 16 MiB"**, a na objekty, které v bucketu už leží, nemá vliv vůbec. Zneškodní ji
teprve Krok 0.

*Proč úplně první a mimo řadu — a co přesně je a není ohrožené.* Ověřeno 2026-07-21:

- **Produkce (B2) zasažená není.** 60 dní logu démona: nula `checksum did not match`,
  zato opakovaně `SDK WARN Skipped validation of multipart checksum` (naposled týž den
  15:30). B2 hlásí composite checksum se sufixem počtu částí, SDK ho rozpozná a validaci
  přeskočí. Garage hlásí holou hodnotu, SDK ji zvaliduje a neprojde.
- **Produkce navíc neběží ani na `136c83b`.** Nasazená binárka se hlásí jako
  `9988457-dirty`, tedy pět commitů zpátky — cache při zápisu tam ještě není. „Bod 2 to
  zneviditelnil" se v produkci zatím nestalo; stane se to při nejbližším `make install`.
- **Zasažené je lokální testování proti Garage**, a to přesně u whole-object `Download`
  nad 5 MiB — tedy u cest, které bloková práce potřebuje testovat nejvíc.

Pořadí tedy neurčuje produkční poplach, ale dvě věci: bez Kroku 0 se bloková práce
testuje proti prostředí, kde je jedna z jejích hlavních cest rozbitá z jiného důvodu,
a hamstor zůstává závislý na tom, jak konkrétně B2 hlásí checksumy.

*Pozor na past v ověřování:* jakmile `136c83b` doputuje do produkce, čerstvě zapsaný
soubor se čte z lokální cache, takže smoke test po zápisu **projde bez ohledu na to,
jestli je `Download` funkční**. Ověřovat se to musí přes `hamstor cache clear` (nebo
čtením souboru, který v cache prokazatelně není), ne zápisem a přečtením.

*Cena:* přicházíme o CRC validaci na GET i u single-PUT objektů. Pod šifrováním nic
neztrácíme (AES-GCM je autentizované, poškozený ciphertext se neotevře), u nešifrovaného
mountu zbývá TLS a integrita na straně S3. Proti „velké soubory jsou nečitelné" je to
dobrý obchod.

*Riziko:* nízké, jeden řádek, reverzibilní. Regresní test = varianta 2 z tabulky.

---

**Krok 1 — `PartSize`, aby byl blok z definice jeden PUT.**
> ✅ **HOTOVO 2026-07-21, commit `39726c5`.** Konstanta `s3store.UploadPartSize`
> (`s3.go:25-37`), testy v `internal/s3store/partsize_test.go`:
> `TestUploaderPartSize` (hlídá i `MinUploadPartSize` a poměr k 8MiB bloku) a
> `TestUploadBodiesAvoidPartBuffers` (tělo requestu zůstává `ReaderAt` — paměťová
> půlka D1). Nenasazeno samostatně.

`s3store.New` (`s3.go:57-61`) dostane `manager.NewUploader(client, func(u *manager.Uploader) { u.PartSize = 16 << 20 })`.
Sanity test, že tělo requestu zůstává `ReaderAt` (jinak 16 MiB × 32 souběžných uploadů
proti 150MB limitu).

*Proč tady:* není to oprava CRC32 bugu — tu udělal Krok 0 — ale **strukturální podmínka
blokového layoutu** z D1: blok ≤ `PartSize` znamená, že bloková cesta multipart nikdy
nevyrobí, nezávisle na tom, jestli je validace zapnutá. Nemá vazbu na schema.

*Riziko:* nízké. Reverzibilní jedním řádkem.

---

**Krok 2 — tabulka `blocks` a DB API, které nikdo nevolá.**
> ✅ **HOTOVO 2026-07-21, commit `b24ef8e`.** Migrace `blocks_table_v1`, konstanta
> `db.BlockSize`, typ `db.BlockCommit`, trojice `CommitBlocks` / `BlocksForInode` /
> `DeleteBlocksForInode`, `AllS3KeySet` rozšířený o bloky a seam `ops.gcScoped`.
> Testy: `internal/db/blocks_test.go` (9 funkcí, žádná nepotřebuje S3) a
> `TestGCPhase1KeepsBlockObjects` v `internal/ops`. Nenasazeno samostatně.

Versioned migrace `blocks_table_v1` (vzorem je `volumes_table_v1`, `db.go:150-163`),
`db.CommitBlocks`, `db.BlocksForInode`, `db.DeleteBlocksForInode`, a **rozšířený
`AllS3KeySet` o `SELECT s3_key FROM blocks`**.

Součástí kroku je **test GC fáze 1, který dnes neexistuje**: nahrát objekt, zapsat jeho
klíč do `blocks`, spustit `GC` a ověřit, že přežil. Ten test má být na místě dřív, než
kterýkoli blok vznikne.

**Jenže takhle, jak je ta věta napsaná, ten test projde vždycky** — i s `AllS3KeySet`,
který o blocích neví. Fáze 1 přeskočí objekty mladší 10 minut (`gc.go:62`) *dřív*, než
klíč s množinou vůbec porovná, takže čerstvě nahraný objekt přežije z důvodu, který
s bloky nesouvisí. Doslovné provedení té věty vyrobí přesně tu zelenou atrapu, před
kterou varuje odstavec o kus výš. Co ji dělá platnou:

- **Grace period musí jít obejít.** `GC` proto deleguje na nezveřejněné
  `gcScoped(..., gcOptions{grace, listPrefix}, ...)`. Produkce předává 10 minut a prázdný
  prefix, test nulu a vlastní prefix; `gcGracePeriod` zůstává konstantou, žádný mutable
  globální stav.
- **Listing musí jít zúžit, jinak ten test rozbíjí cizí balíky.** S nulovou grace je pro
  GC osiřelé všechno, co není v jeho čerstvé temp DB — tedy celý bucket. Ten je sdílený:
  `testutil.RequireS3` používá pět balíků (`hfuse`, `volume`, `s3store`, `replicate`,
  `ops`) a `go test ./...` je pouští paralelně, takže bez prefixu test maže data právě
  běžícím testům jinde. Platí pro každý další test, který nechá GC nebo `fsck` skutečně
  mazat — tedy i pro Kroky 4 a 7.
- **Kontrolní objekt.** Druhý objekt, který nereferencuje nic, a assert „kontrolní
  zmizel" **před** assertem „blokový přežil". Bez něj zelená znamená jen „nesmazalo se
  nic", což je stav, do kterého se test dostane každou chybou v nastavení.
- **Druhý test bez S3.** `TestAllS3KeySetIncludesBlocks` v `internal/db` ověřuje totéž na
  úrovni množiny klíčů. Důvod: GC test se v checkoutu bez `.env.test` skipne, a obrana
  proti Č1 nesmí být skipnutelná.

*Negativní ověření je součástí kroku, ne bonus:* zakomentovat `SELECT s3_key FROM blocks`
a oba testy spustit. Očekávané selhání je 404 na blokovém objektu z reálného GC běhu
proti Garage (ověřeno 2026-07-21), ne assert na počítadle.

*Proč druhý:* po tomhle kroku GC bloky nezničí — a teprve pak smí nějaké vzniknout. Změna
je čistě aditivní, nikdo nové API nevolá, filesystem se chová identicky.

*Riziko:* nízké. Prázdná tabulka.

---

**Krok 3 — zápis vyrábí bloky, čtení je pořád slepuje celé.**
> ✅ **HOTOVO 2026-07-21.** `loadFromBlocks`/`fetchBlock`, `dirtyBlocks` +
> `markDirtyRange`, `flushAsync` na N bloků přes `CommitBlocks`, per-blok šifrování,
> `db.HasBlocks`. Testy: `internal/hfuse/block_test.go` (10 funkcí), `concurrent_test.go`
> prošel beze změny sémantiky. Nenasazeno samostatně. **Sedm věcí vyšlo jinak, než tenhle
> dokument předpokládal — jsou zapsané níž a v sekci 4.**

`Flush`/`flushAsync` nahrají špinavé bloky a commitnou přes `CommitBlocks` (D5).
`inodes.s3_key` se **přestane zapisovat**, ale sloupec ještě existuje a čtecí cesta z něj
umí číst staré soubory. `ensureLoaded` pro soubor s bloky načte **všechny** bloky a slepí
je do souvislého bufferu.

Sem patří D4: `dirtyBlocks`, snapshot špinavých bloků při `Flush`, per-blok šifrování (D6).

*Proč tady:* zápisová cesta je ta složitá (souběžnost, otrava, retence). Nechat čtení
zatím hloupé znamená ladit jednu věc místo dvou. Úspora ještě žádná — všechno funguje
jako dřív, jen v jiných objektech.

*Riziko:* **nejvyšší z celé řady.** Sem míří všechny čtyři invarianty sdíleného write
state. Regresní testy `concurrent_test.go` (12 funkcí) musí projít beze změny sémantiky —
pokud některý začne padat, je to signál, ne úklid.

*Nezapomenout:* `retainPendingUpload` v tomhle kroku ještě umí jen jeden objekt, takže
selhaný upload vícebokového souboru zatím **nezadrží nic**. Musí to logovat jako
`DATA LOST` (větev `handle.go:1165` už existuje) a krok 7 to opraví. Je to vědomý dluh
na dva kroky, ne přehlédnutí — a proto je krok 7 v seznamu, ne v „někdy potom".
> Provedeno takto: retence proběhne, právě když je špinavý jediný blok s indexem 0 a
> velikost souboru ≤ `BlockSize` — tehdy je objekt totožný se souborem a formát
> `<id>.<size>` ho popisuje přesně. Cokoli vícebokového loguje `DATA LOST`.
> Hlídá to `TestMultiBlockUploadFailureRetainsNothing`, aby to nikdo „neopravil" tiše.

#### Co Krok 3 zjistil navíc (zapsáno po implementaci)

1. **Konverze z jiného tvaru úložiště musí přepsat celý soubor, ne jen špinavé bloky.**
   Dokument to nikde neříká a je to nejnebezpečnější věc v celém kroku: `CommitBlocks`
   zahodí předchozí úložiště *vcelku* (vynuluje `s3_key`, dekrementuje volume, vyčistí
   `vol_*`). Kdyby se u legacy celofilového souboru commitly jen dotčené bloky, zbytek
   souboru nemá po commitu **žádné** úložiště — změna jednoho bajtu ve 100MB souboru
   z 92 MB tiše udělá díry. Podmínka je
   `converting := !hasBlocks && (S3Key != "" || VolS3Key != "" || Size > 0)`; nový soubor
   konverzí *není*, jinak by řídký zápis na offset 1 GB nahrál 128 bloků nul.
   Ověřeno negativně: s vypnutým `converting` padá `TestLegacyWholeFileConvertsEntirely`
   hláškou „the untouched 16777216 bytes have no storage now".
2. **D4 snapshot: kopie se v Kroku 3 nedělá, a je to bezpečné.** Návrh ji vyžaduje proto,
   že spill file je „živá lokální materializace, ze které se dál čte a píše". V Kroku 3
   to ještě neplatí — čtení je hloupé, po `Flush`i je `loaded = false` a příští přístup
   načítá znovu z bloků, takže odevzdání vlastnictví je dnešní ověřený mechanismus.
   **Povinnou se kopie stává v Kroku 5**, kdy se spill file změní v řídkou živou
   materializaci; kdo Krok 5 dělá, musí ji zavést, jinak goroutina čte směs dvou verzí
   bloku. Cena opaku (proč se to nedělalo hned): nově zapsaný 4GB soubor = 4 GB kopie
   navíc na každý flush.
3. **Č7 se musel udělat teď, ne v Kroku 4.** Predikátu „commitnutý bez dat v S3" vyhoví
   každý blokový soubor. Změřeno na živém mountu: 20MB soubor `big.bin` starým predikátem
   vyjde jako `unreadable`, `fsck` skončí exit 1 a boot vypíše WARNING. Řeší to konstanta
   `db.noBlockRows`, sdílená `GetStagedInodes`, `Fsck` **a** `CommitNeedlesToVolume`.
4. **`onlyUnpacked` v `CommitNeedlesToVolume` přestal chránit.** Znamená „inode nemá
   úložiště" a testuje `s3_key`/`vol_s3_key` — jenže blokový inode má obojí prázdné, takže
   by builder směl zapakovat zapomenutý staging soubor na inode, který má bloky, a
   `vol_s3_key` by pak servíroval starou verzi. Struktura obrany je trojitá: blokový
   commit staging soubor **maže**, `CommitNeedlesToVolume` ho odmítne, a
   `CleanupStagingDir` ho po pádu uklidí (jinak by ho přejmenoval zpátky a builder by ho
   claimoval a restoroval na každý notify, s osiřelým volume pokaždé).
5. **`Fsync` na blokovém souboru vracel `EIO`.** Inventura ho vede jako „NE (jeden attempt
   na flush)", ale staged větev na `handle.go:1264` má stejný predikát jako Č7: poslala by
   blokový soubor do `VolumeBuilder.FlushInode`, který hledá staging soubor, co nikdy
   neexistoval.
6. **Nový invariant „jednou bloky, vždycky bloky".** Zmenšení pod `MaxNeedleSize` nesmí
   spadnout zpátky do stagingu: `CommitInode` o blocích neví, řádky by commit přežily a
   čtecí cesta (bloky napřed) by servírovala předchozí verzi. Stojí to jeden `HasBlocks`
   dotaz na flush existujícího souboru; `isNew` ho přeskakuje, takže bulk copy neplatí nic.
7. **`concurrent_test.go` má 11 funkcí, ne 12.** A `TestOpenTruncWithCacheBackedSibling`
   se po tomhle kroku sám **skipoval**: `st.cacheFile` plní jen legacy `s3_key` větev,
   kterou zápisová cesta už nevyrábí. Fixture se proto staví ručně (`Upload` +
   `CommitInode`), aby test dál hlídal to, co hlídal. Je to jediná změna v tom souboru a
   je to zároveň důkaz, že „zelená" a „něco se testuje" nejsou totéž.

*Ještě dvě věci, které Krok 3 vědomě nechává rozbité, protože patří Kroku 4:*
`Unlink`/`deleteTree`/`Rename` (Č5) blokové objekty nemažou — nejsou to ztracená data,
protože cascade smaže řádky a GC fáze 1 objekty uklidí, ale do prvního GC to leakuje.
A `rmdir_test.go:106` tím tiše zvakuovatěl (čte `meta.S3Key`, který je nově prázdný);
Krok 4 opraví obojí naráz, protože právě on ten sloupec maže.

*A jedna, která patří Kroku 5:* `db.SetAttr` při zmenšení nemaže bloky za koncem, takže
`truncate(2)` **bez otevřeného handle** nechá řádky ležet a příští zvětšení vydá stará
data místo nul. Ověřeno, že to **není regrese** — `dcf14b0` dělá totéž — a proto to Krok 3
nezdržovalo. Podrobnosti a míra u Kroku 5.

---

**Krok 4 — smazat `InodeMeta.S3Key` a `inodes.s3_key`, ať mluví compiler.**
Po tomhle kroku nemá pole existovat. Build vyjmenuje zbytek; opravit se musí:
`Unlink`/`deleteTree`/`Rename` (Č5), `Cleanup` (Č6), `Fsck` + `CheckStagedData` +
`GetStagedInodes` (Č7), `readStaged` + `CleanupStagingDir` + `builder` predikáty (Č8),
`DeleteInode`/`DeleteInodeWithVolume` (Č4), GC fáze 2 (Č3). Smazat
`internal/ops/migrate.go`, `db.AllS3Keys`, `db.UpdateS3Key`, `db.CommitInode` a příkaz
`migrate` z `main.go`.

Testy, které spadnou kompilací a musí se přepsat: `rmdir_test.go:44` a `rmdir_test.go:106`
(jediné dva testy, které dnes sahají na `S3Key` přímo).

> Obojí se ukázalo být nepřesné: `CommitInode` se smazat nedá a testů je devět souborů.
> Viz „Co Krok 4 zjistil navíc" níž.

*Proč až tady:* dřív by to znamenalo mít rozbité čtení starých souborů. Po kroku 3 už
žádné nové soubory sloupec nepoužívají, takže smazání je čistý řez.

*Riziko:* střední, ale **nízké v tom smyslu, na kterém záleží** — chyby jsou build errory,
ne runtime. Jediné, co compiler nechytí, jsou SQL predikáty ve stringu (Č7, Č8), a ty
jsou v inventuře vyjmenované.

#### Co Krok 4 zjistil navíc (zapsáno po implementaci)

1. **Streaming byl rozbitý od Kroku 3 a nikdo to nevěděl.** `node.Open` zapínalo
   `handle.streaming` na „read-only ∧ bez šifrování ∧ media přípona ∧ `StreamRate > 0`" —
   **bez ohledu na to, jestli soubor má celofilový klíč.** `Read` volí streaming jako
   první větev a `fetchStreamChunk` dělá `DownloadRange(ctx, h.s3Key, …)`. Jenže od
   `5902a6b` je `h.s3Key` u každého nově zapsaného souboru prázdný, `--stream-rate` má
   default **5**, a tak **každý `.mp4` na nešifrovaném mountu vracel EIO**
   (`input member Key must not be empty`). Nechytil to žádný test, protože `setupTest`
   nechává `StreamRate` na nule — streaming neměl pokrytí vůbec.

   Krok 4 by to zabetonoval (vzal by `h.s3Key` poslednímu čtenáři, který by na to mohl
   přijít), takže se řeší tady: enablement se vypíná, média padají na `ensureLoaded`.
   Krok 6 ho zapne nad bloky. Nový `TestStreamingMediaFileReads` to hlídá oběma směry.

2. **`RecoverPending` je build error, který návrh nezmiňoval.** `cleanup.go:160`
   commitovalo přes `CommitInode(inodeID, key, logicalSize)`, tedy celofilovým klíčem.
   Návrh přiřadil celou retenci Kroku 7, ale tenhle řádek musel padnout teď. Vyšlo to
   levně, protože Krok 3 nechal retenci fungovat **jen pro `wholeFileInOneBlock`**:
   zadržený soubor je přesně jeden blok, takže commit je `CommitBlocks` s `{Index: 0,
   Size: logicalSize}`. `Size` musí být **logická** délka z názvu souboru, ne
   `info.Size()` — pod šifrováním se liší a soubor by četl dlouze. Dluh Kroku 7
   (adresář pro víceblokové sady) zůstal nedotčený. Přibyla pojistka: `logicalSize >
   BlockSize` se odmítne a soubor se **nechá ležet**, protože je to jediná kopie.

3. **`db.CommitInode` nešlo smazat**, jak návrh říkal — má tři živé volající
   (`flushStaged`, prázdný soubor ve `Flush`, `RecoverPending`). Zůstává, jen bez
   parametru klíče: nově znamená „commitni inode, který nevlastní žádný objekt".

4. **Testů sahajících na `S3Key` bylo devět souborů, ne dva.** Kromě
   `rmdir_test.go` i `recover_test.go` (čte pole přímo) a šest dalších, které si
   stavěly legacy inode přes `CommitInode(id, key, size)`: `audit_test.go`,
   `concurrent_test.go` (2×), `block_test.go`, `db/blocks_test.go` (2×),
   `ops/gc_test.go` (3×), `volread_test.go`.

   Dva z nich byly od Kroku 3 **prázdné**: `rmdir_test.go:44` sbíralo `c.S3Key` do
   pole, které už bylo vždycky prázdné, a `:106` totéž pro jeden soubor. Teď čtou
   `BlocksForInode` a `t.Fatal`ují, když nic nenajdou — takže se to nemůže zopakovat.
   Ověřeno mutací (vypnout mazání v `deleteTree` ⇒ oba testy spadnou).

   `ops/gc_test.go` přepsané na blokové sady dalo **Č3 první test vůbec**.

5. **`cacheFile` šel celý pryč, `h.s3Key` zůstal.** Obě pole se po tomhle kroku stala
   nedosažitelnými, ale nejsou to stejné případy. Kolem `cacheFile` byly **invarianty**
   (zavřít při `O_TRUNC`, materializovat před `truncate`, překlopit na buf/spill při
   prvním zápisu) a žádný pozdější krok ho neoživuje — Krok 5 cachuje **per blok** do
   sestaveného bufferu, ne jako backing file celého stavu. `h.s3Key` je proti tomu
   inertní string bez jediného pravidla, který drží zkompilované ty čtyři funkce, jež
   má Krok 6 přepsat. Smazat ho = udělat Krok 6 tady.

6. **Č4 se udělal vracením klíčů, jak návrh doporučoval, a hned se to vyplatilo:**
   `Rmdir` a `Cleanup` by se na ně jinak nezeptaly. Rozšířeno i na `Fsync` a
   `volume.Builder`, kde predikát „už je durable" testoval jen `vol_s3_key`: souběžný
   přepis přes `MaxNeedleSize` commitne bloky, takže by `Fsync` protočil všech deset
   backoffů a vrátil EIO za soubor bezpečně uložený v S3. Nově `Builder.durable`.

7. **CLAUDE.md neobsahovala invariant o `cacheFile`**, jak předpokládalo zadání — žil
   jen v komentářích `node.go` a v doc commentu testu, a zmizel s nimi. Zato v ní byl
   **jiný zastaralý záznam z Kroku 3**: bod o cachování při zápisu mluvil o
   `cacheUploaded` a `CommitInode`, přestože se ta funkce ve Kroku 3 přejmenovala na
   `cacheBlock` a cachuje se per blok. Opraveno.

8. **`TestLegacyWholeFileConvertsEntirely` nešlo jen smazat.** Byl jediný, kdo hlídal
   pravidlo „konverze přepíše celý soubor" — `TestStagedFileGrowingIntoBlocksStays-
   Readable` přepisuje od offsetu 0, což ušpiní všechny bloky tak jako tak a projde
   i s rozbitým `converting`. Po zrušení sloupce jsou jediné tvary, ze kterých se
   konvertuje, needle a staging soubor, a oba se vejdou do bloku 0 — takže jediný
   způsob, jak tu chybu ještě vyjádřit, je **řídký** zápis za konec needlu. Nahrazeno
   `TestConvertingToBlocksRewritesUntouchedData`.

---

**Krok 5 — líná materializace: tady zmizí full download a strop 2 GB.**
> ✅ **HOTOVO 2026-07-21.** `presentBlocks`/`blockBacked`, `attachBlocks`/`faultBlock`/
> `materializeRange`/`materializeForWrite`, `db.BlockAt`, `SetAttr` vracející osiřelé klíče,
> `clampLastBlockSize`, `MaxDownloadSize` = `UploadPartSize`. Testy:
> `internal/hfuse/lazy_test.go` (14 funkcí) a dvě v `internal/db/blocks_test.go`.
> Změřeno na živém mountu: append do 100MB souboru se studenou cache **21 ms a 1 stažený
> blok** místo 100 MB; `truncate -s 5T` 2,3 ms bez jediného řádku; zápis 4 B na offsetu
> 4 TB vyrobí 1 blok. **Čtyři věci vyšly jinak, než tenhle dokument předpokládal — jsou
> zapsané níž.** Nenasazeno samostatně.

`presentBlocks`, per-blok fault v `readLoaded`, řídký spill file. Write preload v `Open`
(`node.go:332-361`) přestane cokoli stahovat. `MaxDownloadSize` se překlopí na sanity
check bloku.

**Sem patří i `db.SetAttr` (`:780-803`), jediná položka z inventury 3.2, kterou dosud
neměl žádný krok.** Zmenšení souboru **bez otevřeného handle** (`truncate(2)` po cestě,
`tryAcquireWrite` vrátí `nil`) neprojde flushem, takže `CommitBlocks` se nikdy nezavolá a
řádky bloků za novým koncem přežijí. `SetAttr` proto při zmenšení musí sám smazat
`blocks WHERE block_index > lastLive` a vrátit klíče volajícímu k smazání — stejným
směrem „přečti klíče → smaž řádky → smaž objekty" jako `DeleteBlocksForInode`.

Změřeno po Kroku 3 (3 bloky → `truncate` na 16 B): řádky zůstanou **3**, čtení vrátí
správných 16 B (clamp funguje), ale zvětšení zpátky vydá **8 MiB staré dat místo nul**.

*Tohle není regrese blokového layoutu.* Týž test na `dcf14b0` (celofilové objekty)
resurrektuje přesně týchž 1 048 560 bajtů — zkrácení nikdy nepřepsalo objekt a
`readLoaded` se clampuje na `st.size`. Bloky ten bug **zlevňují**: opravit ho znamená
smazat pár řádků, kdežto u jednoho celofilového objektu by se musel přepsat celý.
S otevřeným handlem je to už teď v pořádku (`Flush` → `CommitBlocks` ořeže podle
velikosti, hlídá `TestShrinkDropsBlocksPastEndOfFile`) — chybí právě jen bezhandlová cesta.

*Proč tady:* je to teprve tenhle krok, který plní cíl dokumentu — do teď se jen měnil
tvar úložiště. Odděleně od kroku 3 proto, že chybu v lazy faultu jde poznat: soubor čte
nuly nebo `EIO`, ne tiše starou verzi.

*Riziko:* střední. Testovat sparse soubory (`dd seek=`), čtení přes hranici bloku,
čtení díry a `truncate` nahoru na velikost, kterou by nešlo materializovat. K `SetAttr`
patří vlastní regresní test: zmenšit **bez otevřeného handle**, zvětšit zpátky a ověřit,
že se čtou nuly — ne starý ocas.

#### Konzumenti předpokladu „backing store obsahuje celý soubor" (pro Kroky 6–7)

Každý krok téhle řady tiše zneplatnil předpoklad, který compiler nevidí: Krok 3 nechal
`h.s3Key` vždycky prázdný (přišlo se na to až po EIO na každé `.mp4`), Krok 4 udělal
`st.cacheFile` nedosažitelným. Krok 5 ruší **„spill file / `buf` obsahuje celý soubor"**.
Inventura, se kterou se pracovalo:

| konzument | jak dopadl |
|---|---|
| `readLoaded` | faultuje čtený rozsah (`materializeRange`) |
| `Write` částečný přepis bloku | **faultuje před zápisem** (`materializeForWrite`); jediná tichá cesta v celém kroku |
| `flushAsync` větev `converting` | přepsána, viz nález 1 níž |
| `flushStaged` bere `st.buf` jako celý soubor | drží `canStage = !hasBlocks`; kryje assert |
| `scheduleThumb` | `wholeFileSnapshot`, viz rozhodnutí níž |
| `retainPendingUpload` | beze změny — `wholeFileInOneBlock` ⇒ blok je dirty ⇒ přítomný |
| `cacheBlock` | beze změny — commitnutý blok je dirty ⇒ přítomný |
| `spillToDisk` → `spillState` | přesun store; **nesmí resetovat `presentBlocks`** |
| `truncateWriteState` | zmenšení zahodí značky za koncem; zvětšení nad `spillThreshold` přejde na řídký spill |
| clamp na `meta.Size` v `Open` | totéž (`dropBlocksPast`) |
| větev `O_TRUNC` v `Open` | čistí `presentBlocks`, `blockBacked`, `wholeLoaded` |
| `logicalSize()` | platí díky invariantu „store je přesně `st.size` dlouhý", který drží `attachBlocks` |
| `O_APPEND` větev | kryje pravidlo pro `Write` |
| `ensureLoaded` fallback „prázdný soubor" | přepsán, viz nález 2 |
| `db.SetAttr` | maže bloky za koncem a vrací klíče |

Navíc dvě čistě výkonnostní: `Open` i `openPreloadStaged` se ptaly `BlocksForInode` jen na
„má bloky?" — u 4TB souboru 524 288 řádků na každé otevření. Nově `HasBlocks`.

**Invariant, který krok zavádí: `dirtyBlocks ⊆ presentBlocks`.** Drží ho pořadí ve `Write`
(fault → zápis → značka) a **jistí runtime assert ve `Flush`**: blok, který má jít nahoru
a nebyl materializovaný, dá `EIO` a otráví stav místo aby nahrál nuly. Ten assert je tam
schválně proti pravidlu „drží to konstrukce" — přesně tenhle druh předpokladu už třikrát
tiše padl.

#### Rozhodnutí: náhledy u částečné materializace se negenerují

Zdrojem náhledu smí být jen snapshot pokrývající celý soubor (`wholeFileSnapshot`, dřív
neformulovaná podmínka ve `flushAsync`). Alternativa „dotáhnout chybějící bloky" by
znamenala stáhnout celý 2GB PSD kvůli 256px náhledu — přesně ten full download, který
tenhle krok ruší — a udělat to v goroutině, která nesmí na `st.mu`. Cena volby je
**zastaralý** náhled místo náhledu s dírami; freedesktop mtime ho nechá prohlížeči
přegenerovat čtením přes mount, kde je soubor kompletní. Zastaralý a sám se opravující
poráží platně vypadající a trvale špatný.

#### Co Krok 5 zjistil navíc (zapsáno po implementaci)

1. **`converting` nesmí vycházet z `inodes.size`, protože `dd seek=` volá `ftruncate`
   dřív než první zápis.** Dokument podmínku `Size > 0` zavedl s tím, že „nový soubor
   konverzí není", jenže nový soubor má velikost nastavenou ještě před prvním `Write`em —
   tak pracuje `dd`, `truncate(1)` i každý předalokující stahovač. Naměřeno na živém
   mountu: `dd bs=1M seek=4096 count=1` na novém souboru commitlo **513 řádků a nahrálo
   4 GiB nul** za 1 KB dat. Test `TestSparseWriteMaterializesOneBlock` to nechytil, protože
   zapisuje na offset **bez** toho `ftruncate` — fixture neodpovídala tomu, co dělají
   skutečné nástroje. Nově rozhoduje `inodeWrite.wholeLoaded`, tedy *jak se store naplnil*,
   ne co říká velikost. Hlídá `TestSparseWriteAfterFtruncateMaterializesOneBlock`.
2. **„Velikost bez dat" je řídký soubor, ne chybějící staging.** Predikát
   „commitnutý, bez needle, bez bloků" bral `truncate -s 4G` jako staged soubor, jehož
   staging file zmizel: `readStaged` protočil pět pokusů a vrátil `EIO` — pro obyčejné
   `dd seek=`. Boot k tomu hlásil `WARNING: unreadable: sparse.bin (4294967296 bytes)`, což
   zabíjí signál pro případ, kdy je to pravda. Rozlišuje to velikost: `flushStaged` se dá
   dosáhnout jen do `MaxNeedleSize`, takže větší soubor **nikdy staged nebyl**. Konstanta
   se proto přestěhovala do `db` (`volume.MaxNeedleSize` je nově alias — `volume` importuje
   `db`, obráceně to nejde) a je součástí `noBlockRows`.
3. **Flush nového souboru přepisoval velikost nastavenou `ftruncate`em.** Kernel posílá
   `FLUSH` hned po `CREATE`; ta větev commitovala `CommitInode(id, 0)`, takže
   `truncate -s 5T newfile` skončil jako 0bajtový soubor. Commituje se nově velikost, kterou
   inode skutečně má. Bylo to tam od Kroku 4, jen to nikdo nezkusil.
4. **Zmenšení musí zkrátit i poslední přeživší blok**, ne jen smazat bloky za koncem —
   viz opravená sekce 2. Předepsaná oprava (`DELETE ... WHERE block_index > lastLive`)
   případ, který dokument naměřil, neřeší vůbec.

*Co zůstává otevřené pro Krok 6:* sekvenční čtení velkého souboru materializuje celý soubor
do spill adresáře. Není to regrese (`ensureLoaded` to dělalo taky, jen navíc předem), ale
omezené čtecí cesty patří Kroku 6 — `readChunked`/`readStreaming` jsou přesně ta místa,
kde má čtení běžet s ohraničenou stopou. A u souborů **do** `MaxNeedleSize` zůstává
„staged" a „samé díry" nerozlišitelné; prakticky to nevadí (staged soubor se přepíše celý),
ale je to poslední kout, kde velikost o obsahu nic neříká.

---

**Krok 6 — smazat chunk sub-cache a přepsat streaming.**
`cache.chunkPath`/`GetChunk`/`PutChunk`/`HasChunk` pryč; s nimi guardy na adresář v `Has`
a `Open`. `readChunked`/`getOrFetchChunk`/`prefetchChunks`/`readStreaming` na bloky.
In-memory ring v streaming módu zrušit (sekce 3.3), `readAheadChunks` přeladit v blocích.
**Povolit streaming pod šifrováním** a přepsat odpovídající „Known Limitation" v CLAUDE.md.

**Pozor: streaming je od Kroku 4 vypnutý úplně**, ne jen pod šifrováním — `Open`
nenastavuje `handle.streaming` vůbec, protože není do čeho rangovat (viz „Co Krok 4
zjistil navíc", bod 1). Tenhle krok ho zapíná zpátky, nad bloky, pro **oba** případy
naráz; `HamstorHandle.s3Key` (dnes vždycky `""`) přitom zaniká. Hlídá to
`TestStreamingMediaFileReads`, který dnes prochází přes `ensureLoaded` a po tomhle
kroku má procházet přes streaming — takže musí projít pořád.

*Proč až tady:* je to čistý úklid mrtvého kódu — smysl dává teprve, když bloky reálně
slouží čtení (krok 5). Dřív by to byla změna dvou cest najednou.

*Riziko:* nízké. Ubývá kód. Hlídat jen paměťový rozpočet streamingu proti 150MB limitu.

---

**Krok 7 — retence a obnova na sadu bloků.**
`retainPendingUpload` → adresář `pending/<inodeID>/` s `meta` + soubory bloků.
`hasRetainedData` na nový tvar (**pozor na glob**, Č2). `RecoverPending` nahraje sadu a
commitne ji jednou transakcí, nebo nechá adresář na místě.

Regresní testy: pád mezi uploadem a commitem (rozšířit `crash_test.go`), selhání uploadu
uprostřed sady, obnova sady přes restart, `Cleanup` nesmí sáhnout na inode s adresářem
(rozšířit `recover_test.go`, 4 funkce).

*Proč poslední:* je to jediný krok, který závisí na finálním tvaru všech ostatních. Do té
doby platí dluh z kroku 3 (selhaný upload = `DATA LOST` v logu).

*Riziko:* střední. Nízká pravděpodobnost, vysoký dopad — proto samostatný krok
s vlastními testy a ne přílepek ke kroku 3.

---

### Co zbývá po posledním kroku

- **Aktualizovat CLAUDE.md**: reader coherence (sekce 4) a zrušené omezení range reads pod
  šifrováním. *(Zmínky o `migrate`, 2GB stropu, sekce o blocích a stav kroků už hotové
  v Kroku 4.)*
- **`purge-s3` + reinit** produkčního bucketu.
- Nezávisle a kdykoli: oprava `evictLRU` pro `volobj/` (sekce 3.3), HKDF subkeys (D6),
  writeback cache, split `dentries` z `inodes`.

---

## Reference

- `project-large-file-layout-roadmap` — dohodnuté pořadí (cache put → writeback → bloky) a proč
- `project-b2-write-costs-free` — Class A = $0, takže argument cenou zápisů neexistuje
- `project-multipart-checksum-eio` — CRC32 bug, reprodukce a dopady
- `project-audit-deferred` — vědomě odložené položky z auditu 2026-06-04
- `manager@v1.20.18/upload.go:386-400,468-528` — `singlePart`, `nextReader`, alokace poolu
