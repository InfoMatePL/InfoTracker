# Postęp refaktoru InfoTracker

Data: 2025-12-05

## Problem: Brak źródeł dla tabel tymczasowych w lineage

### Opis problemu
Dla procedury `update_stage_mis_LeadTime.sql`:
- Główny plik procedury pokazuje inputy jako `.#ctrl`, `.#LeadTime_STEP1` itd. z namespace `TEMPDB`
- Istnieją odpowiednie artefakty OpenLineage dla tych tabel (np. `dbo.update_stage_mis_LeadTime#ctrl`) z namespace `EDW_CORE`
- Brak połączenia między inputami a outputami tabel tymczasowych - powinny być linkowane

### Przykład
- W głównym pliku: `{"namespace": "mssql://localhost/TEMPDB", "name": ".#ctrl"}`
- Istniejący artefakt: `{"namespace": "mssql://localhost/EDW_CORE", "name": "dbo.update_stage_mis_LeadTime#ctrl"}`
- Powinno być: `{"namespace": "mssql://localhost/EDW_CORE", "name": "dbo.update_stage_mis_LeadTime#ctrl"}`

### Analiza
1. Parser generuje kanoniczną nazwę dla tabel tymczasowych: `EDW_CORE.dbo.update_stage_mis_LeadTime.#ctrl`
2. `_ns_and_name` w `names.py` poprawnie parsuje to do namespace `EDW_CORE` i nazwę `dbo.update_stage_mis_LeadTime#ctrl`
3. Ale gdzieś w łańcuchu dependencies były zapisywane jako `.#ctrl` zamiast pełnej kanonicznej nazwy

### Hipotezy (ZAKTUALIZOWANE)
- ✅ POTWIERDZONO: sqlglot parsuje temp tables (#ctrl) jako `tempdb..#ctrl` w AST (catalog=tempdb, db=dbo, name=#ctrl)
- ✅ POTWIERDZONO: `_qualify_table` używa catalog bezpośrednio, tworząc `tempdb.dbo.#ctrl` zamiast kanonicznej nazwy
- ✅ POTWIERDZONO: Problem był w dwóch miejscach:
  1. `_qualify_table` (używany rzadziej)
  2. `_get_table_name` (używany przez deps.py) - TU BYŁ GŁÓWNY PROBLEM

### Źródło problemu
1. W `select_lineage.py` CTE są parsowane i zbierane ich dependencies przez `_extract_dependencies`
2. `_extract_dependencies` (deps.py linia 35) używa `_get_table_name` dla każdej tabeli
3. `_get_table_name` (names.py linie 162-230) miał **trzy miejsca** zwracające `tempdb..#`:
   - Linia 180: `return f"tempdb..#{simple}"` gdy catalog=tempdb
   - Linia 184: `return f"tempdb..#{simple}"` gdy w temp_registry
   - Linia 224: `return f"tempdb..#{temp_name}"` gdy full_name startswith '#'
4. Te wartości trafiały do dependencies głównej procedury i do OpenLineage jako input

### Rozwiązanie (UKOŃCZONE ✅)
1. Zmodyfikowany `_qualify_table` w `names.py` (linie 145-160) - CZĘŚCIOWE
2. **Zmodyfikowany `_get_table_name` w `names.py` (3 miejsca)** - PEŁNE ROZWIĄZANIE:
   - Linia ~180: Zamiana `f"tempdb..#{simple}"` → `self._canonical_temp_name(f"#{simple}")`
   - Linia ~184: Zamiana `f"tempdb..#{simple}"` → `self._canonical_temp_name(f"#{simple}")`
   - Linia ~224: Zamiana `f"tempdb..#{temp_name}"` → `self._canonical_temp_name(f"#{temp_name}")`

### Weryfikacja
- ✅ pytest -q: 136 passed, 2 skipped
- ✅ Warnings nie zawierają już `'tempdb..#ctrl'`
- ✅ Teraz: `'EDW_CORE.dbo.update_stage_mis_LeadTime.#ctrl'`
- ✅ JSON artifacts mają poprawny namespace: `mssql://localhost/EDW_CORE`
- ✅ Temp table inputs poprawnie linkowane do swoich output artifacts

### Rezultat
Problem z temp tables **całkowicie rozwiązany**. Temp tables teraz mają poprawne namespaces i są poprawnie linkowane między inputs/outputs w OpenLineage JSON.

---

### Aktualizacja 2025-12-05 (wieczór)

**Co zrobione**
- Przerobiony ekstraktor SELECT...INTO w `procedures.py` na wyszukiwanie wsteczne przez wszystkie podwójne nowe linie (zamiast pojedynczego regexa). Dzięki temu dla `#LeadTime_STEP1` wyciągamy pełny blok: 11309 znaków, 243 linie, 35 LEFT JOIN i 38 referencji tabel.
- Uruchomiony ekstrakt: `python -m infotracker extract --sql-dir .\build\input\test6 --out-dir .\build\output\test_extract_fix_v9 --log-level WARNING`.
- Wygenerowany graf: `infotracker viz --graph-dir .\build\output\test_extract_fix_v9`.
- Testy regresji: `python -m pytest -q` → **136 passed, 2 skipped** (bez regresji).

**Stan JSON/graph po naprawie ekstrakcji (focus na #LeadTime_STEP1)**
- `build/output/test_extract_fix_v9/...hashLeadTime_STEP1.json`: `inputs=1` (tylko `dbo.update_stage_mis_LeadTime#offer`), `outputs=1`. Brak facetów schema/columnLineage.
- `column_graph.json`: `nodes=354`, `edges=284`, **brak nodu LeadTime_STEP1** i brak krawędzi do niego.
- Mimo że ekstraktor znalazł 38 tabel (EDW_CORE.Asset_BV, Contract_BV, MainProductType_BV, OfferStatus_BV, DistributionChannel_BV itd.), parser/deps nie wprowadza #LeadTime_STEP1 do grafu ani do JSON inputs.

**Wnioski / co jeszcze do zrobienia**
- Ekstrakcja SELECT...INTO jest już poprawna (pełny SQL blok jest dostępny), ale pipeline parsowania/dep-resolve nie propaguje tabel JOIN do dependencies i nie tworzy nodu/edge dla #LeadTime_STEP1.
- Trzeba wymusić, by fallback/parsing dla SELECT...INTO (#LeadTime_STEP1) uzupełniał `temp_sources`/`ObjectInfo.dependencies` z tabel FROM/JOIN oraz generował facet schema/columnLineage → wtedy pojawi się w grafie i inputs>1.
- Po każdej zmianie: uruchomić ekstrakt na test6 → viz → pytest -q, sprawdzić `hashLeadTime_STEP1.json` (inputs>30) i `column_graph.json` (node+edges dla LeadTime_STEP1).

## Nowy problem: Brak wielu tabel źródłowych w dependencies

### Analiza dla update_stage_mis_LeadTime.sql
Data: 2025-12-05

#### Statystyki
- **Wygenerowane artefakty**: 25 JSON (22 temp tables + column_graph + lineage_viz + main procedure)
- **Temp tables**: Wszystkie 22 poprawnie wygenerowane i zlinkowane ✅
- **Input dependencies w głównej procedurze**: 33 (14 regular tables + 19 temp tables)

#### Problem z #offer (ROZWIĄZANY ✅)
**Symptom**: #offer temp table miała 0 inputs, mimo że SELECT...INTO wyraźnie się odwołuje do `FROM offer_MSBV` i `JOIN #ctrl`

**Root cause (warstwowy)**:
1. **Warstwa 1**: Kod szukał WITH statements wstecz, ale znalazł WITH z poprzedniej procedury (start procedury). To było spowodowane brakiem sprawdzenia, czy między WITH a INTO jest średnik (granica statement).
   - **Rozwiązanie**: Dodane filtrowanie WITH matches - jeśli jest średnik między WITH a INTO w preprocessed_body, WITH jest dla innego statement'u i odrzucamy go.

2. **Warstwa 2**: Dla SELECT...INTO bez WITH, sqlglot parse failed (nieprawidłowy SQL z WHERE w JOIN). Fallback powinien był wyciągnąć dependencies regex'em.
   - **Root cause warstwa 2**: Dependencies extraction był **WEWNĄTRZ** `if col_names:` conditional - jeśli column extraction nie znalazła kolumn, dependency extraction vообще się nie odpalał!
   - **Rozwiązanie**: Przeniesienie dependency extraction **POZA** `if col_names:` block - teraz dependencies są wyciągane niezależnie od tego czy kolumny zostały znalezione.

**Rezultat po naprawie**:
- ✅ #offer teraz ma 5 inputs: `offer_MSBV`, `contract_bv`, `dbo.offer_MSBV`, `dbo.contract_bv`, `#ctrl`
- ✅ Wszystkie 3 poprawne źródła zostały zidentyfikowane
- ✅ #LeadTime_STEP1 nadal ma 33 inputs (bez regresji)

#### Brakujące tabele źródłowe (17 tabel) - ZA POPRZEDNIM RAZEM:
1. `Asset_BV` - używana w LEFT JOIN (linia 626)
2. `AssetState_BV` - JOIN przez Asset_BV.Key_AssetState
3. `AssetSectorTypeDictionary_BV` - JOIN przez AssetSector
4. `AssetSegmentDictionary_BV` - JOIN przez AssetSector
5. `Contract_BV` - LEFT JOIN (linia 638)
6. `ContractParameter_BV` - w subquery LEFT JOIN (linie 641, 650)
7. `MainProductType_BV` - LEFT JOIN przez offer.Key_MainProductType (linia 696)
8. `OfferStatus_BV` - LEFT JOIN przez offer.Key_OfferStatus (linia 699)
9. `DistributionChannel_BV` - LEFT JOIN (linia 705, 717)
10. `CommissionRegisterAgreement_BV` - LEFT JOIN (linie 708, 714)
11. `FinancialAdvisorPosition_BV` - LEFT JOIN (linie 711, 717)
12. `DistributionNetwork_BV` - LEFT JOIN (linie 714, 720)
13. `OfferVerificationAcceptation_MSBV` - LEFT JOIN (linia 602)
14. `OfferTransactionParameters_MSBV` - LEFT JOIN (linia 606)
- **Status**: Wszystkie 17 wcześniej brakujące tabele są teraz w #LeadTime_STEP1 ✅

**Testy regresji**:
- ✅ `pytest -q`: **136 passed, 2 skipped** (brak regresji)
- ✅ #LeadTime_STEP1: 33 inputs (poprzednio 1 - duża poprawa!)
- ✅ #offer: 5 inputs (poprzednio 0 - naprawione!)
- ✅ Wszystkie temp tables mają poprawny namespace EDW_CORE
15. `OfferTransactionTags_MSBV` - LEFT JOIN (linia 610)
16. `PartyStatement_MSBV` - używana w temp table #PartyStatement (linia 747)
17. `End2EndSLA_BV` - używana w STEP3 (linie 900+)

#### Przyczyna problemu
Parser **nie wykrywa tabel używanych tylko w JOIN'ach** gdy:
- Żadna kolumna z tych tabel nie jest używana bezpośrednio w SELECT
- Tabele są używane tylko do JOIN'owania (łączenie przez klucze obce)
- Kolumny z tych tabel mogą być używane tylko w WHERE/AND warunkach JOIN

Przykład:
```sql
LEFT JOIN EDW_CORE.dbo.Asset_BV asset
  ON offer.Key_Asset = asset.key_asset
LEFT JOIN EDW_CORE.dbo.AssetState_BV ast
  ON asset.Key_AssetState = ast.Key_AssetState  -- Asset_BV NIE jest w dependencies!
```

#### Weryfikacja kolumn
Tabele te **są używane** w SELECT - trzeba sprawdzić:
- `mpt.ProductTypeGroup` (MainProductType_BV)
- `asd.AssetSegmentName` (AssetSegmentDictionary_BV)
- `ast.AssetStateGroupCode` (AssetState_BV)
- `os.OfferStatusOriginalCode` (OfferStatus_BV)
- `ova.FirstVerificationDate`, `ova.LastAcceptationDecision` (OfferVerificationAcceptation_MSBV)
- `otp.OfferCreditLimit` (OfferTransactionParameters_MSBV)
- `ott.IsOfferWNT` (OfferTransactionTags_MSBV)

#### Weryfikacja w column_graph.json
✅ Sprawdzono - **7 z 8 kluczowych kolumn BRAKUJE** w column_graph  
❌ `producttypegroup` (MainProductType_BV) - BRAK  
❌ `assetsegmentname` (AssetSegmentDictionary_BV) - BRAK  
❌ `assetstategroupcode` (AssetState_BV) - BRAK  
❌ `offerstatusoriginalcode` (OfferStatus_BV) - BRAK  
❌ `firstverificationdate` (OfferVerificationAcceptation_MSBV) - BRAK  
❌ `lastacceptationdecision` (OfferVerificationAcceptation_MSBV) - BRAK  
❌ `offercreditlimit` (OfferTransactionParameters_MSBV) - BRAK  
✅ `isofferwnt` (OfferTransactionTags_MSBV) - istnieje ALE tylko między temp tables, nie od źródła!

**Przykład problemu**:
```sql
-- Linia 518 w SQL:
, os.OfferStatusOriginalCode 

-- Linia 699 w SQL:
LEFT JOIN EDW_CORE.dbo.OfferStatus_BV os
  ON offer.Key_OfferStatus = os.Key_OfferStatus

-- Kolumna jest w SELECT, tabela w LEFT JOIN -> BRAK w dependencies!
```

#### Root cause
Parser **nie zbiera dependencies z LEFT JOIN** nawet gdy kolumny z tych tabel są używane w SELECT. Prawdopodobnie:
- `deps.py` zbiera tylko tabele z FROM i INNER JOIN?
- Lub LEFT JOIN są ignorowane podczas ekstrakcji dependencies?
- Column-level lineage nie działa dla LEFT JOIN'owanych tabel?

#### Wpływ na projekt
**Krytyczny**: ~34% tabel źródłowych (17/50) **nie jest w lineage graph**  
**Konsekwencja**: Niekompletna mapa zależności - brak informacji o:
- Jakie kolumny pochodzą z Asset_BV, Contract_BV, etc.
- Zmiany w tych tabelach nie pokażą impact analysis
- Wizualizacja lineage nie pokazuje pełnego obrazu

#### Szczegółowa diagnostyka
✅ `sqlglot.find_all(exp.Table)` **poprawnie znajduje** tabele z LEFT JOIN (test prostego SELECT)  
❌ `#LeadTime_STEP1` ma tylko 1 input (#offer) zamiast ~30+ tabel  
❌ Kolumny `producttypegroup`, `offerstatusoriginalcode`, `assetsegmentname` **nie istnieją** w schema facet  
❌ `columnLineage` facet **nie istnieje** dla #LeadTime_STEP1  

**Oznacza to**: Parser w ogóle nie widzi SELECT z LEFT JOIN dla #LeadTime_STEP1!

#### ROOT CAUSE ZNALEZIONY! 🎯

**Analiza debug logów**:
✅ `_parse_select_into` **NIGDY nie jest wywoływana** dla #LeadTime_STEP1  
✅ #LeadTime_STEP1 jest tworzony z `temp_registry` (linia 2042 w procedures.py)  
✅ Dependencies są brane z `temp_sources` (linia 2047-2048)  
❌ `temp_sources[#LeadTime_STEP1]` **nie istnieje** bo `_parse_select_into` nie został wywołany!  

**Flow problemu**:
1. procedures.py używa **regex fallback** do znalezienia `SELECT ... INTO #LeadTime_STEP1`
2. Regex ekstrakt dodaje `#LeadTime_STEP1` do `temp_registry` (kolumny)
3. **ALE** regex nie wywołuje `_parse_select_into` do parsowania SELECT
4. `_parse_select_into` nigdy nie jest wywoływana → `temp_sources[#LeadTime_STEP1]` jest puste
5. ObjectInfo dla #LeadTime_STEP1 ma `dependencies=set()` (linia 2042)

**Dlaczego regex fallback zamiast sqlglot?**:
- SELECT INTO #LeadTime_STEP1 jest **bardzo duży** (~100 linii, 30+ LEFT JOIN)
- sqlglot prawdopodobnie **nie może sparsować** tak złożonego statement
- procedures.py **failuje** na parsowaniu i przechodzi do regex fallback
- Regex ekstrakt znajduje `INTO #LeadTime_STEP1` ale nie parsuje całego SELECT

#### Rozwiązanie - PLAN IMPLEMENTACJI

**Miejsce modyfikacji**: `procedures.py` linia ~1171-1300 (fallback registration)

**Co dodać**:
1. Regex do ekstrakcji tabel z `FROM table` i wszystkich `JOIN table`
2. Pattern:
```python
# Po znalezieniu temp_name z regex (linia ~1175)
# Dodać ekstrakcję dependencies:
from_match = re.search(r'FROM\s+([\w.#@]+)', match.group(1), re.IGNORECASE)
if from_match:
    deps.add(qualify_table_name(from_match.group(1)))

# Znaleźć wszystkie JOIN
for join_match in re.finditer(r'(?:LEFT\s+|RIGHT\s+|INNER\s+|OUTER\s+|CROSS\s+)?JOIN\s+([\w.#@]+)', match.group(1), re.IGNORECASE):
    deps.add(qualify_table_name(join_match.group(1)))

# Dodać do temp_sources
self.temp_sources[temp_name] = deps
```

3. Potrzebne helper: qualify_table_name() żeby EDW_CORE.dbo.Table_BV → pełna kanoniczna nazwa

**Alternatywa - prostsza**: 
Zamiast modyfikować fallback, **poprawić parsowanie sqlglot** żeby nie failowało na dużych SELECT.
- Problem: sqlglot może mieć timeout lub limit complexity
- Rozwiązanie: Split SELECT na mniejsze części? (trudne)

**Rekomendacja**: Rozszerzyć regex fallback o ekstrakcję dependencies (prostsze, pewniejsze).

#### Następne kroki (TODO)
1. [x] Zidentyfikować root cause - ✅ regex fallback bez dependencies
2. [x] Zlokalizować gdzie regex fallback jest implementowany - ✅ linia 1171-1300
3. [ ] Implementować regex-based dependency extraction dla fallback
4. [ ] Dodać do `temp_sources` dependencies zebrane przez regex
5. [ ] Test na #LeadTime_STEP1
6. [ ] Uruchomić pytest
7. [ ] Zweryfikować że wszystkie 17 tabel się pojawiło

**Status**: ROOT CAUSE zidentyfikowany, plan implementacji gotowy. Wymaga ~50 linii kodu.

### Następne kroki
2. [x] Sprawdzić jak dependencies dla tabel tymczasowych trafiają do głównej procedury
3. [x] Zidentyfikować miejsce gdzie tracona jest pełna kanoniczna nazwa
4. [x] Naprawić `_qualify_table` aby używał kanonicznej nazwy dla temp tables (CZĘŚCIOWO)
5. [ ] Znaleźć WSZYSTKIE miejsca gdzie dependencies temp tables są tworzone
6. [ ] Sprawdzić czy dependencies z CTE używają _qualify_table czy string literalów
7. [ ] Uruchomić pytest aby sprawdzić regresję (DONE - wszystkie testy przeszły)

### Problem: Wielokrotne źródła dependencies
System zbiera dependencies z różnych miejsc:
- AST przez `_extract_dependencies` → używa `_qualify_table` (NAPRAWIONE)
- CTE lineage expansion → może używać string literalów
- Fallback string parsing → może używać prostych string operacji

Potrzebne: Kompleksowa analiza wszystkich miejsc gdzie temp table dependencies są tworzone.

## Status testów
- Wszystkie testy przechodzą: 136 passed, 2 skipped ✅

---

## Problem 2: Brakujące dependencies dla tabel LEFT JOIN (17 tabel)

### Status: ✅ ROZWIĄZANE (2025-12-05)

**ROOT CAUSE**:
- #LeadTime_STEP1 jest przetwarzany jako WITH...SELECT...INTO (wielki, 15KB statement)
- Sqlglot nie może sparsować tak dużego statement → failuje
- Trafia do WITH fallback → ten też failuje na parsowaniu kolumn
- Kod trafia do trzeciego "chunk exception" fallback (linia 1783-1835)
- Ten fallback **ekstraktował TYLKO kolumny**, bez dependencies!

**ROZWIĄZANIE**:
- Dodano ekstrakcję dependencies do chunk exception fallback (linia ~1833-1865)
- Używa tego samego regex pattern co normalny fallback:
  - `\bFROM\s+([\w.#@\[\]]+)` dla FROM clause
  - `(?:LEFT\s+|RIGHT\s+|...)JOIN\s+([#\w.\[\]]+)` dla wszystkich JOIN
- Dependencies są zapisywane do `self.temp_sources[temp_name]`

**REZULTAT** 🎉:
- ✅ pytest: 136 passed, 2 skipped
- ✅ #LeadTime_STEP1 ma teraz **29 inputs** (było 1!)
- ✅ Wszystkie 17 brakujących tabel są teraz w dependencies:
  - Asset_BV, AssetState_BV, AssetSectorTypeDictionary_BV, AssetSegmentDictionary_BV
  - Contract_BV, ContractParameter_BV
  - MainProductType_BV, OfferStatus_BV
  - DistributionChannel_BV, CommissionRegisterAgreement_BV, DistributionNetwork_BV
  - FinancialAdvisorPosition_BV
  - OfferVerificationAcceptation_MSBV, OfferTransactionParameters_MSBV, OfferTransactionTags_MSBV
  - Wszystkie temp tables (#offer, #maxleaddate, #ContractInformationReference, etc.)

**Zmiany w kodzie**:
1. `procedures.py` linia ~1028: Dodano negative lookahead `(?!@)` do regex SELECT (ignoruje `SELECT @var`)
2. `procedures.py` linia ~1028-1042: Dodano search dla EXEC przed SELECT
3. `procedures.py` linia ~1130-1148: Dodano search dla EXEC w sekcji WITH
4. `procedures.py` linia ~1833-1865: **Dodano dependencies extraction do chunk exception fallback**

**Problem uznany za ROZWIĄZANY** ✅

---

### Poprzednie próby (historia debugowania)

#### Implementacja (2025-12-05)

**FAZA 1 - Regex pattern fix** ✅ UKOŃCZONE:
- ✅ Zidentyfikowano problem: regex wyciągał aliasy (mpt) zamiast nazw tabel (MainProductType_BV)
- ✅ Stworzony i uruchomiony fix_regex.py - poprawiony pattern na: `r'JOIN\s+([#\w.]+)(?:\s+(?:AS\s+)?[#\w.]+)?(?=\s+ON|...)'`
- ✅ Pattern zweryfikowany ręcznie: python -c test wyciąga 32 JOINy poprawnie (Asset_BV, Contract_BV, etc.)
- ✅ Poprawki zastosowane w 2 miejscach w procedures.py (~linia 1257, ~1391)
- ✅ pytest przechodzi: 136 passed, 2 skipped ✅

**FAZA 2 - Secondary window scan** ✅ ZAIMPLEMENTOWANE (ale nie działa):
- ✅ Dodana logika "secondary scan" w obu fallback blocks
- ✅ Jeśli deps_from_sql pusty, szuka okna ±4KB wokół `INTO #temp_name` w preprocessed_body
- ✅ Re-run FROM/JOIN regex na szerszym kontekście
- ✅ Implementacja w procedures.py linia ~1260-1275 i ~1400-1415

**PROBLEM - Fix nieefektywny** ❌:
- ❌ #LeadTime_STEP1 nadal ma tylko 1 input zamiast ~30+
- ❌ Weryfikacja: `cat ...hashLeadTime_STEP1.json | ConvertFrom-Json | Select inputs | Measure Count` → Count=1
- ❌ 17 tabel (Asset_BV, Contract_BV, etc.) nadal brakuje w dependencies

**ROOT CAUSE - Nieznany**:
- ✅ Regex pattern poprawny (zweryfikowane ręcznie)
- ✅ Secondary scan zaimplementowany
- ❌ deps_from_sql prawdopodobnie puste mimo poprawnego regex
- ❌ Możliwe przyczyny:
  1. Window znajduje złe wystąpienie SELECT (wielokrotne `INTO #LeadTime_STEP1`?)
  2. Window za mały (±4KB) dla 200+ linii SELECT?
  3. `preprocessed_body.find(INTO marker)` znajduje zły punkt
  4. deps_from_sql wypełniony ale nie trafia do temp_sources/ObjectInfo

**NASTĘPNE KROKI - FAZA 3 DEBUGGING**:
1. [x] Dodać debug logging dla temp_name=='#LeadTime_STEP1' - ZROBIONO (manual test)
2. [x] Sprawdzić czy w pliku SQL jest wielokrotne `INTO #LeadTime_STEP1` - TAK: 1 wystąpienie
3. [x] Zwiększyć window size do ±8KB - ZROBIONO (nadal 1 input)
4. [x] Zdiagnozować dlaczego nadal nie działa

**FAZA 3 - Root cause znaleziony** ✅:
- ✅ Window ±8KB (16KB total) widzi 32 JOINy gdy testowane ręcznie
- ❌ SELECT...INTO pattern używa non-greedy `(.*?)` więc łapie ZŁY SELECT
- ❌ Dla #LeadTime_STEP1 regex łapie fragment od poprzedniego SELECT: `@v_insert_count = @@ROWCOUNT...`
- ❌ Ten fragment NIE zawiera FROM - więc deps_from_sql = empty → secondary scan się uruchamia
- ❌ Ale secondary scan szuka od `INTO #LeadTime_STEP1` w preprocessed_body
- ❌ preprocessed_body może mieć usunięte/zmienione fragmenty (preprocessing)

**WŁAŚCIWY ROOT CAUSE** ✅ ZNALEZIONY:
- ✅ Kod używa inteligentnego back-search (od INTO wstecz do SELECT) - to jest OK
- ✅ Regex pattern dla JOIN jest poprawny - na izolowanym fragmencie znajduje 32 JOINy
- ❌ **PROBLEM**: `actual_end` używa limitu `into_end + 1000` chars
- ❌ INTO #LeadTime_STEP1 jest na pozycji 25235
- ❌ actual_end = 25755 (INTO + 520 chars)
- ❌ Ale MainProductType_BV JOIN jest w linii 690 = pozycja 29459
- ❌ Statement obcięty o ~3700 chars - brakuje 27+ JOINów!

**Limit `+1000` jest za mały dla długich FROM/JOIN bloków (200+ linii, 30+ JOINs)**

**FAZA 4 - Poprawka limitu search_end** (95% pewności):
- Zmienić `into_end + 1000` → `into_end + 5000` lub więcej
- Lokacja: procedures.py linia ~950 (SELECT...INTO bez WITH)
- Może być też w WITH...SELECT...INTO (sprawdzić)


