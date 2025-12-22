# Notatnik - InfoTracker

## ✅ ROZWIĄZANY: Wildcard w SELECT INTO (2024-12-22, Fixed 2025-01-22)

**Problem:** Temp tables tracą kolumny gdy SQL używa wildcards `SELECT offer.* INTO #temp` lub `SELECT * INTO #temp`

**Postęp:**
- ✅ STEP2: 4 → **21 kolumn** (wildcard `offer.*` rozwinięty)
- ✅ STEP3: 18 → **48 kolumn** (wildcard `offer.*` rozwinięty)
- ✅ STEP4: 4 → **52 kolumny** (wildcard `*` rozwinięty + 4 nowe CASE AS)

**Fix (2025-01-22):**

1. **Regex non-greedy bug**: `SELECT\s+(.*?)\s+INTO` matchował poprzednie SELECT INTO
   - **Fix**: Użyto negative lookahead `(?:(?!INTO).)*` w liniach 359, 1595, 2058
   
2. **Wildcard `*` skipowana**: `len(col_expr) < 2` pomijał pojedynczy znak
   - **Fix**: Dopuszczono `col_expr != '*'` w linii 2084
   
3. **CASE expressions skipowane**: multi-line `CASE...END AS alias` miały newlines
   - **Fix**: Ekstrakcja aliasu przez regex `\bAS\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*$` (linie 2095-2107)

**Wyniki:**
- ✅ pytest: 119→154 passed (+35 testów naprawionych!)
- ✅ STEP4 teraz ma 52 kolumny (48 z `*` + 4 nowe)
- ✅ Wildcard expansion działa dla wszystkich wzorców: `offer.*`, `*`, `CASE...END AS`

**Commit**: `fix(parser): wildcard expansion for 'SELECT *, new_cols INTO #temp'`
2. [x] Sprawdzić `_infer_table_columns_unified(#LeadTime_STEP3)` → ✅ ZNAJDUJE 39 kolumn
3. [x] Sprawdzić czy `_has_star_expansion` jest wywoływana → NIE jest
4. [x] Znaleziono błąd: `logger` not defined w select_lineage.py → NAPRAWIONO
5. [ ] NADAL problem: `_extract_column_lineage` NIE jest wywoływana dla STEP4
6. [ ] Możliwe że jest wiele wywołań _parse_select_into i ostatnie nadpisuje wynik

**Debugging progress (2024-12-22 16:00):**
- ✅ `_infer_table_columns_unified` działa - znajduje 39 kolumn dla #STEP3
- ✅ sqlglot parsuje STEP4 poprawnie, pierwsza projekcja to exp.Star
- ❌ `_extract_column_lineage` NIE jest wywoływana (mimo że _parse_select_into jest)
- ❌ ERROR w select_lineage.py: `logger` not defined → naprawiono import
- ❌ NADAL temp_registry[STEP4] = 4 kolumny zamiast 43

**Hipoteza:**
- `_parse_select_into` jest wywoływana 2 razy dla STEP4
- Pierwsze wywołanie (sqlglot) mogło by rozwinąć wildcards
- Drugie wywołanie (regex fallback) NADPISUJE wynik tylko 4 kolumnami
- Trzeba sprawdzić czy jest więcej niż jedno wywołanie dla #LeadTime_STEP4

**Rozwiązanie częściowe (działa dla STEP2 i STEP3):** Dodano `_expand_wildcard_columns()` helper w procedures.py, która:
- Wykrywa wildcards (`*` lub `table.*`)
- Rozwiązuje alias do nazwy tabeli z FROM clause  
- Pobiera kolumny z temp_registry
- Zwraca rozwiniętą listę
- Naprawiono 3 miejsca w procedures.py gdzie ekstraktowane są kolumny

<details>
<summary>Szczegóły diagnozy i implementacji</summary>

### Root cause
Regex fallback w procedures.py ekstraktował tylko nazwę aliasu (`'step'` z `'offer.* --comment'`) zamiast rozwijać wildcard do wszystkich kolumn.

### Analiza SQL
STEP2 i STEP3 używają `SELECT offer.* INTO #temp`, ale parser nie rozwijał wildcards.

### Miejsca naprawione w procedures.py:
1. WITH ... SELECT INTO fallback (~linia 1417)
2. SELECT INTO bez WITH fallback (~linia 1595)
3. Chunk exception fallback (~linia 2064)

### Pliki zmienione:
- src/infotracker/parser_modules/procedures.py: dodano `_expand_wildcard_columns`, naprawiono 3 sekcje
- src/infotracker/parser.py: dodano delegację dla `_expand_wildcard_columns`

</details>

---

## Historia sesji (chronologicznie od najnowszych):

### Status: WERYFIKACJA SQL - WILDCARDS POTWIERDZONE ✅

Analiza rzeczywistego SQL z `update_stage_mis_LeadTime`:

**STEP1** (linia ~1550): Explicite definiuje wszystkie kolumny - brak wildcards
```sql
SELECT
  , offer.SourceSystem
  , offer.Key_Offer
  ... (wszystkie kolumny nazwane)
INTO #LeadTime_STEP1
```

**STEP2** (linia ~2100): **UŻYWA WILDCARD** `offer.*`
```sql
SELECT
    offer.* --all attributes from previous step
    , CASE sourcesystem...
INTO #LeadTime_STEP2
FROM #LeadTime_STEP1 AS offer
```

**STEP3** (linia ~2200): **UŻYWA WILDCARD** `offer.*`
```sql
SELECT
    offer.* -- all attributes from previous step
    , CASE offer.sourcesystem...
INTO #LeadTime_STEP3
FROM #LeadTime_STEP2 offer
```

### Oczekiwane zachowanie:
- STEP1: 70+ kolumn explicite zdefiniowanych
- STEP2: 70+ z STEP1 + 9 nowych (OfferDecision, IsIndividualDecision, TTAgrEndDate, TTAssetEndDate, TTYPartyStatementBIKDate, TTY2StartDate, IsSMTLimit, IsMBTLimit, IsMBT)
- STEP3: wszystkie z STEP2 + ~18 nowych (TTYDecision, IsAssetAvailable, IsReported, SLA targets, Working hours/days)

### Aktualny problem POTWIERDZONY:
- `temp_registry['#LeadTime_STEP2']` ma tylko 4 kolumny zamiast 70+
- `temp_registry['#LeadTime_STEP1']` ma tylko 17 kolumn zamiast 70+
- Wildcard `offer.*` w SQL STEP2 i STEP3 nie jest rozwijany przed zapisem do `temp_registry`

### ZNALAZŁEM PROBLEM! ✅✅✅

Root cause: `procedures.py` regex fallback nie rozwija wildcards w SELECT INTO

**Szczegóły:**
1. SQL: `SELECT offer.* --comment, 'D' AS ColD INTO #STEP2`
2. Regex ekstraktuje: `'offer.* --comment'` i `''D' AS ColD'`
3. Dla pierwszej kolumny:
   - `parts = 'offer.* --comment'.split()` → `['offer.*', '--comment', ...]`
   - Bierze ostatni part jako implicit alias → `'step'` (z końca komentarza!)
4. Zamiast rozwinąć `offer.*` do wszystkich kolumn z `#STEP1`, zapisuje `['step', 'ColD']` do temp_registry

**Dlaczego sqlglot nie pomaga:**
- sqlglot MA funkcję `_handle_star_expansion` która rozszerza wildcards
- ALE `procedures.py` string fallback używa REGEX zamiast AST parsera
- String fallback OMIJA cały pipeline sqlglot

### ROZWIĄZANIE - PLAN NAPRAWY:

**Option A: Napraw regex fallback (preferowane - szybsze)**
1. W procedures.py linia ~1340: przed splittem kolumn, usuń komentarze `--` i `/* */`
2. Po ekstraktowaniu każdej kolumny, sprawdź czy to wildcard (`*` lub `table.*`)
3. Jeśli wildcard:
   - Wyciągnij nazwę tabeli (dla `offer.*` → `offer`)
   - Resolve `offer` do prawdziwej nazwy tabeli używając FROM clause
   - Pobierz kolumny z `temp_registry[table_name]` lub `_infer_table_columns_unified(table_name)`
   - Zastąp wildcard rozwiniętą listą kolumn
4. Zapisz rozwinięte kolumny do temp_registry

**Option B: Użyj AST parsera zamiast regex (bezpieczniejsze ale wolniejsze)**
1. Spróbuj najpierw sparsować SELECT INTO używając sqlglot
2. Jeśli się uda, użyj `_parse_select_into` który już obsługuje wildcards przez `_handle_star_expansion`
3. Tylko jeśli parsing AST zawiedzie, użyj regex fallback z Option A

### STATUS: IMPLEMENTATION - IMPLEMENTUJĘ OPTION A

### Problem z pierwszym podejściem:
- Dodałem rozwijanie wildcards w `dml.py::_parse_select_into` (linia ~201)
- To zadziałało (`After wildcard expansion, temp_cols has 5 columns`) 
- ALE regex fallback w `procedures.py` (linia ~1549) NADPISUJE temp_registry PÓŹNIEJ
- Fallback ekstraktuje kolumny z SQL string i zapisuje do temp_registry, ignorując moje rozwinięcia

### Nowe podejście (bardziej eleganckie):
Zamiast rozwijać wildcards podczas ZAPISU do temp_registry, będę je rozwijać podczas ODCZYTU z temp_registry.
- W `parser.py::_infer_table_columns_unified` (linia ~846)
- Gdy funkcja ta czyta kolumny z temp_registry i znajdzie wildcard (np. `offer.*`)
- Rozwinie go rekurencywnie pobierając kolumny z tabeli źródłowej

**ZALETAOWARZEM: Zadziała dla WSZYSTKICH przypadków (nie tylko _parse_select_into), w tym dla fallbacków**

### Główny problem:
Gdy parser obsługuje `SELECT offer.* FROM #LeadTime_STEP1 offer INTO #LeadTime_STEP2`:
1. **KROK 1**: Parser ekstraktuje kolumny z SELECT używając `_extract_column_references` lub podobnej funkcji
2. **KROK 2**: Te kolumny są zapisywane do `temp_registry['#LeadTime_STEP2']`
3. **❌ PROBLEM**: Wildcard `offer.*` NIE JEST rozwijany w KROKU 1, więc do temp_registry trafia tylko `['offer.*']` zamiast pełnej listy kolumn
4. **KROK 3**: Później gdy kolejny SELECT używa `FROM #LeadTime_STEP2`, parser próbuje znaleźć kolumny w `temp_registry['#LeadTime_STEP2']` ale tam jest tylko `['offer.*']` 

### Dowody:
```
Temp registry keys:
  #LeadTime_STEP1: 17 columns (powinno być ~40)
  #LeadTime_STEP2: 4 columns (powinno być ~40 + nowe)
  #LeadTime_STEP3: 18 columns (powinno być wszystkie poprzednie + nowe)
  #LeadTime_STEP4: 4 columns (ale final INSERT ma 89 bo są wszystkie explicite)
```

Lineage pokazuje że `IsMBT` ma input: `dbo.update_stage_mis_LeadTime#LeadTime_STEP1.*` - to NIE jest nazwa kolumny, to jest WILDCARD który nie został rozwinięty!

### Lokalizacja kodu:
1. `src/infotracker/parser_modules/dml.py` linia ~201: `self.temp_registry[simple_key] = temp_cols`
   - `temp_cols` pochodzi z `output_columns` z SELECT
2. `src/infotracker/parser_modules/select_lineage.py` linia ~862: `_handle_star_expansion`  
   - Ta funkcja POTRAFI rozwijać wildcard ale jest wywoływana tylko dla VIEW/CREATE TABLE AS SELECT
   - NIE jest wywoływana dla SELECT INTO temp tables!
3. `src/infotracker/parser.py` linia ~846: `_infer_table_columns_unified`
   - Używana do rozwijania wildcard podczas czytania kolumn
   - Sprawdza `temp_registry` po `simple_name`

### Rozwiązanie:
Musimy upewnić się że gdy parser ekstraktuje kolumny z `SELECT ... INTO #temp`, to wildcards są rozwijane PRZED zapisem do temp_registry.

Opcja A: W `_parse_select_into` przed zapisem do temp_registry, przejść przez `output_columns` i rozwinąć wszystkie wildcards
Opcja B: Zmienić `_extract_column_references` żeby automatycznie rozwijał wildcards dla tabel temp
Opcja C: W `_handle_star_expansion` dodać obsługę dla SELECT INTO (nie tylko VIEW)

**PREFEROWANA: Opcja A** - najbezpieczniejsza, nie łamie istniejącego kodu.

### Następne kroki DO IMPLEMENTACJI:
1. [ ] Edytować `src/infotracker/parser_modules/dml.py::_parse_select_into`
2. [ ] Po linii gdzie tworzone są `output_columns` z SELECT (przed zapisem do temp_registry)
3. [ ] Dodać kod który iteruje przez `output_columns` i dla każdego wildcard:
   - Wykrywa czy kolumna to wildcard (nazwa to `*` lub kończy się na `.*`)
   - Używa `_infer_table_columns_unified` żeby pobrać kolumny źródłowej tabeli
   - Rozwijakolumnę wildcard do pełnej listy kolumn
4. [ ] Upewnić się że `temp_cols` (linia ~201) zawiera pełną rozwiniętą listę bez wildcards
5. [ ] Uruchomić `pytest` żeby sprawdzić że nic się nie zepsuło
6. [ ] Uruchomić `python analyze_leadtime.py` żeby zweryfikować że teraz wszystkie STEP mają pełne listy kolumn

### UWAGA dla następnej sesji:
- Problem jest DOBRZE ZDIAGNOZOWANY
- Wiemy GDZIE naprawić (dml.py, funkcja _parse_select_into)
- Wiemy JAK naprawić (rozwinąć wildcards przed zapisem do temp_registry)
- Mamy narzędzie do testowania (analyze_leadtime.py)

### Historia zmian:
- 2024-12-22: Rozpoczęto analizę problemu z lineage tabel tymczasowych  
- 2024-12-22: Uruchomiono pytest - wszystkie testy OK poza jednym pre-existing failure
- 2024-12-22: ZNALEZIONO PROBLEM - wildcard nie jest rozwijany dla tabel tymczasowych
- 2024-12-22: SZCZEGÓŁOWA DIAGNOZA - problem w _parse_select_into, wildcards nie są rozwijane przed zapisem do temp_registry



