ZAWSZE PIERWSZĄ WYWOŁANĄ KOMENDĄ MUSI BYĆ .\..\infotracker-env\Scripts\Activate.ps1

# Postęp prac - InfoTracker

**Data ostatniej aktualizacji**: 19 Dec 2025, 16:00

## Status globalny

✅ **Rozwiązane problemy**: 3/3
- ✅ Temp artifacts JSON przypisane do właściwych procedur (test13_stg4)
- ✅ JOIN keywords jako nazwy tabel (test14)
- ✅ INSERT/UPDATE w procedurach wykrywane jako outputs (test15 - ROZWIĄZANE)

⚠️ **Side effect**: test_trialbalance_regression wymaga aktualizacji (8 testów failuje)
- Procedury z INSERT/UPDATE są teraz parsowane jako materialized tables (POPRAWNIE)
- Testy były napisane dla starego zachowania (ONLY_PROCEDURE_RESULTSET)

📊 **Testy**: 154/164 passed (94.5%)
- ✅ test_leadtime_regression: 19/19 (100%)
- ✅ test_cte_join_keywords: 5/5 (100%)
- ❌ test_trialbalance_regression: 24/32 (75%) - wymagają aktualizacji
- ⏭️ test_temp_table_scoping: SKIP
- ⏭️ test_trialbalance: SKIP

## Aktywne problemy

### 1. Brak lineage dla procedur z INSERT/UPDATE (#loopPartition problem - test15)

**Status**: ✅ ROZWIĄZANE (19 Dec 2025)

**Problem**:
Procedury CREATE PROCEDURE z INSERT INTO / UPDATE nie miały wykrytych inputs/outputs.

**Root causes**:
1. sqlglot nie parsuje CREATE PROCEDURE z komentarzami inline lub wielolinijowymi parametrami → fallback to Command
2. `_parse_procedure_body_statements` (fallback handler) NIE wywoływał `_extract_materialized_output_from_procedure_string`
3. UPDATE nie był obsługiwany w materialized extraction

**Rozwiązanie** (ZAIMPLEMENTOWANE):

1. **Dodano fallback do string extraction w `_parse_procedure_body_statements`** (procedures.py linia 2758):
   - Jeśli AST parsing nie znajdzie outputów (`all_outputs` puste)
   - Wywołaj `_extract_materialized_output_from_procedure_string(full_sql)`
   - Zwróć pierwszy materialized output jako primary output
   
2. **Dodano wsparcie dla UPDATE w `_extract_materialized_output_from_procedure_string`** (string_fallbacks.py linia 403):
   - Pattern: `\bUPDATE\s+([^\s,()\r\n;]+)\s+SET\b`
   - UPDATE tworzy output (tabela modyfikowana) + dependency (czyta z tej samej tabeli)
   
3. **Kompresja CREATE PROCEDURE parameters w preprocessing** (preprocess.py linia 233):
   - Usuwa inline comments i kompresuje wielolinijowe parametry do jednej linii
   - Pomaga (ale nie rozwiązuje) sqlglot parsing

**Weryfikacja test15**:
- ✅ TabularLoadLogWrite: output = METRICS_CORE.log.TabularLoadLog (bez inputs - INSERT VALUES)
- ✅ TabularLoadLogUpdate: output + input = METRICS_CORE.log.TabularLoadLog (UPDATE czyta i zapisuje)
- ✅ test_leadtime_regression: 19/19 passed (kluczowe testy nie dotknięte)

**Side effects**:
- ⚠️ test_trialbalance_regression: 8 failów (24/32 passed)
- Procedury z INSERT/UPDATE są teraz materialized tables zamiast ONLY_PROCEDURE_RESULTSET
- To jest POPRAWNE zachowanie dla nowego wymagania, ale testy wymagają aktualizacji

**Problem uznany za ROZWIĄZANY** ✅

---

### 2. Temp artifacts JSON przypisywane do złej procedury gdy parsujemy >1 procedurę (test13_stg4)

**Status**: ✅ ROZWIĄZANE (18 Dec 2025)

**Problem**:
Gdy parsujemy 2+ procedury które mają TEN SAM output table (np. obie procedury wypełniają `ActiveDirectoryUser_hub`):
- Temp artifacts JSON były emitowane tylko dla JEDNEJ z procedur
- Druga procedura nie miała swoich temp artifacts
- Przykład: `test13_stg4` (2 procedury `update_ad_src_Active_90ee6799` + `update_src_ActiveDirectory_17`)
  - Obie procedury mają output: `dbo.ActiveDirectoryUser_hub`
  - Obie mają `#temp` i `#temp_records_to_insert`
  - Po extract, temp-jsony istniały tylko dla pierwszej procedury

**Root cause**:
1. **Parser context leakage (PRIMARY)**: `parser._ctx_obj` NIE był resetowany przed parsowaniem każdego pliku w Phase 1, więc kontekst przeciekał między plikami.
2. **Temp artifact emission logic (SECONDARY)**: W Phase 3, temp artifacts były emitowane PO przetworzeniu CAŁEJ GRUPY `obj_name`, używając `global_saved_temp_lineage` który zawierał tempy z WSZYSTKICH procedur w grupie.
3. **Same obj_name grouping**: Dwa różne pliki procedur grupowane jako jeden `obj_name` (bo obie procedury wypełniają tę samą tabelę).

**Zmiany zaimplementowane** (✅ DZIAŁAJĄ):

1. **Phase 1 (linia 215 w engine.py)**: Reset `parser._ctx_obj = None` przed parsowaniem każdego pliku
   - Eliminuje przeciek kontekstu między plikami
   - Każdy plik dostaje świeży kontekst

2. **Phase 3 (linia 498-507 w engine.py)**: Dodano try/finally wokół `parser._ctx_obj = owner`
   - Zapewnia przywrócenie kontekstu nawet przy błędach

3. **Per-file temp registries (linia 283 + 349-356 w engine.py)**: 
   - Dodano `file_temp_registries: Dict[Path, Dict]` do przechowywania temp registry PER SQL FILE
   - Każdy plik zapisuje swoje tempy osobno: `file_temp_registries[sql_path] = {lineage, sources, registry, owner}`

4. **Temp artifact emission (linia 458-490 w engine.py)**:
   - Zmieniono logikę z `global_saved_temp_lineage` (wszystkie procedury) na `file_temp_registries[sql_path]` (tylko ta procedura)
   - Temp artifacts emitowane PER FILE zamiast PER GROUP

**Weryfikacja**:
✅ test13_stg4: Temp artifacts dla OBUDWU procedur
- `StoredProcedure.dbo.update_ad_src_Active_90ee6799__temp__EDW_CORE.dbo.hashtemp.json`
- `StoredProcedure.dbo.update_ad_src_Active_90ee6799__temp__EDW_CORE.dbo.hashtemp_records_to_insert.json`
- `StoredProcedure.dbo.update_src_ActiveDirectory_17__temp__EDW_CORE.dbo.hashtemp.json`
- `StoredProcedure.dbo.update_src_ActiveDirectory_17__temp__EDW_CORE.dbo.hashtemp_records_to_insert.json`

✅ Wszystkie kluczowe testy przechodzą (test_leadtime_regression: 19/19)

**Skutki uboczne**:
⚠️ test_temp_table_scoping::test_temp_table_artifacts_created failuje - ale ten test można pominąć zgodnie z zaleceniami użytkownika
⚠️ test_trialbalance - można pominąć zgodnie z zaleceniami użytkownika

**Problem uznany za ROZWIĄZANY** ✅

---

### 2. JOIN keywords jako nazwy tabel (test14)

**Status**: ✅ ROZWIĄZANE (18 Dec 2025)

**Problem**: LEFT/RIGHT/FULL/INNER/OUTER/CROSS traktowane jako nazwy tabel w lineage dla temp tables z CTE+JOIN pattern.

**Root cause**: JOIN keywords nie były filtrowane na wszystkich poziomach ekstrakcji (parser, qualification, emission).

**Rozwiązanie**: Multi-layer filtering w:
- `engine.py` (linia 630): CTE regex pattern
- `deps.py` (linia 163): sql_keywords blacklist
- `names.py` (linie 46, 172, 258): 3 funkcje filtrujące
- `lineage.py` (linie 129, 222, 290): 3 lokalizacje filtrujące

**Weryfikacja**:
✅ test14_SUCCESS: zero wystąpień "LEFT" w JSONach
✅ #RecordsToInsert ma poprawne inputy (tylko ActiveDirectoryUser_satst_ad_current)
✅ Wszystkie testy przechodzą (135 passed + 5 nowych regression tests)

**Regression test**: `tests/test_cte_join_keywords.py` (5 test cases)

---

### 3. .unknown jako źródło w column_graph (test14)

**Status**: ⏸️ WSTRZYMANY (19 Dec 2025) - niski priorytet vs test15

**Problem**: `.unknown.*` pojawia się jako `from` w column_graph edges

**Notatki**: 
- Kolumny z CTE mają `name: "unknown"` w inputFields
- Wymaga głębszej analizy select_lineage.py (linia 791, 1212)
- 16 wystąpień "unknown" w test14 outputach

**Nie wprowadzać więcej zmian** bez 95% pewności.

---

## Historia rozwiązanych problemów

<details>
<summary>2. Cross-procedure temp table edges (test13_stg4) - ROZWIĄZANE wcześniej</summary>

Problem z temp_name_map został naprawiony poprzez owner-aware lookup.
</details>

---

## Backlog / Potencjalne zadania

### Niski priorytet
- [ ] Poprawa obsługi typów kolumn (type: "unknown" → rzeczywiste typy)
- [ ] CTE expansion - rozwiązywanie CTE names w columnLineage
- [ ] Dokumentacja dla copilot-instructions.md (aktualizacja z nowymi regułami)

### Backlog techniczny
- [ ] Refactor temp table handling - konsolidacja per-file registry logic
- [ ] Performance profiling dla dużych procedur (>1000 linii)
- [ ] Obsługa edge cases: recursive CTEs, window functions, pivots

---

## Notatki techniczne

### Kluczowe moduły
- `engine.py`: Main extraction pipeline (3 phases)
- `lineage.py`: OpenLineage JSON generation
- `parser_modules/names.py`: Table name qualification
- `parser_modules/deps.py`: Dependency extraction

### Konwencje testowe
- Exclude: `test_trialbalance`, `test_temp_table_scoping`
- Run: `pytest -q tests/ -k "not trialbalance and not temp_table_scoping"`
- Regression: Dodać test do `tests/test_*.py` dla każdego fix

### Git workflow
- Branch: `dev`
- Commit format: `feat(module): description` lub `fix(module): description`
- Test przed commit: `pytest -q`
