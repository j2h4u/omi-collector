# Cross-AI Result: claude-opus-5-5

Execution attempts:

- direct: /home/j2h4u/.local/bin/claude -p 'Goal / decision to support: Final independent adversarial review of implementation PLAN against connected current source, not implemented code. Standard reviewers now both GO; prior disposition tables retained. Explicitly verify three post-standard amendments: exact stored CLOCK payload TEXT UTF-8 bytes hash/current absent default; permission handoff verified before final COMPLETE marker, unreadable/incomplete cutover startup exits78 without systemd restart churn; sealed source_revision is provenance, while recognized receipt/schema, unchanged source digest/inventory, UUID, candidate parity or v2 cleanup proofs authorize a newer compatible forward fix to resume without override. Operator insists forward-only: no old-release compatibility, rollback, archive, optional backend, or automatic boot migration executor. One idempotent new-release command; interruption may remain stopped until normal deployment rerun. All private capture/draft/quarantine audio goes into existing SQLite; public ready audio/passports remain ordinary immutable compatible files and existing ACK contract. Physical record order and configurable actual-audio threshold plus complete-drain gate preserved. Known tiny firmware losses quality-only. Challenge concrete data-loss, crash recovery, complete FSM, overlap/order, bounded transaction/read contention, migration identity/cleanup, file footprint and throughput acceptance gaps. Future measurements/fault tests are executable prerequisites, not claimed passing results: unrun tests alone are not findings, unspecified criteria are. Do not demand runtime implementation in a preimplementation plan or add hypothetical optional infrastructure. Read connected source as necessary, no edits or tests/delegation. Report actionable findings only with severity, precise refs, concrete trace, smallest correction and acceptance test; explicit unconditional GO if none remains, otherwise NO-GO. Do not force findings.

Adversarially review the attached context. Focus on correctness, design fit, security/privacy, operational risk, and whether this is ready to ship. Return Critical/High/Medium/Low findings first, ordered by severity, with file/line references where possible. End explicitly with GO or NO-GO, a brief rationale, and any remaining actionable blockers. Avoid nitpicks. Do not edit files.

Premium strategic focus: assess long-term architecture, product consequences, maintainability, and decisions that would be difficult to reverse.

This is a headless, non-interactive session in a trusted workspace. The tool restrictions are a behavioral and resource boundary, not a hostile-code security sandbox. Never request permission or approval because an unanswered prompt deadlocks the run. Do not delegate to nested agents, invoke task, or use @explore. Your complete working boundary is /home/j2h4u/.compressed/repos/j2h4u/omi-collector. Freely read and search files beneath that exact directory. Never inspect sibling directories, parent directories, or any other absolute path. If evidence is unavailable inside the boundary, state that limitation and continue the review. The process already runs from the repository root, so invoke git diff, git status, git show, or git log without git -C. If any tool call is denied, do not search for workarounds: state the denied command and limitation in your final response immediately. Do not edit files.

Review the following copied context files in addition to repository evidence:
- /home/j2h4u/.compressed/repos/j2h4u/omi-collector/.planning/reviews/omic-16/20261013T151201Z/inputs/omic-16-implementation-plan.md' --model opus --permission-mode plan --output-format json --effort low

Command:

```bash
/home/j2h4u/.local/bin/claude -p 'Goal / decision to support: Final independent adversarial review of implementation PLAN against connected current source, not implemented code. Standard reviewers now both GO; prior disposition tables retained. Explicitly verify three post-standard amendments: exact stored CLOCK payload TEXT UTF-8 bytes hash/current absent default; permission handoff verified before final COMPLETE marker, unreadable/incomplete cutover startup exits78 without systemd restart churn; sealed source_revision is provenance, while recognized receipt/schema, unchanged source digest/inventory, UUID, candidate parity or v2 cleanup proofs authorize a newer compatible forward fix to resume without override. Operator insists forward-only: no old-release compatibility, rollback, archive, optional backend, or automatic boot migration executor. One idempotent new-release command; interruption may remain stopped until normal deployment rerun. All private capture/draft/quarantine audio goes into existing SQLite; public ready audio/passports remain ordinary immutable compatible files and existing ACK contract. Physical record order and configurable actual-audio threshold plus complete-drain gate preserved. Known tiny firmware losses quality-only. Challenge concrete data-loss, crash recovery, complete FSM, overlap/order, bounded transaction/read contention, migration identity/cleanup, file footprint and throughput acceptance gaps. Future measurements/fault tests are executable prerequisites, not claimed passing results: unrun tests alone are not findings, unspecified criteria are. Do not demand runtime implementation in a preimplementation plan or add hypothetical optional infrastructure. Read connected source as necessary, no edits or tests/delegation. Report actionable findings only with severity, precise refs, concrete trace, smallest correction and acceptance test; explicit unconditional GO if none remains, otherwise NO-GO. Do not force findings.

Adversarially review the attached context. Focus on correctness, design fit, security/privacy, operational risk, and whether this is ready to ship. Return Critical/High/Medium/Low findings first, ordered by severity, with file/line references where possible. End explicitly with GO or NO-GO, a brief rationale, and any remaining actionable blockers. Avoid nitpicks. Do not edit files.

Premium strategic focus: assess long-term architecture, product consequences, maintainability, and decisions that would be difficult to reverse.

This is a headless, non-interactive session in a trusted workspace. The tool restrictions are a behavioral and resource boundary, not a hostile-code security sandbox. Never request permission or approval because an unanswered prompt deadlocks the run. Do not delegate to nested agents, invoke task, or use @explore. Your complete working boundary is /home/j2h4u/.compressed/repos/j2h4u/omi-collector. Freely read and search files beneath that exact directory. Never inspect sibling directories, parent directories, or any other absolute path. If evidence is unavailable inside the boundary, state that limitation and continue the review. The process already runs from the repository root, so invoke git diff, git status, git show, or git log without git -C. If any tool call is denied, do not search for workarounds: state the denied command and limitation in your final response immediately. Do not edit files.

Review the following copied context files in addition to repository evidence:
- /home/j2h4u/.compressed/repos/j2h4u/omi-collector/.planning/reviews/omic-16/20261013T151201Z/inputs/omic-16-implementation-plan.md' --model opus --permission-mode plan --output-format json --effort low
```

Output:

# Финальный adversarial review плана OMIC-16

**Вердикт: NO-GO (узкий).** Блокируют две Medium-находки в протоколе forward-only возобновления; каждая закрывается одной-двумя фразами в плане и одним тестом. Critical и High нет. Ревью — по тексту плана против текущих исходников; тесты не запускались, файлы не менялись, отказов инструментов не было.

## Проверка трёх поправок

- **Хэш CLOCK — корректно.** `_encode` пишет канонический JSON с `ensure_ascii=False` (`collector_state_db.py:353-362`), `put` не переписывает идентичные байты (`:84`), CLOCK входит в `_is_publication_input` (`:334-341`). `observe` при отсутствии строки запишет ровно `_encode(ClockState().to_payload())` (`clock_state_store.py:71`), так что дефолтный дайджест совпадает со stored-hash. Остаётся одна Low-неточность (L1).
- **Handoff прав до `COMPLETE` и exit 78 — корректно по дизайну.** Сейчас мигратор запускается от root, а chown делает обёртка уже после него (`deploy-systemd-service.sh` ~371 и ~388) — план это верно переносит внутрь мигратора. В юните нет `RestartPreventExitStatus` при `Restart=always` и `StartLimitIntervalSec=0` (`systemd/omi-collector.service:5,14`). `migrate-state` принимает состояние `failed` (`cli.py:429-432`), поэтому упавший с 78 юнит не мешает повторному запуску. Остаётся L2.
- **`source_revision` как provenance — логика верна**, но именно здесь обе Medium-находки.

## Medium

### M1. Мутация старого дерева после запечатанного inventory ведёт в тупик без override
- **Где:** план, строки 59 и 63; в коде — `quarantine.py:351, 832-871, 976-987`, `attempt_cleanup.py:98-130`.
- **Трасса:**
  1. Receipt `BUILDING` с точным file inventory фиксируется до создания candidate.
  2. Затем план требует «Reconcile old pending file operations or fail stopped» и отдельно «capture-root temporary recovery handled independently», не говоря, когда и над чем это выполняется.
  3. Если реконсиляция физически доигрывает `deletion_pending`, move-intent или `cleanup_intent` на старом дереве после шага 1 и процесс убит до активации, следующий запуск видит изменённый inventory.
  4. По правилу «changed v1 source fails stopped; no override flag» сервис остаётся остановленным навсегда, выход — только ручная хирургия.
- **Минимальная правка:** добавить инвариант: между фиксацией source receipt и активацией v2 мигратор не меняет ни одного старого приватного файла. Незавершённые операции либо идемпотентно доигрываются до снятия inventory, либо моделируются только в candidate.
- **Тест:** фикстуры с `deletion_pending`, незавершённым quarantine-move, `cleanup_intent` и capture-temp; SIGKILL после каждого доактивационного шага; fingerprint старого дерева равен receipt, повторный запуск проходит под тем же UUID.

### M2. Не определена судьба своего, но неадоптируемого candidate при каноническом v1
- **Где:** план, строки 59 и 63.
- **Трасса:**
  1. Релиз A довёл candidate до `PREPARED` (эта фаза записана внутри candidate, в каноническом v1 остаётся `BUILDING`) и убит до rename.
  2. Forward-fix B меняет DDL или импорт, поэтому schema или full parity candidate не совпадают.
  3. Правило адопции не выполняется; правило пересборки описано только для «kill during `BUILDING`»; а «mismatched … candidate fails stopped; no override».
  4. Тупик, хотя v1 и старые файлы нетронуты и candidate по определению одноразовый. Это ровно сценарий, ради которого введена третья поправка.
- **Минимальная правка:** пока canonical = v1, любой candidate, совпадающий по пути, UUID и UID мигратора (не symlink), но не прошедший адопцию, удаляется и пересобирается под тем же UUID независимо от записанной в нём фазы. Fail-stop — только для чужого, неизвестного или меняющегося candidate.
- **Тест:** `PREPARED`-candidate от ревизии A с отличающейся схемой или parity плюс v1 canonical; B удаляет его, пересобирает и завершает миграцию; candidate с чужим UID по-прежнему отклоняется.

## Low (не блокируют, стоит внести заодно)

- **L1. Stale-предикат и NULL** (план, строка 21). `mapping_revision != current_clock_hash` в SQL даёт NULL для строки, вставленной drain'ом без проекции: она не попадает ни в stale, ни в current и не будет найдена refresh-сканом. Правка: `mapping_revision NOT NULL` с фиксированным sentinel «не проецировано» либо предикат `IS NOT`. Тест: свежая строка считается stale у клиента и подбирается refresh.
- **L2. Классификация exit 78** (план, строка 65). `_connect` сворачивает любую ошибку в один `StateDBError("unavailable")` (`collector_state_db.py:186-187`), а `_command_state` даёт exit 1 (`cli.py:176-178`). План не говорит, по какому признаку выбирается 78. Если отобразить широко, транзиентный `SQLITE_BUSY` на старте оставит сервис лежать до следующего деплоя, и кольцо кулона переполнится. Правка: 78 только по положительному признаку — schema v1, receipt не `COMPLETE`, или `EACCES` на DB/журнале; всё остальное — обычный рестарт. Тест: busy-lock на старте даёт код ≠78 и рестарт; каждый из трёх признаков даёт 78 без рестарта.

## Что проверено и замечаний не вызвало

- **Arena и tail:** байты ниже received-watermark неизменяемы (`transfer_arena.py:214-229`), снимок tail на loop-потоке согласован с моделью.
- **Drain:** текущий цикл уже режет по `chunk_records` (`attempt_writer.py:651-667`), commit на каждый drain укладывается в него.
- **Публикация:** идентичность bundle и lineage зависят только от исходных байтов и диапазонов (`ready_bundles.py:367-390, 1024-1027, 1581-1586`), переносятся на SQL-срезы без изменения манифеста и ACK.
- **Порог при крэше:** сегодня он действительно переприменяется к остатку (`ready_bundles.py:204-220`), cohort-permit это закрывает.
- **Scratch:** UUID-каталог не покрыт recovery (`ready_bundles.py:804`), как и утверждает план.
- **Throughput и space:** критерии заданы формулами; измерения — предусловия стадии 1.
- **Rescue-пакет `recovery/`:** не входит в инвентаризируемые корни плана; судя по `docs/FILE_NECESSITY_AUDIT.md` и PR #244, уже выведен. Проверить живое состояние из рабочей границы я не мог.

## Стратегические замечания (не блокеры)

- **Внешние SQL-клиенты.** «Queryable by ordinary SQLite clients» превращает приватную схему в де-факто контракт, а в DELETE-журнале долгий внешний SELECT блокирует commit писателя. Захват при этом прервётся без потери данных (ADVANCE не уйдёт), но в документации стоит явно ограничить клиентов короткими read-транзакциями.
- **Необратимость после активации.** Без архива любой дефект v2-схемы или импорта чинится только in-place поверх живых данных. Это осознанное решение оператора; его цена — parity-тесты стадии 5 становятся единственной страховкой.
- **Код перехода.** План изолирует v1-декодеры, но не говорит, когда они удаляются. Это будущая работа, сейчас она нигде не заведена: удалить их и receipt-логику следующим релизом после live acceptance.

## Блокеры до GO

1. M1 — инвариант «старое дерево неизменно между receipt и активацией» плюс тест.
2. M2 — правило «свой неадоптируемый candidate при v1 пересобирается» плюс тест.

После внесения этих двух правок (L1 и L2 желательно тем же проходом) возражений к началу стадии 1 у меня нет.

Не относится к ревью: MCP-коннекторы Atlassian Rovo, Gmail, Plane.so, Slack, exa и my mcp registry требуют авторизации в настройках коннекторов claude.ai; в этой сессии они недоступны и не понадобились.
