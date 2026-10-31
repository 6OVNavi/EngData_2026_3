# ДЗ 4. ZooKeeper: ансамбль из трёх серверов и отказоустойчивый координатор

- `zoo.cfg` — конфигурация серверов, общая для всех трёх; номер сервера в `myid` задаёт `ZOO_MY_ID` в `compose.yaml`
- `compose.yaml` — три сервера ZooKeeper и две копии координатора
- `coordinator.py` — расчёт пар навыков из ДЗ 3 с выбором лидера, чекпоинтами и защитой по эпохе (`FENCING=1`)
- `logs/` — журналы координаторов, `outputs/` — выводы команд

```
docker compose up -d
$env:RUN="run1"; $env:FENCING="0"; docker compose --profile coord up -d coord1 coord2
$env:RUN="run2"; $env:FENCING="1"; docker compose --profile coord up -d coord1 coord2
```

Отчёт: `report.pdf`
