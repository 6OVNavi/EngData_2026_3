# ДЗ 5. Управление ресурсами в Kubernetes

Вариант 9. Пары навыков у разработчиков мобильных приложений. [Отчёт в PDF](report.pdf).

- `pairs.py` и `Dockerfile` содержат программу и сборку образа `hw5-pairs:1`.
- В `k8s/` лежат манифесты, в `logs/` журналы программы, в `outputs/` выводы Docker и kubectl.
- [Полный расчёт](logs/pairs-fit.txt) и [расчёт по четырём частям](logs/pairs-reduce.txt) дали те же десять пар, что и в ДЗ 3.

## Порядок запуска

Для первого этапа нужен RustFS из ДЗ 2 на порту 9000 и файл `s3://hh-lake/raw/it_vacancies_2024.csv`. Образ собирается командой `docker build -t hw5-pairs:1 .` в Docker Desktop. Узел Kubernetes должен видеть локальный образ.

1. Запустить `01-job-oom.yaml`. Для замера в шаблоне `02-job-fit.yaml` заменить `pairs-fit` на `pairs-measure`, а `LIMIT` на `1Gi`. Измеренный пик составил 137,7 МиБ. Для повторного запуска с пределом 176Mi есть готовый `02-job-fit-applied.yaml`.
2. Применить `03-rustfs.yaml` и дождаться готовности RustFS. Затем по очереди запустить задания 04–07. Перед каждым следующим шагом дождаться завершения предыдущего. Итоговый файл в бакете: `results/k8s_top10.tsv`.
3. Применить `08-pending.yaml` и посмотреть события пода через `kubectl describe pod -l job-name=cpu-pending`.
4. Применить `09-priority.yaml`. В шаблонах 10 и 11 подставить вместо `CPU_REQ` по 60 % доступного CPU узла. На узле с 4 CPU это `2400m`. Сначала запустить low-job и дождаться Running, затем запустить high-job.
5. Применить манифесты 12 и 13. Проверить `kubectl describe quota -n hw5-quota` и `kubectl describe job quota-job -n hw5-quota`.
