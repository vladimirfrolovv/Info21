# Info21
SQL проект реализованный на postgresql. В проекте реализованы различные процедуры и фукции связанные с обработкой информации предоставленной бд.

## Logical view of database model

![SQL2](./image/SQL2.png)

## Cоздание бд и импорт данных в part1.sql

Чтобы создать базу данных и таблицы, описанные во входных данных, выполните следующие действия:

1. Укажите в part1.sql путь до необходимых tsv файлов которые лежат в import
2. Запустите скрипт `part1.sql` из репозитория.
3. Этот скрипт создаст необходимые таблицы, а также включает процедуры импорта и экспорта данных для каждой таблицы из/в файлы CSV и TSV.


## Описание в part2.sql

1) Написать процедуру добавления P2P проверки
Параметры: ник проверяемого, ник проверяющего, название задания, статус P2P проверки, время. 
Если задан статус "начало", добавить запись в таблицу Checks (в качестве даты использовать сегодняшнюю). 
Добавить запись в таблицу P2P. 
Если задан статус "начало", в качестве проверки указать только что добавленную запись, иначе указать проверку с незавершенным P2P этапом.

2) Написать процедуру добавления проверки Verter'ом
Параметры: ник проверяемого, название задания, статус проверки Verter'ом, время. 
Добавить запись в таблицу Verter (в качестве проверки указать проверку соответствующего задания с самым поздним (по времени) успешным P2P этапом)

3) Написать триггер: после добавления записи со статутом "начало" в таблицу P2P, изменить соответствующую запись в таблице TransferredPoints

4) Написать триггер: перед добавлением записи в таблицу XP, проверить корректность добавляемой записи
Запись считается корректной, если:

Количество XP не превышает максимальное доступное для проверяемой задачи
Поле Check ссылается на успешную проверку
Если запись не прошла проверку, не добавлять её в таблицу.

## Описание функций и процедур в part3.sql

##### 1) Написать функцию, возвращающую таблицу TransferredPoints в более человекочитаемом виде
Ник пира 1, ник пира 2, количество переданных пир поинтов. \
Количество отрицательное, если пир 2 получил от пира 1 больше поинтов.

Пример вывода:
| Peer1  | Peer2  | PointsAmount |
|--------|--------|--------------|
| Aboba  | Amogus | 5            |
| Amogus | Sus    | -2           |
| Sus    | Aboba  | 0            |

##### 2) Написать функцию, которая возвращает таблицу вида: ник пользователя, название проверенного задания, кол-во полученного XP
В таблицу включать только задания, успешно прошедшие проверку (определять по таблице Checks). \
Одна задача может быть успешно выполнена несколько раз. В таком случае в таблицу включать все успешные проверки.

Пример вывода:
| Peer   | Task | XP  |
|--------|------|-----|
| Aboba  | C8   | 800 |
| Aboba  | CPP3 | 750 |
| Amogus | DO5  | 175 |
| Sus    | A4   | 325 |

##### 3) Написать функцию, определяющую пиров, которые не выходили из кампуса в течение всего дня
Параметры функции: день, например 12.05.2022. \
Функция возвращает только список пиров.

##### 4) Найти процент успешных и неуспешных проверок за всё время
Формат вывода: процент успешных, процент неуспешных

Пример вывода:
| SuccessfulChecks | UnsuccessfulChecks |
|------------------|--------------------|
| 35               | 65                 |

##### 5) Посчитать изменение в количестве пир поинтов каждого пира по таблице TransferredPoints
Результат вывести отсортированным по изменению числа поинтов. \
Формат вывода: ник пира, изменение в количество пир поинтов

Пример вывода:
| Peer   | PointsChange |
|--------|--------------|
| Aboba  | 8            |
| Amogus | 1            |
| Sus    | -3           |

##### 6) Посчитать изменение в количестве пир поинтов каждого пира по таблице, возвращаемой [первой функцией из Part 3](#1-написать-функцию-возвращающую-таблицу-transferredpoints-в-более-человекочитаемом-виде)
Результат вывести отсортированным по изменению числа поинтов. \
Формат вывода: ник пира, изменение в количество пир поинтов

Пример вывода:
| Peer   | PointsChange |
|--------|--------------|
| Aboba  | 8            |
| Amogus | 1            |
| Sus    | -3           |

##### 7) Определить самое часто проверяемое задание за каждый день
При одинаковом количестве проверок каких-то заданий в определенный день, вывести их все. \
Формат вывода: день, название задания

Пример вывода:
| Day        | Task |
|------------|------|
| 12.05.2022 | A1   |
| 17.04.2022 | CPP3 |
| 23.12.2021 | C5   |

##### 8) Определить длительность последней P2P проверки
Под длительностью подразумевается разница между временем, указанным в записи со статусом "начало", и временем, указанным в записи со статусом "успех" или "неуспех". \
Формат вывода: длительность проверки

##### 9) Найти всех пиров, выполнивших весь заданный блок задач и дату завершения последнего задания
Параметры процедуры: название блока, например "CPP". \
Результат вывести отсортированным по дате завершения. \
Формат вывода: ник пира, дата завершения блока (т.е. последнего выполненного задания из этого блока)

Пример вывода:
| Peer   | Day        |
|--------|------------|
| Sus    | 23.06.2022 |
| Amogus | 17.05.2022 |
| Aboba  | 12.05.2022 |

##### 10) Определить, к какому пиру стоит идти на проверку каждому обучающемуся
Определять нужно исходя из рекомендаций друзей пира, т.е. нужно найти пира, проверяться у которого рекомендует наибольшее число друзей. \
Формат вывода: ник пира, ник найденного проверяющего

Пример вывода:
| Peer   | RecommendedPeer  |
|--------|-----------------|
| Aboba  | Sus             |
| Amogus | Aboba           |
| Sus    | Aboba           |

##### 11) Определить процент пиров, которые:
- Приступили к блоку 1
- Приступили к блоку 2
- Приступили к обоим
- Не приступили ни к одному

Параметры процедуры: название блока 1, например CPP, название блока 2, например A. \
Формат вывода: процент приступивших к первому блоку, процент приступивших ко второму блоку, процент приступивших к обоим, процент не приступивших ни к одному

Пример вывода:
| StartedBlock1 | StartedBlock2 | StartedBothBlocks | DidntStartAnyBlock |
|---------------|---------------|-------------------|--------------------|
| 20            | 20            | 5                 | 55                 |

##### 12) Определить *N* пиров с наибольшим числом друзей
Параметры процедуры: количество пиров *N*. \
Результат вывести отсортированным по кол-ву друзей. \
Формат вывода: ник пира, количество друзей

Пример вывода:
| Peer   | FriendsCount |
|--------|-------------|
| Amogus | 15          |
| Aboba  | 8           |
| Sus    | 0           |

##### 13) Определить процент пиров, которые когда-либо успешно проходили проверку в свой день рождения
Также определите процент пиров, которые хоть раз проваливали проверку в свой день рождения. \
Формат вывода: процент успехов в день рождения, процент неуспехов в день рождения

Пример вывода:
| SuccessfulChecks | UnsuccessfulChecks |
|------------------|--------------------|
| 60               | 40                 |

##### 14) Определить кол-во XP, полученное в сумме каждым пиром
Если одна задача выполнена несколько раз, полученное за нее кол-во XP равно максимальному за эту задачу. \
Результат вывести отсортированным по кол-ву XP. \
Формат вывода: ник пира, количество XP

Пример вывода:
| Peer   | XP    |
|--------|-------|
| Amogus | 15000 |
| Aboba  | 8000  |
| Sus    | 400   |

##### 15) Определить всех пиров, которые сдали заданные задания 1 и 2, но не сдали задание 3
Параметры процедуры: названия заданий 1, 2 и 3. \
Формат вывода: список пиров

##### 16) Используя рекурсивное обобщенное табличное выражение, для каждой задачи вывести кол-во предшествующих ей задач
То есть сколько задач нужно выполнить, исходя из условий входа, чтобы получить доступ к текущей. \
Формат вывода: название задачи, количество предшествующих

Пример вывода:
| Task | PrevCount |
|------|-----------|
| CPP3 | 7         |
| A1   | 9         |
| C5   | 1         |

##### 17) Найти "удачные" для проверок дни. День считается "удачным", если в нем есть хотя бы *N* идущих подряд успешных проверки
Параметры процедуры: количество идущих подряд успешных проверок *N*. \
Временем проверки считать время начала P2P этапа. \
Под идущими подряд успешными проверками подразумеваются успешные проверки, между которыми нет неуспешных. \
При этом кол-во опыта за каждую из этих проверок должно быть не меньше 80% от максимального. \
Формат вывода: список дней

##### 18) Определить пира с наибольшим числом выполненных заданий
Формат вывода: ник пира, число выполненных заданий

Пример вывода:
Output example:
| Peer   | XP    |
|--------|-------|
| Amogus | 5     |

##### 19) Определить пира с наибольшим количеством XP
Формат вывода: ник пира, количество XP

Пример вывода:
| Peer   | XP    |
|--------|-------|
| Amogus | 15000 |

##### 20) Определить пира, который провел сегодня в кампусе больше всего времени
Формат вывода: ник пира

##### 21) Определить пиров, приходивших раньше заданного времени не менее *N* раз за всё время
Параметры процедуры: время, количество раз *N*. \
Формат вывода: список пиров

##### 22) Определить пиров, выходивших за последние *N* дней из кампуса больше *M* раз
Параметры процедуры: количество дней *N*, количество раз *M*. \
Формат вывода: список пиров

##### 23) Определить пира, который пришел сегодня последним
Формат вывода: ник пира

##### 24) Определить пиров, которые выходили вчера из кампуса больше чем на *N* минут
Параметры процедуры: количество минут *N*. \
Формат вывода: список пиров

##### 25) Определить для каждого месяца процент ранних входов
Для каждого месяца посчитать, сколько раз люди, родившиеся в этот месяц, приходили в кампус за всё время (будем называть это общим числом входов). \
Для каждого месяца посчитать, сколько раз люди, родившиеся в этот месяц, приходили в кампус раньше 12:00 за всё время (будем называть это числом ранних входов). \
Для каждого месяца посчитать процент ранних входов в кампус относительно общего числа входов. \
Формат вывода: месяц, процент ранних входов

Пример вывода:
| Month    | EarlyEntries |  
| -------- | -------------- |
| January  | 15           |
| February | 35           |
| March    | 45           |
