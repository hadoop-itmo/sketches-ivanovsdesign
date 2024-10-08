## Решение

Пояснения к решению в блокноте `bloom.ipynb`

Зависимости из pyenv (ipykernel + смежные зависимости включены)
в `requirements.txt`

В задании 1-3 таблица результатов сохраняется в `results/`

В остальных заданиях результаты выводятся в stdout командной строки

## Введение

Здесь мы не будем напрямую трогать руками Hadoop/MapReduce, но закрепим полезные знания,
которые имеют к нему весьма прямое отношение.

Попишем код на Python. Пригодится библиотека для вычисления MurMurHash,

https://github.com/hajimes/mmh3 

Про алгоритм вычисления можно узнать здесь (не критично для домашки, просто для общего развития):

(https://en.wikipedia.org/wiki/MurmurHash)

В репозитории добавлен вспомогательный код, который поможет вам сгенерировать нужные тестовые данные.

В каждом решении напишите, на каких данных вы его тестировали.


## Задание 1

10 баллов

Реализуем Bloom-Filter на одной хеш-функции.

```
class BloomFilter:
    def __init__(self, n):
        pass

    def put(self, s):
        pass

    def get(self, s):
        pass

    del size(self, s):
        pass
``` 

Ожидается, что `s` - строка.

`get` возвращает True или False.

`size` возвращает количество единиц.

Один из смыслов Bloom-фильтра - экономия памяти. Поэтому ее надо использовать максимально экономно.

И хотя технически его можно реализовать через питоновский список с логическими значениями, это не самый лучший вариант.

Интересными выглядят два варианта

- битовая арифметика на питоновский целых числах

- битовая арифметика на Numpy-массивах - используя все доступные биты

Проведем эксперименты.

Возьмем следующие размеры Bloom-фильтра:

- 8
- 64
- 1024
- 64K
- 16M

Возьмем следующие размеры набора уникальных строк:

- 5
- 50
- 500
- 5000
- 5000000

Для всех комбинаций размеров проведем такой эксперимент: пробегая в цикле по набору уникальных строк, вызывайте `get`,
потом `put` и считайте количество `True`, которые вернулись в `get`. А в конце посчитайте количество единиц в BF. 

Выведем данные в табличке со столбцами `bf_size`, `set_size`, `fp_count`(`get`, вернувших `True`), `ones_count`


## Задание 2

10 баллов
 
Реализуем Bloom Filter на `к` хеш-функций.
 
```
class BloomFilter:
    def __init__(self, k, n):
        pass

    def put(self, s):
        pass

    def get(self, s):
        pass

    def size(self, s):
        pass
``` 

`size` пусть возвращает количество единичных битов, деленное на `k`.

Проведем аналогичные эксперименты, добавив еще одно измерений для `k = 1, 2, 3, 4`

Сравним корректность такой оценки для разных `k`.


## Задание 3 

15 баллов

Реализуем Counting Bloom Filter на `к` хеш-функций.

https://en.wikipedia.org/wiki/Counting_Bloom_filter

`n` - количество счетчиков (аналог числа бит в Bloom Filter)

`сap` - количество битов, отведенных на счетчик (1 даст по сути Bloom Filter)

```
class CountingBloomFilter:
    def __init__(self, k, n):
        pass

    def put(self, s):
        pass

    def get(self, s):
        pass

    del size(self, s):
        pass
```

`size` - сумма счетчиков, деленная на `k`.

Соображения про компактность остаются в силе.

При реализации на Numpy-массивах в идеале надо использовать все биты. Но если количество битов счетчика не делит 64 нацело,
допускается оставить незанятыми биты в количестве остатка от деления. Например, если заказан счетчик размером 5 бит, то в 64 битах
размещаем 12 фрагментов по 5 бит и 4 бита оставляем неиспользованными.

Поставьте эксперименты по аналогии с первыми двумя заданиями. Подберите интересные комбинации значений.
Поясните, чем они интересны.


## Задание 4

15 баллов

Реализуем HyperLogLog.

```
class HyperLogLog:
    def __init__(self, k, n):
        pass

    def put(self, s):
        pass

    del est_size(self, s):
        pass
```

Строгое описание алгоритма можно прочитать здесь: https://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf
(прочитать всю статью полезно для общего развития, но не критично для задания).

Реализовав алгоритм, продумаем свой план экспериментов, интересных для наблюдения за алгоритмом и проверкой корректности реализации.
Продумав эксперименты, проведем их и проинтерпретируем результаты.


## Задание 5

20 баллов

Васе подарили два csv-файла. Каждый из них содержит по 10 миллиардов строк.

Вася хочет сделать JOIN по их первым колонкам, но опасается, что по некоторым ключам будет
слишком много записей и JOIN будет работать слишком долго.

Вася разбирается в том, как делать JOIN и он даже прибросил, что пороговым значением будет 60000.
То есть если у двух таблиц будет общий ключ и в каждой таблице по этому ключу будет более 60000 записей -
то будут проблемы.

Вася попробовал посчитать наивным скриптом через `Counter`, но не хватило памяти.

Напишите Васе скрипт. который посчитает и памяти которому хватит. Прочитать файлы несколько раз можно, но чем
меньше - тем лучше. Два прохода по каждому файлу должно точно хватать. Но в некоторых случаях может хватить и
меньшего. И этими случаями лучше воспользоваться.

## Задание 6

30 баллов

Вася открыл стартап. Для целей аналитики он собирает данные. И получает два csv-файла.

И между ними хорошо бы сделать JOIN по первому полю.

Но если оценить размер выдачи этого потенциального JOIN-а по количеству записей - это само по себе может иметь
значение (как некая степень общности данных из этих файлов). 

От способности решить эту задачу зависит привлечение в стартап хороших инвесторов.
Вася готов сделать вас сооснователем стартапа, если вы решите ему эту задачу.

Хотелось бы как-то встроиться в процесс записи, собирать какие-то данные и оценивать размер потенциального JOIN-а.

В каждом файле может быть до 100 миллионов уникальных ключей. Ключи могут повторяться.
Общее число записей не превышает 1 триллиона. Размер ключа может достигать 100 байт.

Если в наивном решении завести `Counter` и считать вхождения всех ключей, то только на ключи уйдет до 10Gb на
каждый файл. Это отпадает.

С другой стороны - если уникальных ключей в одном файле будет 1 миллион - то можно просто посчитать для одного файла.
А если удастся для обоих - то вообще можно дать точный ответ.

Но если не удастся - можно дать неточный.

Максимум возможного надо собрать без дополнительного чтения. Мимо вас пролетают ключи и вы храните какие-то данные в памяти.

Если дополнительные проходы по уже собранным данным могут улучшить ответ - можно их сделать. Но их должно быть константное число.
И небольшое число. 1-2 на файл.

Вася будет счастлив, если ваше решение будет обладать следующими свойтвами:

- на файлах до 1 миллиона уникальных ключей (обоих) - даст точный ответ без дополнительных проходов

- в случае непересекающихся множеств ключей - даст ответ 0 (для "маленького" случая гарантированно, для произвольного - с большой вероятностью)

- если реальный размер JOIN превышает 10 миллионов, то решение должно это гарантированно понять, даже если нужен дополнительный проход (но если
можно без него, то лучше без него) - и в таком случае точный подсчет значения не нужен

- в остальных случаях даст разумно точную оценку

Критерий точности не дается в терминах "отклонение не более, чем на 10 процентов с вероятностью не менее, чем 0.9" потому что доказать
такую оценку сложнее, чем оценить вероятность false positive в Bloom Filter. Мы это делать не учились и это не основной фокус курса.

В этой части есть элемент размахивания руками, но полагаю, что при все при этом можно объективно отличить более хорошее решение от менее
хорошего.

В сдаваемом решении не надо встариваться в какой-то процесс создания csv. Надо написать скрипт, принимающий параметром 2 csv-файла и напечатать ответ. 
Это будет вполне нормальным эквивалентом постановки задачи в версии Васи.

Вы можете прочитать эти файлы от начала до конца. На ваше усмотрение - сначала один полностью, а потом другой - или как-то чередуясь. Важно то,
что один проход по каждому из файлов - это как будто вы наблюдаете за данными, которые проходят через вас. Если вам потребовался еще один проход - 
это уже проход по сохраненным данным.

