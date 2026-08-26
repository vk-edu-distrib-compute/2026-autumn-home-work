# 2026-autumn-home-work 

[![Build Status](https://github.com/vk-edu-distrib-compute/2026-autumn-home-work/actions/workflows/gradle-build.yaml/badge.svg)](https://github.com/vk-edu-distrib-compute/2026-autumn-home-work/actions/workflows/gradle-build.yaml/badge.svg)
[![Code Style Check](https://github.com/vk-edu-distrib-compute/2026-autumn-home-work/actions/workflows/gradle-code-style.yaml/badge.svg)](https://github.com/vk-edu-distrib-compute/2026-autumn-home-work/actions/workflows/gradle-code-style.yaml)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/fdb601d406384215a5a37372cc3cf06a)](https://app.codacy.com/gh/vk-edu-distrib-compute/2026-autumn-home-work/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade)

# Правила репозитория

**ВНИМАНИЕ!** Нарушение следующих правил повлечёт снижение баллов, будьте аккуратны

* Не добавлять файлы ресурсов и др. файлы не являющиеся исходными файлами на языке Java. Все нужные вам доп. файлы (например файлы данных) храните вне репозитория
* Не трогать чужой код
* Не трогать билд-скрипты и настройки чекеров
* Не добавлять зависимости. Все задания возможно выполнить с текущим списком зависимостей. Возможно он будет расширяться для новых заданий. 

Если нашли ошибку в общих файлах репозитория или хотите что-то улучшить - присылайте отдельный PR, обсудим.

# Домашние задание курса "Распределенные вычисления" осень 2026 года.

Будем строить бэкэнд сервиса сокращения сслылок (аналог https://vk.ru/cc, https://clck.ru/, https://bitly.com/). 

### Fork
[Форкните проект](https://help.github.com/articles/fork-a-repo/), склонируйте и добавьте `upstream`:
```
$ git clone git@github.com:<username>/2026-autumn-home-work.git
Cloning into '2026-autumn-home-work'...

...

$ git remote add upstream git@github.com:vk-edu-distrib-compute/2026-autumn-home-work.git
$ git fetch upstream
From github.com:vk-edu-distrib-compute/2026-autumn-home-work
 * [new branch]      master     -> upstream/master
```
### Run
Запустить можно так (если вы вдруг используете винду то SERVICE_FACTORY переменную нужно задавать по-другому и запускать `gradle.bat`):
```bash
$ SERVICE_FATORY=<fully.qualified.service.factory.class.name> SERVICE_PORT=8080 ./gradlew run
````

### Test
Так можно запустить тесты:
```bash
$ ./gradlew check
```

### Code style checks
```bash
$ ./gradlew codeStyleChecks
```

### Develop
Откройте в IDE -- [OpenIDE](https://openide.ru/download/) нам будет достаточно.

**ВНИМАНИЕ!** При запуске тестов или сервера в IDE необходимо передавать Java опцию `-Xmx128m`. 

## Task 1. URL-shortener service

Сделать сервис, который из длинной ссылки, делает короткую. Короткая ссылка должна быть вида `http://localhost:<urlShortener SERVICE_PORT>/<ID>`, где `<ID>` - это `random alpha-numeric string of 10 characters`

Для этого в своём Java package `company.vk.edu.distrib.compute.<username>.urlshortener` реализуйте интерфейс [`UrlShortenerService`](src/main/java/company/vk/edu/distrib/compute/urlshortener/UrlShortenerService.java) и поддержите следующий HTTP API протокол:

* `GET /v0/status` -- `200` в нормальной ситуации, `503` в случае проблем.
* `GET /v0/links/<ID>` -- получить длинную ссылку по `<ID>` короткой ссылки. Возвращает `200 OK`, `Content-Type: text/html; charset=utf-8` и ссылку или `404 Not Found`. 
* `POST /v0/links` -- создать короткую ссылку для заданной в теле, `Content-Type: text/html; charset=utf-8`. Возвращает `201 Created`, `Content-Type: text/html; charset=utf-8` и короткую ссылку в теле. 
* `PUT /v0/links/<ID>` -- изменить существующую короткую ссылку по `<ID>` на заданную в теле, `Content-Type: text/html; charset=utf-8`. Возвращает `200 OK`, или `404 Not found` если такого `<ID>` нет.
* `DELETE /v0/links/<ID>` -- удалить ссылку по `<ID>`. Возвращает `202 Accepted`.
* `GET /<ID>` -- отдаётся редирект `301 Moved Permanently` и заголовок `Location: <длинная ссылка соотвествующая ID>`. `404 Not Found` если такого `<ID>` нет.
* Во всех случаях, кога передаётся либо не валидный `<ID>` либо невалидная ссылка в тебе запроса (POST/PUT методы) - надо вернуть `422 Unprocessable Content`

1. Сделать наследника [`AbstractHttpServiceFactory`](src/main/java/company/vk/edu/distrib/compute/AbstractHttpServiceFactory.java) в пакете со своим именем/ником, у класса должен быть публичный конструктор **без параметров**
2. Ваша реализация интерфейса `UrlShortenerService`, возвращаемая из вашей же `AbstractHttpServiceFactory`, должна запускать [HttpServer из JDK](https://docs.oracle.com/en/java/javase/25/docs/api/jdk.httpserver/com/sun/net/httpserver/HttpServer.html).
3. Ваш `UrlShortenerService` должен работать с вашей же реализацией интерфейса [`Dao`](src/main/java/company/vk/edu/distrib/compute/Dao.java) и делегировать работу по хранению данных.
4. В минимальной реализации `Dao` достаточно хранить данные в памяти. `T` в `Dao` будет `String`.
5. Добавить своего наследника `AbstractHttpServiceFactory` в поле [`UrlShortenerServiceFactoryArgumentsProvider.factories`](src/integrationTest/java/company/vk/edu/distrib/compute/test/urlshortener/UrlShortenerServiceFactoryArgumentsProvider.java)

Продолжайте запускать тесты и исправлять ошибки, не забывая [подтягивать новые тесты и фиксы из `upstream`](https://help.github.com/articles/syncing-a-fork/). 
Если заметите ошибку в `upstream`, заводите баг и присылайте pull request ;)

### Report
Когда всё будет готово, присылайте pull request со своей реализацией на review. Не забывайте **отвечать на комментарии в PR** и **исправлять замечания**!

### Bonus tasks

#### Authentication

1. Добавить проверку заголовка [basic-аутентификации](https://datatracker.ietf.org/doc/html/rfc7617) согласно со спекой.
2. Пользователей и пароли хранить в отдельном `Dao<String>`
3. Добавить в HTTP API протокол сервиса: `POST /internal/users` -- добавить пользователя, `Content-Type: text/html; charset=utf-8`, тело состоит из одной строки содержащей имя пользователя и пароль разделённые двоеточием (например `admin:super_pass`). Возвращает `200 OK`, если пользователь уже есть заменить пароль на заданный. Метод нужен, чтобы можно было наполнить базу пользователей для простоты тестирования. В реальных сервисах такое делается по-другому.
4. Аутентификацией должны быть закрыты все запросы, кроме `status` и `/intenal/users`
5. Добавить своего наследника `AbstractHttpServiceFactory` в поле [`AuthenticatedUrlShortenerServiceFactoryArgumentsProvider.factories`](src/integrationTest/java/company/vk/edu/distrib/compute/test/urlshortener/AuthenticatedUrlShortenerServiceFactoryArgumentsProvider.java)

#### Persistent Dao

1. Сделать Dao которые хранят данные на диске, чтобы переживали рестарты

### Credits

* Основное задание - 10 балов
* Authentication - 5 бала
* Persistent Dao - 1 бал

