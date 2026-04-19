## hw4_software_architecture

Я обрала Kafka як message queue.


## Інсталяція

Щоб підняти всі сервіси, використовується docker compose `docker compose up -d --build`

## Демонстрація

Завдання:
- Запускається config-server,
- Запустити facade-service, counter-service та три екземпляра
logging-service (кожен з екземплярів запускається у власному
Docker-контейнері), і далі реєструється в config-server,
- Відповідно має запуститись також кластер Hazelcast з трьох
серверів

Після білду перевіряємо, що всі контейнери запущені через `docker ps`

![alt text](img1.png)

Базові ендпоінти:

Перевіряємо список зареєстрованих сервісів

![alt text](img2.png)

Видно, що facade-service, counter-service та всі три екземпляри logging-service успішно зареєструвалися.

Перевіряємо конфігурацію Kafka та Hazelcast

![alt text](img3.png)


- Через HTTP POST записати 10 транзакцій msg1-msg10 через facade-service

Далі, я надсилаю POST запити з manual_test.http:

Приклад відповіді для msg2 

![alt text](img4.png)

Приклад для msg4 

![alt text](img5.png)

Приклад для msg7

![alt text](img6.png)

Кожен запит потрапляє на різні partition/offset пари, що підтверджує коректну роботу черги.

Загальна ситуація по акаунтах

![alt text](img7.png)


- Показати що повідомлення отримують різні екземпляри logging-service (це має бути видно у логах сервісу)

Перевіряємо логи кожного з трьох екземплярів logging-service. Повідомлення розподіляються між інстансами, що підтверджує балансування навантаження.

**logging1** отримував транзакції msg1, msg6, msg8:

![alt text](img8.png)

**logging2** отримував транзакції msg9, msg10:

![alt text](img9.png)

**logging3** отримував транзакції msg2, msg3, msg4, msg5, msg7:

![alt text](img10.png)

Кожен екземпляр успішно підключився до Hazelcast кластера `lab3-cluster`, запустив gRPC-сервер на порту 50051 та зареєструвався в config-server.

Логи counter-servise

![alt text](img11.png)


- Через HTTP GET з facade-service прочитати транзакції, перевірити що значення на рахунках є коректним


Запит `GET /accounts` повертає всі поточні баланси, всі 10 користувачів мають коректні значення від 1.0 до 10.0

![alt text](img7.png)

В логах counter-service видно, що він успішно зконсюмив усі повідомлення з Kafka:

![alt text](img11.png)

Надсилаємо ще транзакцію msg1 на 50.00 

![alt text](img12.png)


Перевіряємо рахунок msg1 через `GET /user/msg1`. Баланс 51, в транзакціях відображаються обидва записи 1 та 50

![alt text](img13.png)

## Відмовостійкість

- Вимкнути чи зробити недоступним (docker pause) counter-service
та перевірити, що запис транзакцій йде без помилок (бо транзакції
до counter-service накопичуються у message queue)

Перевіряєм відмовостійкість, коли падає counter.Зупиняємо counter-service через `docker pause`

![alt text](img14.png)

Перевіряємо статус, контейнер у стані Paused:

![alt text](img15.png)

Надсилаємо нові транзакції (msg1 на 50.00, msg2 на 100.00) поки counter-service недоступний. Facade-service успішно приймає запити і повертає статус 200, транзакції ставляться у чергу Kafka без помилок:

![alt text](img16.png)

![alt text](img17.png)


- При цьому, відповідно GET-запити не мають спрацьовувати, мають або повертати null у якості значень на рахунку

Поки counter-service зупинений, GET-запити повертають `null`, оскільки сервіс не може отримати актуальний баланс

![alt text](img18.png)

![alt text](img19.png)

`GET /accounts` також повертає `null`, оскільки counter впав

![alt text](img20.png)

Перевіряєм стан Kafka.
![alt text](img21.png)

Видно lag на partition 2, що свідчить про накопичення непрочитаних повідомлень


- Знову запустити counter-service та переконатись, що за деякий час він зчитує з черги накопичені до нього транзакції, застосує їх та почне повертати коректні значення рахунків

Розморожуємо counter-service через `docker unpause` та переглядаємо логи

![alt text](img22.png)

Сервіс успішно перепідключився до Kafka Broker та вичитав усі накопичені повідомлення з черги

Повторна перевірка consumer group показує lag = 0 по всіх партиціях, отже усі повідомлення оброблені

![alt text](img23.png)

Перевіряємо `GET /user/msg1`. Баланс 101, всі транзакції враховані

![alt text](img24.png)

Перевіряємо `GET /user/msg2`, баланс 102

![alt text](img25.png)

`GET /accounts` тепер повертає коректні баланси для всіх користувачів

![alt text](img26.png)

Всі транзакції дійшли успішно