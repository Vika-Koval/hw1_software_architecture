# hw1_software_architecture

## АРХІТЕКТУРА ПРОЄКТУ ТА ПОТІК ЗАПИТІВ

Система складається з трьох мікросервісів, які керуються за допомогою Docker Compose. Потік взаємодії виглядає наступним чином:

1. Клієнт відправляє POST-запит до Facade Service.
2. Facade Service звертається до Logging Service для збереження інформації про транзакцію.
3. Logging Service успішно зберігає дані та повертає статус 200 OK.
4. Facade Service звертається до Counter Service для оновлення балансу.
5. Counter Service оновлює баланс у пам'яті та повертає статус 200 OK.
6. Facade Service формує відповідь та повертає Клієнту Transaction ID і оновлений баланс.

## РЕЗУЛЬТАТИ РУЧНОГО ТЕСТУВАННЯ

Мікросервіси були протестовані за допомогою запитів з файлу manual_test.http.
Нижче наведено результати ручного тестування:

## Client 1

### client-1 +150
POST http://localhost:8080/transaction
Content-Type: application/json

{
    "user_Id": "client-1",
    "amount": 150.00
}
<img width="1851" height="1054" alt="Screenshot from 2026-02-22 19-51-34" src="https://github.com/user-attachments/assets/cfd7771f-013d-4a50-b49d-3ca4952d1a8b" />

### client-1 -50
POST http://localhost:8080/transaction
Content-Type: application/json

{
    "user_Id": "client-1",
    "amount": -50.00
}
<img width="1851" height="1054" alt="Screenshot from 2026-02-22 19-51-44" src="https://github.com/user-attachments/assets/08853ca2-5de7-4d56-a79c-4f073ee274e8" />

### Check balance and transactions of Client 1
GET http://localhost:8080/user/client-1
<img width="1851" height="1054" alt="Screenshot from 2026-02-22 19-53-12" src="https://github.com/user-attachments/assets/6d155f51-5af2-49e4-ba3c-35166f57e518" />

## Client 2

### client-2 +300.5
POST http://localhost:8080/transaction
Content-Type: application/json

{
    "user_Id": "client-2",
    "amount": 300.50
}
<img width="1851" height="1054" alt="Screenshot from 2026-02-22 19-51-54" src="https://github.com/user-attachments/assets/127b7044-cfbf-43eb-9ad1-c97b29726f7f" />

### client-2 -10.5
POST http://localhost:8080/transaction
Content-Type: application/json

{
    "user_Id": "client-2",
    "amount": -10.50
}
<img width="1851" height="1054" alt="Screenshot from 2026-02-22 19-52-05" src="https://github.com/user-attachments/assets/92fdefab-56dd-4bd2-94b6-1b5a35172183" />

### Check balance and transactions of Client 2
GET http://localhost:8080/user/client-2
<img width="1851" height="1054" alt="Screenshot from 2026-02-22 19-53-27" src="https://github.com/user-attachments/assets/b0a3df44-6cb9-4d7e-8804-c5e880122008" />

## Client 3

### client-3 -25
POST http://localhost:8080/transaction
Content-Type: application/json

{
    "user_Id": "client-3",
    "amount": -25.00
}
<img width="1851" height="1054" alt="Screenshot from 2026-02-22 19-52-14" src="https://github.com/user-attachments/assets/98c1f924-9728-45c1-93be-a7399c304a4e" />

### Check balance and transactions of Client 3
GET http://localhost:8080/user/client-3
<img width="1851" height="1054" alt="Screenshot from 2026-02-22 19-53-36" src="https://github.com/user-attachments/assets/8b974cf5-f089-4816-a261-2c1dc967f1b6" />

## All

### Check balances of all clients
GET http://localhost:8080/accounts
<img width="1851" height="1054" alt="Screenshot from 2026-02-22 19-53-50" src="https://github.com/user-attachments/assets/f2754ab6-2ea2-47c5-81b4-ae1096d0b5b0" />

### Check execution time 
GET http://localhost:8080/metrics
<img width="1851" height="1054" alt="Screenshot from 2026-02-22 19-53-58" src="https://github.com/user-attachments/assets/8507ec67-0a9c-440b-bb65-86e5b18d7579" />

Як видно зі скріншотів, баланси оновлюються коректно, а сервіси взаємодіють належним чином.

## ОЦІНКА ПРОДУКТИВНОСТІ

Навантажувальне тестування проводилося за допомогою скрипта load_test.py.
Тести виконувалися на Intel Core i7

Було розглянуто два сценарії:

Сценарій 1: 10 клієнтів одночасно роблять по 10 000 транзакцій на власні окремі рахунки (Разом 100 000 запитів).
Сценарій 2: 10 клієнтів одночасно роблять по 10 000 транзакцій на один і той самий спільний рахунок (Разом 100 000 запитів).
<img width="1441" height="498" alt="Screenshot from 2026-02-22 22-24-15" src="https://github.com/user-attachments/assets/26787a56-5a34-49b2-9d40-78e7714c9760" />


## АНАЛІЗ ПРОДУКТИВНОСТІ

Порівняння метрик для двох сценаріїв:

* Загальний час виконання: 811.94 с (Сценарій 1) проти 850.99 с (Сценарій 2)
* Пропускна здатність (RPS): 123.16 req/s (Сценарій 1) проти 117.51 req/s (Сценарій 2)
* Точність кінцевого балансу: Підтверджено 10 000 на кожного юзера (Сценарій 1) та підтверджено рівно 100 000 на спільному рахунку (Сценарій 2).

Було розраховано середній час обробки одного запиту для кожного внутрішнього мікросервісу:

* Logging Service: ~47.19 мс на запит (4719 с / 100 000)
* Counter Service: ~46.92 мс на запит (4692 с / 100 000)

Висновки:
Обидва сценарії завершилися успішно. Пропускна здатність залишилася практично ідентичною незалежно від розподілу навантаження за рахунками. У сценарії 2 всі 10 конкурентних клієнтів успішно змінювали баланс одного й того самого рахунку без втрати даних або станів race conditions. Це доводить, що імплементація блокувань у Counter Service є надійною.

## МАЙБУТНІ АРХІТЕКТУРНІ ПОКРАЩЕННЯ

1. Перевести комунікацію з Logging Service на брокер повідомлень (наприклад, Kafka) за принципом "fire-and-forget", щоб швидше розблоковувати Facade.
2. Замінити стандартні REST HTTP-виклики між мікросервісами на gRPC для швидшої серіалізації та зменшення мережевого оверхеду.
3. Впровадити пакетну обробку транзакцій, щоб зменшити загальну кількість мережевих викликів під високим навантаженням
