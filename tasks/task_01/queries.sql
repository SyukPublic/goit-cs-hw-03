/*
Отримати всі завдання певного користувача. Використайте SELECT для отримання завдань конкретного користувача за його user_id.
*/
SELECT t.* FROM tasks AS t WHERE t.user_id = 5;

WITH vars AS (SELECT 5 AS user_id)
SELECT t.* FROM tasks AS t WHERE t.user_id IN (select user_id from vars);


/*
Вибрати завдання за певним статусом. Використайте підзапит для вибору завдань з конкретним статусом, наприклад, 'new'.
*/

SELECT t.* FROM tasks AS t WHERE t.status_id IN (SELECT s.id FROM status AS s WHERE s.name = 'new');

WITH new_status AS (SELECT s.id FROM status AS s WHERE s.name = 'new')
SELECT t.* FROM tasks AS t WHERE t.status_id IN (select id from new_status);


/*
Оновити статус конкретного завдання. Змініть статус конкретного завдання на 'in progress' або інший статус.
*/

UPDATE tasks SET status_id = 2 WHERE id = 26;

UPDATE tasks SET status_id = (SELECT s.id FROM status AS s WHERE s.name = 'in progress') WHERE id = 35;


/*
Отримати список користувачів, які не мають жодного завдання. Використайте комбінацію SELECT, WHERE NOT IN і підзапит.
*/

SELECT u.* FROM users AS U WHERE u.id NOT IN (SELECT DISTINCT t.user_id FROM tasks AS t);


/*
Додати нове завдання для конкретного користувача. Використайте INSERT для додавання нового завдання.
*/

INSERT INTO tasks (title, description, status_id, user_id)
VALUES('дуже важлива таска', 'Треба терміново зробити цю дуже важливу таску ;)', 1, 5);


/*
Отримати всі завдання, які ще не завершено. Виберіть завдання, чий статус не є 'завершено'.
*/

SELECT t.* FROM tasks AS t WHERE t.status_id NOT IN (SELECT s.id FROM status AS s WHERE s.name = 'completed');

WITH completed_status AS (SELECT s.id FROM status AS s WHERE s.name = 'completed')
SELECT t.* FROM tasks AS t WHERE t.status_id NOT IN (select id from completed_status);


/*
Видалити конкретне завдання. Використайте DELETE для видалення завдання за його id.
*/

DELETE FROM tasks WHERE id = 5;


/*
Знайти користувачів з певною електронною поштою. Використайте SELECT із умовою LIKE для фільтрації за електронною поштою.
*/

SELECT u.* FROM users AS u WHERE u.email LIKE 'korbutvasyl@example.com';

SELECT u.* FROM users AS u WHERE u.email = 'korbutvasyl@example.com';


/*
Оновити ім'я користувача. Змініть ім'я користувача за допомогою UPDATE.
*/

UPDATE users SET fullname = 'John Dou' WHERE id = 5;


/*
Отримати кількість завдань для кожного статусу. Використайте SELECT, COUNT, GROUP BY для групування завдань за статусами.
*/

SELECT s.name AS status, COUNT(*) AS tasks_number FROM tasks AS t INNER JOIN status AS s ON t.status_id = s.id GROUP BY s.name;


/*
Отримати завдання, які призначені користувачам з певною доменною частиною електронної пошти. Використайте SELECT з умовою LIKE в поєднанні з JOIN, щоб вибрати завдання, призначені користувачам, чия електронна пошта містить певний домен (наприклад, '%@example.com').
*/

SELECT t.* FROM tasks AS t INNER JOIN users AS u ON t.user_id = u.id WHERE u.email LIKE '%@example.net';


/*
Отримати список завдань, що не мають опису. Виберіть завдання, у яких відсутній опис.
*/

SELECT t.* FROM tasks AS t WHERE t.description IS NULL OR t.description = '';


/*
Вибрати користувачів та їхні завдання, які є у статусі 'in progress'. Використайте INNER JOIN для отримання списку користувачів та їхніх завдань із певним статусом.
*/

SELECT u.*, t.* FROM users AS u INNER JOIN tasks AS t ON u.id = t.user_id INNER JOIN status AS s ON t.status_id = s.id WHERE s.name = 'in progress';


/*
Отримати користувачів та кількість їхніх завдань. Використайте LEFT JOIN та GROUP BY для вибору користувачів та підрахунку їхніх завдань.
*/


SELECT u.id AS user_id, COUNT(t.id) AS tasks_number FROM users AS u LEFT OUTER JOIN tasks AS t ON u.id = t.user_id GROUP BY u.id ORDER BY tasks_number DESC;

SELECT u.id AS user_id, COALESCE(tg.tasks_number, 0) AS tasks_number FROM users AS u LEFT OUTER JOIN (SELECT t.user_id, COUNT(*) AS tasks_number FROM tasks AS t GROUP BY t.user_id) AS tg ON u.id = tg.user_id ORDER BY tasks_number DESC;
