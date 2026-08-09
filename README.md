# LAVENDER PRO 6.0 STABLE

Стабильная версия для GitHub + Render.

Исправлено:
- админ-вход на ПК и телефоне;
- стабильная работа Socket.IO с переподключением;
- устранены частые двойные перезагрузки интерфейса;
- атомарная запись data.json;
- защита от повреждения data.json;
- нормальная мобильная адаптация;
- аватарки автоматически уменьшаются перед загрузкой;
- стабильное обновление Stream Studio -> OBS;
- обработка ошибок API;
- backup/export и import;
- health endpoint /health;
- кэширование статических файлов без агрессивного кэша HTML;
- сервер не выводит пароль администратора в лог.

Render:
Build Command: npm install
Start Command: node server.js

Environment Variables:
LAVENDER_ADMIN_USER
LAVENDER_ADMIN_PASSWORD
NODE_ENV=production

Важно:
На бесплатном Render локальный data.json НЕ является постоянным хранилищем.
После redeploy данные могут сброситься к файлу из GitHub. Используй Backup или подключи постоянную БД.
