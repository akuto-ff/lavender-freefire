# LAVENDER PRO 7.0 — STREAMER ZONE

Добавлено:
- отдельная роль STREAMER;
- админ создаёт, редактирует, отключает и удаляет стримеров;
- каждый стример входит своим логином/паролем;
- у каждого стримера своя приватная Streamer Zone;
- стример может начать/остановить стрим;
- менять название, платформу, ссылку;
- выбирать матч для своего оверлея;
- менять цвет и текст своего оверлея;
- у каждого стримера отдельный OBS URL:
  /overlay.html?streamer=ID
- публичная вкладка "Стримеры";
- админ по-прежнему имеет отдельную Studio и Admin Panel.

Render Environment:
LAVENDER_ADMIN_USER
LAVENDER_ADMIN_PASSWORD
LAVENDER_SESSION_SECRET
NODE_ENV=production

Важно: на Free Render data.json не постоянный между redeploy. Для реального продакшена лучше подключить PostgreSQL.
