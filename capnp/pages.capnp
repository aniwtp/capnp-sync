@0xfb5aa62e911a3191;

# Страницы глав: chapter (проверил права на тайтл) → uploader (хранилище).
# Один сервис на соединение, все вызовы — от имени команды `teamId`.

struct PageUpload {
  data @0 :Data;        # байты изображения как пришли от клиента
  userId @1 :UInt64;
  teamId @2 :UInt64;    # 0 — админская сессия
  titleId @3 :UInt32;
  web @4 :Bool;         # часть страницы вебтуна (ширина ≤ 1000, высота ≤ доли ширины), иначе страница манги (≤ 3000×4000)
  mini @5 :Data;        # готовая миниатюра от клиента (webp/jpeg/png, ≤400 по длинной стороне, кадр 2:3); пусто — миниатюры нет
  chapterId @6 :UInt32; # глава страницы (папка `titles/{titleId}/{chapterId}`); 0 — не задана (400)
}

struct PageStored {
  status @0 :UInt8;     # 0 — ок; 1 — не изображение/битое/габариты (400); 2 — сбой хранилища (500)
  error @1 :Text;
  token @2 :Text;       # nanoid-12, ключ объекта
  hash @3 :Data;        # 16 байт плейсхолдера (палитра)
  width @4 :UInt32;
  height @5 :UInt32;
}

struct PagesResult {
  status @0 :UInt8;     # 0 — ок; 1 — отказ (см. bad); 2 — сбой
  error @1 :Text;
  bad @2 :List(Text);   # токены неизвестные, не страницы или чужой команды
}

interface PageStore {
  # Залить страницу: запись `pending`, без claim её удалит GC.
  upload @0 (page :PageUpload) -> (result :PageStored);
  # Страницы вошли в главу: `pending` → `done`. Все или ничего: при любом плохом токене ничего не меняется.
  # Токен, чья папка ≠ `titles/{titleId}/{chapterId}`, считается плохим.
  claim @1 (tokens :List(Text), teamId :UInt64, titleId :UInt32, chapterId :UInt32) -> (result :PagesResult);
  # Страницы больше не нужны (убраны из главы или из черновика): `released`, объекты удалит GC.
  release @2 (tokens :List(Text), teamId :UInt64) -> (result :PagesResult);
}
