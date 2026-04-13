from fastapi.responses import HTMLResponse


def render_index_html(examples: list[str]) -> HTMLResponse:
    examples_html = "".join(
        f'<button class="example-item" type="button" data-question="{example}">{example}</button>'
        for example in examples
    )

    html = f"""<!DOCTYPE html>
<html lang="pl">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Survey Chatbot</title>
    <link rel="stylesheet" href="/static/styles.css" />
  </head>
  <body>
    <main class="shell">
      <section class="hero">
        <p class="eyebrow">Analiza ankiety</p>
        <h1>Chatbot odpowiadający wyłącznie na podstawie wyników badania</h1>
        <p class="intro">
          Zadaj pytanie o liczby, przekroje, zależności albo odpowiedzi otwarte.
          Jeśli ankieta czegoś nie obejmuje, aplikacja odmówi lub zwróci brak danych.
        </p>
      </section>

      <section class="panel">
        <form id="chat-form" class="chat-form">
          <label for="question" class="label">Twoje pytanie</label>
          <div class="composer">
            <input
              id="question"
              name="question"
              type="text"
              maxlength="500"
              placeholder="Np. Czy częstsze używanie AI wiąże się z większą efektywnością?"
              required
            />
            <button type="submit">Zapytaj</button>
          </div>
        </form>

        <div class="examples">
          <h2>Przykładowe pytania</h2>
          <div class="example-list">{examples_html}</div>
        </div>

        <div class="answers">
          <h2>Odpowiedzi</h2>
          <ul id="messages" class="message-list"></ul>
        </div>
      </section>
    </main>

    <script src="/static/app.js"></script>
  </body>
</html>"""
    return HTMLResponse(html)
