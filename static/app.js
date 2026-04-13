const form = document.querySelector("#chat-form");
const input = document.querySelector("#question");
const messages = document.querySelector("#messages");

function flushList(container, listType, items) {
  if (!items.length || !listType) {
    return;
  }

  const list = document.createElement(listType);
  list.className = "formatted-list";

  items.forEach((itemText) => {
    const item = document.createElement("li");
    item.textContent = itemText;
    list.appendChild(item);
  });

  container.appendChild(list);
}

function renderFormattedText(text) {
  const container = document.createElement("div");
  container.className = "formatted-content";

  const normalized = String(text || "").replace(/\r\n/g, "\n").trim();
  const lines = normalized ? normalized.split("\n") : [];

  let currentListType = null;
  let currentItems = [];

  function resetList() {
    flushList(container, currentListType, currentItems);
    currentListType = null;
    currentItems = [];
  }

  lines.forEach((rawLine) => {
    const line = rawLine.trim();

    if (!line) {
      resetList();
      return;
    }

    if (line.startsWith(">")) {
      resetList();
      const quote = document.createElement("blockquote");
      quote.className = "formatted-quote";
      quote.textContent = line.replace(/^>\s*/, "");
      container.appendChild(quote);
      return;
    }

    if (/^[-*\u2022]\s+/.test(line)) {
      const itemText = line.replace(/^[-*\u2022]\s+/, "");
      if (currentListType !== "ul") {
        resetList();
        currentListType = "ul";
      }
      currentItems.push(itemText);
      return;
    }

    if (/^\d+\.\s+/.test(line)) {
      const itemText = line.replace(/^\d+\.\s+/, "");
      if (currentListType !== "ol") {
        resetList();
        currentListType = "ol";
      }
      currentItems.push(itemText);
      return;
    }

    resetList();
    const paragraph = document.createElement("p");
    paragraph.className = "formatted-paragraph";
    paragraph.textContent = line;
    container.appendChild(paragraph);
  });

  resetList();

  if (!container.childNodes.length) {
    const paragraph = document.createElement("p");
    paragraph.className = "formatted-paragraph";
    paragraph.textContent = text;
    container.appendChild(paragraph);
  }

  return container;
}

function appendMessage(role, text, meta = "") {
  const item = document.createElement("li");
  item.className = `message ${role}`;

  const body = document.createElement("div");
  body.className = "message-body";

  if (role === "assistant") {
    body.appendChild(renderFormattedText(text));
  } else {
    body.textContent = text;
  }

  item.appendChild(body);

  if (meta) {
    const footer = document.createElement("div");
    footer.className = "message-meta";
    footer.textContent = meta;
    item.appendChild(footer);
  }

  messages.prepend(item);
}

async function submitQuestion(question) {
  appendMessage("user", question);

  try {
    const response = await fetch("/chat", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ question }),
    });

    const data = await response.json();
    const meta = [data.source, data.warning].filter(Boolean).join(" \u2022 ");
    appendMessage("assistant", data.answer, meta);
  } catch (error) {
    appendMessage("assistant", "Nie uda\u0142o si\u0119 po\u0142\u0105czy\u0107 z backendem.");
  }
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  const question = input.value.trim();
  if (!question) {
    return;
  }

  input.value = "";
  await submitQuestion(question);
});

document.querySelectorAll(".example-item").forEach((button) => {
  button.addEventListener("click", async () => {
    const question = button.dataset.question;
    input.value = question;
    input.focus();
  });
});
