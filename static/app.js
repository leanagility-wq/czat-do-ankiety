const form = document.querySelector("#chat-form");
const input = document.querySelector("#question");
const messages = document.querySelector("#messages");

function appendMessage(role, text, meta = "") {
  const item = document.createElement("li");
  item.className = `message ${role}`;

  const body = document.createElement("div");
  body.className = "message-body";
  body.textContent = text;
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
    const meta = [data.source, data.warning].filter(Boolean).join(" • ");
    appendMessage("assistant", data.answer, meta);
  } catch (error) {
    appendMessage("assistant", "Nie udało się połączyć z backendem.");
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
