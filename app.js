const form = document.getElementById("name-form");
const nameInput = document.getElementById("name-input");
const stage = document.getElementById("stage");

let activeNode = null;
let animationId = null;

function stopAnimation() {
  if (animationId) {
    cancelAnimationFrame(animationId);
    animationId = null;
  }
}

function startFlight(name) {
  stopAnimation();

  if (activeNode) {
    activeNode.remove();
  }

  const node = document.createElement("div");
  node.className = "flying-name";
  node.textContent = name;
  stage.appendChild(node);
  activeNode = node;

  let x = Math.max(20, window.innerWidth * 0.15);
  let y = Math.max(20, window.innerHeight * 0.25);
  let vx = 4 + Math.random() * 2;
  let vy = 3 + Math.random() * 2;

  function frame() {
    const rect = node.getBoundingClientRect();
    const maxX = window.innerWidth - rect.width;
    const maxY = window.innerHeight - rect.height;

    x += vx;
    y += vy;

    if (x <= 0 || x >= maxX) {
      vx *= -1;
      x = Math.max(0, Math.min(x, maxX));
    }

    if (y <= 0 || y >= maxY) {
      vy *= -1;
      y = Math.max(0, Math.min(y, maxY));
    }

    node.style.transform = `translate3d(${x}px, ${y}px, 0)`;
    animationId = requestAnimationFrame(frame);
  }

  frame();
}

form.addEventListener("submit", (event) => {
  event.preventDefault();
  const name = nameInput.value.trim();
  if (!name) {
    return;
  }

  startFlight(name);
});

window.addEventListener("resize", () => {
  if (activeNode) {
    activeNode.style.transform = "translate3d(20px, 20px, 0)";
  }
});
