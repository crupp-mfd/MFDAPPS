const arena = document.getElementById("arena");
const startBtn = document.getElementById("start-btn");
const scoreEl = document.getElementById("score");
const timeEl = document.getElementById("time");
const hitsEl = document.getElementById("hits");
const shotsEl = document.getElementById("shots");
const accEl = document.getElementById("accuracy");
const statusEl = document.getElementById("status-text");
const crosshair = document.getElementById("crosshair");
const pistol = document.getElementById("pistol");

const skins = ["glow", "royal", "sunset"];
const badges = ["✨", "🕶️", "💎", "🌟", "💖"];

let running = false;
let score = 0;
let shots = 0;
let hits = 0;
let timeLeft = 60;
let spawnTimer = null;
let gameTimer = null;
let rafId = null;
let chickens = [];

const audioCtx = new (window.AudioContext || window.webkitAudioContext)();

function playShotSound() {
  const osc = audioCtx.createOscillator();
  const gain = audioCtx.createGain();
  osc.type = "square";
  osc.frequency.setValueAtTime(160, audioCtx.currentTime);
  osc.frequency.exponentialRampToValueAtTime(60, audioCtx.currentTime + 0.08);
  gain.gain.setValueAtTime(0.15, audioCtx.currentTime);
  gain.gain.exponentialRampToValueAtTime(0.0001, audioCtx.currentTime + 0.09);
  osc.connect(gain);
  gain.connect(audioCtx.destination);
  osc.start();
  osc.stop(audioCtx.currentTime + 0.09);
}

function updateHud() {
  scoreEl.textContent = String(score);
  shotsEl.textContent = String(shots);
  hitsEl.textContent = String(hits);
  timeEl.textContent = String(timeLeft);
  const acc = shots > 0 ? Math.round((hits / shots) * 100) : 0;
  accEl.textContent = `${acc}%`;
}

function spawnChicken() {
  if (!running) return;

  const el = document.createElement("div");
  el.className = `chicken ${skins[Math.floor(Math.random() * skins.length)]}`;
  const tag = document.createElement("span");
  tag.className = "tag";
  tag.textContent = badges[Math.floor(Math.random() * badges.length)];
  el.appendChild(tag);

  const size = 72 + Math.random() * 44;
  const fromLeft = Math.random() > 0.5;
  const y = 40 + Math.random() * Math.max(80, arena.clientHeight - size - 60);
  const speed = 1.6 + Math.random() * 2.8;

  const chicken = {
    el,
    size,
    x: fromLeft ? -size : arena.clientWidth + size,
    y,
    vx: fromLeft ? speed : -speed,
    alive: true,
  };

  el.style.width = `${size}px`;
  el.style.left = `${chicken.x}px`;
  el.style.top = `${chicken.y}px`;

  arena.appendChild(el);
  chickens.push(chicken);
}

function gameLoop() {
  chickens.forEach((c) => {
    if (!c.alive) return;
    c.x += c.vx;
    c.el.style.left = `${c.x}px`;
    if (c.x < -c.size - 20 || c.x > arena.clientWidth + c.size + 20) {
      c.alive = false;
      c.el.remove();
    }
  });

  chickens = chickens.filter((c) => c.alive);
  rafId = requestAnimationFrame(gameLoop);
}

function shootAt(clientX, clientY) {
  if (!running) return;

  shots += 1;
  pistol.classList.remove("recoil");
  void pistol.offsetWidth;
  pistol.classList.add("recoil");
  playShotSound();

  const arenaRect = arena.getBoundingClientRect();
  const x = clientX - arenaRect.left;
  const y = clientY - arenaRect.top;

  let hitSomething = false;

  for (const c of chickens) {
    if (!c.alive) continue;
    const rect = c.el.getBoundingClientRect();
    if (clientX >= rect.left && clientX <= rect.right && clientY >= rect.top && clientY <= rect.bottom) {
      c.alive = false;
      c.el.classList.add("hit");
      setTimeout(() => c.el.remove(), 160);
      score += 12;
      hits += 1;
      hitSomething = true;
      break;
    }
  }

  if (!hitSomething) {
    score = Math.max(0, score - 2);
  }

  updateHud();
}

function stopGame() {
  running = false;
  clearInterval(spawnTimer);
  clearInterval(gameTimer);
  cancelAnimationFrame(rafId);
  spawnTimer = null;
  gameTimer = null;
  rafId = null;

  const acc = shots > 0 ? Math.round((hits / shots) * 100) : 0;
  statusEl.textContent = `Runde vorbei. Score ${score}, Trefferquote ${acc}%.`;
  startBtn.disabled = false;
  startBtn.textContent = "Nochmal Spielen";
}

function startGame() {
  chickens.forEach((c) => c.el.remove());
  chickens = [];

  running = true;
  score = 0;
  shots = 0;
  hits = 0;
  timeLeft = 60;
  updateHud();

  statusEl.textContent = "Jagd laeuft. Klick = Schuss.";
  startBtn.disabled = true;
  startBtn.textContent = "Laeuft...";

  spawnTimer = setInterval(spawnChicken, 650);
  gameTimer = setInterval(() => {
    timeLeft -= 1;
    updateHud();
    if (timeLeft <= 0) {
      stopGame();
    }
  }, 1000);

  for (let i = 0; i < 4; i += 1) {
    spawnChicken();
  }

  gameLoop();
}

window.addEventListener("mousemove", (event) => {
  crosshair.style.left = `${event.clientX}px`;
  crosshair.style.top = `${event.clientY}px`;
});

arena.addEventListener("click", (event) => {
  shootAt(event.clientX, event.clientY);
});

arena.addEventListener("touchstart", (event) => {
  const touch = event.changedTouches[0];
  if (!touch) return;
  crosshair.style.left = `${touch.clientX}px`;
  crosshair.style.top = `${touch.clientY}px`;
  shootAt(touch.clientX, touch.clientY);
});

startBtn.addEventListener("click", async () => {
  if (audioCtx.state === "suspended") {
    await audioCtx.resume();
  }
  startGame();
});

updateHud();
