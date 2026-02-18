window.addEventListener("DOMContentLoaded", () => {
  const train = document.getElementById("train");
  if (!train) return;

  // Kleine, bewusste Variation für natürlicheres Rollen.
  const durations = [11, 12, 13];
  const pick = durations[Math.floor(Math.random() * durations.length)];
  train.style.animationDuration = `${pick}s`;
});
