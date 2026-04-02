const SUPABASE_URL = "https://dnrlaowhagxjfjzkoyur.supabase.co";
const SUPABASE_KEY = "sb_publishable_8Rsg9hKeaur_seeAGVJd8w_H60X9ZVG";

const supabaseClient = supabase.createClient(
  SUPABASE_URL,
  SUPABASE_KEY
);

// ===============================
// 🔍 YOUTUBE ID EXTRACTION
// ===============================
function extractYouTubeID(url) {
  if (!url) return null;

  const regex =
    /(?:youtube\.com\/(?:watch\?v=|embed\/|live\/)|youtu\.be\/)([^&\n?#]+)/;

  const match = url.match(regex);
  return match ? match[1] : null;
}


// ===============================
// LOAD DATA FOR NEXT VIDEO
// ===============================
async function loadNextLaunchVideo() {
  const container = document.getElementById("next-video");

  const nowISO = new Date().toISOString();

  const { data, error } = await supabaseClient
    .from("launch_ref")
    .select(`
      mission_name,
      rocket_full_name,
      lsp_name,
      net,
      vid_url
    `)
    .gte("net", nowISO)
    .in("status_abbrev", ["Go"])
    .not("vid_url", "is", null)
    .order("net", { ascending: true })
    .limit(1); // 🔴 SOLO il più recente

  if (error || !data || data.length === 0) {
    container.innerHTML = "No video available";
    return;
  }

  const launch = data[0]; // ✅ sempre l'ultimo
  renderNextVideo(launch);
}


// ===============================
// RENDER NEXT VIDEO
// ===============================
function renderNextVideo(launch) {
  const container = document.getElementById("next-video");
  const textContainer = document.getElementById("next-video-text");

  const url = launch.vid_url;

  // 📝 TESTO
  textContainer.innerHTML = `
    <div>
      <h3 style="text-align: left;">
        <span style="font-weight: normal;">
        🛰️ Here you can watch live the next rocket launch.   
        ${launch.lsp_name || ""} 
		${launch.rocket_full_name || ""}, 
        ${launch.mission_name || ""}
        will be launched at ${launch.net || ""}.</span>
      </h3>
    </div>
  `;

  // ===============================
  // 🎥 YOUTUBE
  // ===============================
  const ytId = extractYouTubeID(url);

  if (ytId) {
    container.innerHTML = `
      <iframe
        width="800"
        height="450"
        src="https://www.youtube.com/embed/${ytId}"
        frameborder="0"
        allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture"
        referrerpolicy="strict-origin-when-cross-origin"
        allowfullscreen>
      </iframe>
    `;
    return;
  }

  // ===============================
  // 🐦 X.COM (TWITTER)
  // ===============================
  if (url.includes("x.com")) {
    container.innerHTML = `
      <a href="${url}" target="_blank">
        <img
          src="https://spacepatches.github.io/launches/Livefeed.png"
          alt="Watch broadcast"
          width="800"
        />
      </a>
    `;
    return;
  }

  // ===============================
  // 🎬 VIMEO
  // ===============================
  if (url.includes("vimeo.com")) {
    const videoId = url.split("/").pop();

    container.innerHTML = `
      <iframe
        width="800"
        height="450"
        src="https://player.vimeo.com/video/${videoId}"
        frameborder="0"
        allowfullscreen>
      </iframe>
    `;
    return;
  }

  // ===============================
  // 🌐 FALLBACK
  // ===============================
  container.innerHTML = `
    <a href="${url}" target="_blank">Watch video</a>
  `;
}

// ===============================
// ▶️ START
// ===============================
loadNextLaunchVideo();