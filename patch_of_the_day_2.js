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
// 🚀 LOAD DATA
// ===============================
async function loadPatchOfTheDay() {
  const container = document.getElementById("latest-video");
  const textContainer = document.getElementById("latest-video-text");

  // 🔹 Fetch all patches
  const { data, error } = await supabaseClient
    .from("patch_of_the_day")
    .select("*");

  if (error || !data || data.length === 0) {
    container.innerHTML = "No patch available today";
    return;
  }

  // 🔹 Get today's patch
  const today = new Date();
  const todayDay = today.getDate();
  const todayMonth = today.getMonth() + 1;

  const patch = data.find(item => {
    const d = new Date(item.date);
    return d.getDate() === todayDay && d.getMonth() + 1 === todayMonth;
  });

  if (!patch) {
    console.log("Nessuna patch per oggi");
    container.innerHTML = "No patch available today";
    return;
  }

  renderPatchOfTheDay(patch);
}

// ===============================
// 🎥 RENDER VIDEO
// ===============================
function renderPatchOfTheDay(patch) {
  const container = document.getElementById("latest-video");
  const textContainer = document.getElementById("latest-video-text");

  const url = patch.vid_url;

  // 📝 TESTO
  textContainer.innerHTML = `
    <div>
      <h3 style="text-align: left;">
        <span style="font-weight: normal;">
        🛰️ Patch of the Day:<br>  
        <b>${patch.title || ""}</b><br>
        ${patch.description || ""}
        </span>
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
          src="https://spacepatches.github.io/launches/LatestLiveFeed.png"
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
loadPatchOfTheDay();