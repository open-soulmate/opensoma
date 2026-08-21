/// Embedded web assets for the OpenSoma status server dashboard.
///
/// All HTML, CSS, and JS files are included at compile time via `include_str!`
/// so the binary is fully self-contained — no external files needed at runtime.
// Main dashboard HTML page
pub const INDEX_HTML: &str = include_str!("index.html");

/// Admin framework CSS (sidebar layout, cards, tables)
pub const ADMIN_CSS: &str = include_str!("admin-framework.css");

/// Admin framework JS (sidebar toggle, theme, navigation)
pub const ADMIN_JS: &str = include_str!("admin-framework.js");

/// Shared sidebar CSS — same file used by OpenMate/OpenSoul/OpenSoma
pub const SHARED_CSS: &str = include_str!("shared-sidebar.css");

/// Sidebar CSS (new unified version)
pub const SIDEBAR_CSS: &str = include_str!("sidebar.css");

/// Sidebar JS
pub const SIDEBAR_JS: &str = include_str!("sidebar.js");

/// App-specific JS
pub const APP_JS: &str = include_str!("app.js");

/// App-specific CSS
pub const STYLE_CSS: &str = include_str!("style.css");
