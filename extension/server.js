/**
 * MCP Bridge: conecta Claude Desktop al servidor HTTP de SIGPAC Explorer.
 * Puente stdio → HTTP usando mcp-remote.
 */
const { spawn } = require("child_process");

const serverUrl = process.env.MCP_SERVER_URL || "http://192.168.2.203:31904/mcp";

const child = spawn(
  "npx",
  ["-y", "mcp-remote", serverUrl, "--allow-http"],
  {
    stdio: "inherit",
    env: { ...process.env },
    shell: true,
  }
);

child.on("error", (err) => {
  console.error("Error launching mcp-remote:", err.message);
  process.exit(1);
});

child.on("exit", (code) => {
  process.exit(code || 0);
});
