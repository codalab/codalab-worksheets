const fs = require("fs");

const path =
  process.argv[2] === "dev"
    ? "/opt/frontend/public/js/env.js"
    : "build/js/env.js";

const args = Object.keys(process.env).filter((arg) =>
  arg.startsWith("REACT_APP_")
);

const vars = args.map((arg) => `window.env.${arg}="${process.env[arg]}"`);

fs.writeFileSync(path, "window.env = {}\n" + vars.join("\n"));
